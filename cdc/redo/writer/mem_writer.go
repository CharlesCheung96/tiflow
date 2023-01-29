//  Copyright 2021 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package writer

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/pingcap/tiflow/pkg/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// memoryLogWriter implement the RedoLogWriter interface.
type memoryLogWriter struct {
	cfg           *logWriterConfig
	op            *writerOptions
	extStorage    storage.ExternalStorage
	uuidGenerator uuid.Generator

	// This fields are used to process DML and DDL logs.
	eventCh       chan *polymorphicRedoLog
	eventPool     sync.Pool
	encodeWorkers *encodingWorkerGroup
	fileWorkers   *fileWorkerGroup

	// This fields are used to process meta files and perform
	// garbage collection of logs.
	meta        *common.LogMeta
	preMetaFile string

	eg     *errgroup.Group
	cancel context.CancelFunc
	closed chan struct{}
}

func newMemoryLogWriter(
	ctx context.Context, cfg *logWriterConfig, opts ...Option,
) (*memoryLogWriter, error) {
	if cfg == nil || !cfg.CheckCompatibliity() {
		return nil, errors.WrapError(errors.ErrRedoConfigInvalid,
			errors.New("invalid LogWriterConfig"))
	}
	fixScheme(cfg)
	if !cfg.UseExternalStorage {
		return nil, errors.WrapError(errors.ErrRedoConfigInvalid,
			errors.New("memoryLogWriter only support external storage"))
	}

	extStorage, err := redo.InitExternalStorage(ctx, cfg.URI)
	if err != nil {
		return nil, err
	}
	op := &writerOptions{}
	for _, opt := range opts {
		opt(op)
	}
	var uuidGenerator uuid.Generator
	if op.getUUIDGenerator != nil {
		uuidGenerator = op.getUUIDGenerator()
	} else {
		uuidGenerator = uuid.NewGenerator()
	}
	chanSize := defaultEncodingWorkerNum * defaultEncodingInputChanSize

	eg, ctx := errgroup.WithContext(ctx)
	lwCtx, lwCancel := context.WithCancel(ctx)
	lw := &memoryLogWriter{
		cfg:           cfg,
		op:            op,
		extStorage:    extStorage,
		uuidGenerator: uuidGenerator,
		eventCh:       make(chan *polymorphicRedoLog, chanSize),
		eventPool: sync.Pool{
			New: func() interface{} {
				return &polymorphicRedoLog{}
			},
		},
		encodeWorkers: newEncodingWorkerGroup(defaultEncodingWorkerNum),
		eg:            eg,
		cancel:        lwCancel,
		closed:        make(chan struct{}),
	}

	if lw.cfg.EmitMeta {
		if err = lw.preCleanupExtStorage(lwCtx); err != nil {
			if err == nil {
				panic("preCleanupExtStorage should not return nil error")
			}
			log.Warn("pre clean redo logs fail",
				zap.String("namespace", cfg.ChangeFeedID.Namespace),
				zap.String("changefeed", cfg.ChangeFeedID.ID),
				zap.Error(err))
			return nil, err
		}
		// TODO: maybe meta should be initialized with changefeed checkpoint?
		if err = lw.initMeta(lwCtx); err != nil {
			log.Warn("init redo meta fail",
				zap.String("namespace", cfg.ChangeFeedID.Namespace),
				zap.String("changefeed", cfg.ChangeFeedID.ID),
				zap.Error(err))
			return nil, err
		}
	}

	if lw.cfg.EmitRowEvents {
		lw.fileWorkers = newFileWorkerGroup(redo.RedoRowLogFileType, defaultFlushWorkerNum,
			lw.cfg.MaxLogSize, lw.extStorage, lw.getLogFileName)
	} else if lw.cfg.EmitDDLEvents {
		lw.fileWorkers = newFileWorkerGroup(redo.RedoDDLLogFileType, defaultFlushWorkerNum,
			lw.cfg.MaxLogSize, lw.extStorage, lw.getLogFileName)
	}

	if lw.fileWorkers != nil {
		eg.Go(func() error {
			return lw.encodeWorkers.run(lwCtx)
		})
		eg.Go(func() error {
			return lw.fileWorkers.run(lwCtx)
		})
		eg.Go(func() error {
			return lw.bgDispatchEvent(lwCtx)
		})
	}
	return lw, nil
}

func fixScheme(cfg *logWriterConfig) {
	if cfg == nil {
		return
	}
	// "nfs" and "local" scheme is not supported by memoryLogWriter
	if !cfg.UseExternalStorage {
		cfg.URI.Scheme = "file"
		cfg.UseExternalStorage = redo.IsExternalStorage(cfg.URI.Scheme)
	}
}

// initMeta will read the meta file from external storage and initialize the meta
// field of memoryLogWriter.
func (l *memoryLogWriter) initMeta(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}
	l.meta = &common.LogMeta{}

	var metas []*common.LogMeta
	var toRemoveMetaFiles []string
	err := l.extStorage.WalkDir(ctx, nil, func(path string, size int64) error {
		// TODO: use prefix to accelerate traverse operation
		if !strings.HasSuffix(path, redo.MetaEXT) {
			return nil
		}
		toRemoveMetaFiles = append(toRemoveMetaFiles, path)

		data, err := l.extStorage.ReadFile(ctx, path)
		if err != nil && !util.IsNotExistInExtStorage(err) {
			return err
		}
		var meta common.LogMeta
		_, err = meta.UnmarshalMsg(data)
		if err != nil {
			return err
		}
		metas = append(metas, &meta)
		return nil
	})
	if err != nil {
		return errors.WrapError(errors.ErrRedoMetaInitialize,
			errors.Annotate(err, "read meta file fail"))
	}

	var checkpointTs, resolvedTs uint64
	common.ParseMeta(metas, &checkpointTs, &resolvedTs)
	err = l.flushLogMeta(ctx, checkpointTs, resolvedTs, true)
	if err != nil {
		return errors.WrapError(errors.ErrRedoMetaInitialize,
			errors.Annotate(err, "flush meta file fail"))
	}
	return util.DeleteFilesInExtStorage(ctx, l.extStorage, toRemoveMetaFiles)
}

func (l *memoryLogWriter) preCleanupExtStorage(ctx context.Context) error {
	deleteMarker := getDeletedChangefeedMarker(l.cfg.ChangeFeedID)
	ret, err := l.extStorage.FileExists(ctx, deleteMarker)
	if err != nil {
		return errors.WrapError(errors.ErrExternalStorageAPI, err)
	}
	if !ret {
		return nil
	}

	changefeedMatcher := getChangefeedMatcher(l.cfg.ChangeFeedID)
	err = util.RemoveFilesIf(ctx, l.extStorage, func(path string) bool {
		if path == deleteMarker || !strings.Contains(path, changefeedMatcher) {
			return false
		}
		return true
	}, nil)
	if err != nil {
		return err
	}

	err = l.extStorage.DeleteFile(ctx, deleteMarker)
	if err != nil && !util.IsNotExistInExtStorage(err) {
		return errors.WrapError(errors.ErrExternalStorageAPI, err)
	}

	return nil
}

// GC implement GC api
func (l *memoryLogWriter) GC(ctx context.Context, checkPointTs model.Ts) error {
	if l.isClosed() {
		return errors.ErrRedoWriterStopped.GenWithStackByArgs()
	}
	return util.RemoveFilesIf(ctx, l.extStorage, func(path string) bool {
		return l.shouldRemoved(path, checkPointTs)
	}, nil)
}

// shouldRemoved remove the file which maxCommitTs in file name less than checkPointTs, since
// all event ts < checkPointTs already sent to sink, the log is not needed any more for recovery
func (l *memoryLogWriter) shouldRemoved(path string, checkPointTs uint64) bool {
	changefeedMatcher := getChangefeedMatcher(l.cfg.ChangeFeedID)
	if !strings.Contains(path, changefeedMatcher) {
		return false
	}
	if filepath.Ext(path) != redo.LogEXT {
		return false
	}

	commitTs, fileType, err := redo.ParseLogFileName(path)
	if err != nil {
		if err != nil {
			log.Error("parse file name failed", zap.String("path", path), zap.Error(err))
			return false
		}
	}
	return commitTs < checkPointTs && fileType == l.fileWorkers.fileType
}

// WriteLog implement RedoLogWriter.WriteLog
func (l *memoryLogWriter) WriteLog(ctx context.Context, rows ...RedoEvent) error {
	return l.writeEvent(ctx, rows)
}

// WriteDDL implement RedoLogWriter.WriteDDL
func (l *memoryLogWriter) WriteDDL(ctx context.Context, rows ...RedoEvent) error {
	return l.writeEvent(ctx, rows)
}

func (l *memoryLogWriter) writeEvent(ctx context.Context, rows []RedoEvent) error {
	if l.isClosed() {
		return errors.ErrRedoWriterStopped.GenWithStackByArgs()
	}
	for _, row := range rows {
		if row == nil {
			log.Warn("write nil event to redo log", zap.String("capture", l.cfg.CaptureID))
			continue
		}
		event := l.eventPool.Get().(*polymorphicRedoLog)
		event.event = row
		event.finished = make(chan struct{})
		if err := l.encodeWorkers.addEvent(ctx, event); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-l.closed:
			return errors.ErrRedoWriterStopped.GenWithStackByArgs()
		case l.eventCh <- event:
		}
	}
	return nil
}

// FlushLog implement FlushLog api
func (l *memoryLogWriter) FlushLog(ctx context.Context, checkpointTs, resolvedTs model.Ts) error {
	var flushEvent *polymorphicRedoLog
	if l.fileWorkers != nil {
		flushEvent = newPolymorphicRedoLogWithFlushFlag()
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-l.closed:
			return errors.ErrRedoWriterStopped.GenWithStackByArgs()
		case l.eventCh <- flushEvent:
		}
	}

	if l.cfg.EmitMeta {
		err := l.maybeFlushLogMeta(ctx, checkpointTs, resolvedTs)
		if err != nil {
			return err
		}
	}

	if flushEvent != nil {
		return flushEvent.waitFlushed(ctx, l.closed)
	}
	return nil
}

func (l *memoryLogWriter) bgDispatchEvent(ctx context.Context) error {
	defer close(l.closed)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case event, ok := <-l.eventCh:
			if !ok {
				return nil // channel closed
			}
			if event.flushFlag {
				// flush all file caches
				if err := l.fileWorkers.flushAll(ctx); err != nil {
					return errors.Trace(err)
				}
				event.markFlushed()
			} else {
				// write log to file cache
				if err := event.waitFinished(ctx); err != nil {
					return errors.Trace(err)
				}
				if err := l.fileWorkers.writeLog(ctx, event); err != nil {
					return errors.Trace(err)
				}
				event.reset()
				l.eventPool.Put(event)
			}
		}
	}
}

// GetMeta implement GetMeta api
func (l *memoryLogWriter) GetMeta() (checkpointTs, resolvedTs model.Ts) {
	return l.meta.CheckpointTs, l.meta.ResolvedTs
}

// DeleteAllLogs delete all redo logs and leave a deleted mark.
func (l *memoryLogWriter) DeleteAllLogs(ctx context.Context) error {
	// Write deleted mark before clean any files.
	deleteMarker := getDeletedChangefeedMarker(l.cfg.ChangeFeedID)
	err := l.extStorage.WriteFile(ctx, deleteMarker, []byte("D"))
	log.Info("redo manager write deleted mark",
		zap.String("namespace", l.cfg.ChangeFeedID.Namespace),
		zap.String("changefeed", l.cfg.ChangeFeedID.ID),
		zap.Error(errors.WrapError(errors.ErrExternalStorageAPI, err)))

	changefeedMatcher := getChangefeedMatcher(l.cfg.ChangeFeedID)
	return util.RemoveFilesIf(ctx, l.extStorage, func(path string) bool {
		if path == deleteMarker || !strings.Contains(path, changefeedMatcher) {
			return false
		}
		return true
	}, nil)
}

// Close implements RedoLogWriter.Close
func (l *memoryLogWriter) Close() (err error) {
	if l.cancel != nil {
		l.cancel()
	}
	return l.eg.Wait()
}

func (l *memoryLogWriter) isClosed() bool {
	select {
	case <-l.closed:
		return true
	default:
		return false
	}
}

func (l *memoryLogWriter) maybeUpdateMeta(checkpointTs, resolvedTs uint64) ([]byte, error) {
	// NOTE: both checkpoint and resolved can regress if a cdc instance restarts.
	hasChange := false
	if checkpointTs > l.meta.CheckpointTs {
		l.meta.CheckpointTs = checkpointTs
		hasChange = true
	} else if checkpointTs > 0 && checkpointTs != l.meta.CheckpointTs {
		log.Warn("flushLogMeta with a regressed checkpoint ts, ignore",
			zap.Uint64("currCheckpointTs", l.meta.CheckpointTs),
			zap.Uint64("recvCheckpointTs", checkpointTs),
			zap.String("namespace", l.cfg.ChangeFeedID.Namespace),
			zap.String("changefeed", l.cfg.ChangeFeedID.ID))
	}
	if resolvedTs > l.meta.ResolvedTs {
		l.meta.ResolvedTs = resolvedTs
		hasChange = true
	} else if resolvedTs > 0 && resolvedTs != l.meta.ResolvedTs {
		log.Warn("flushLogMeta with a regressed resolved ts, ignore",
			zap.Uint64("currResolvedTs", l.meta.ResolvedTs),
			zap.Uint64("recvResolvedTs", resolvedTs),
			zap.String("namespace", l.cfg.ChangeFeedID.Namespace),
			zap.String("changefeed", l.cfg.ChangeFeedID.ID))
	}

	if !hasChange {
		return nil, nil
	}

	data, err := l.meta.MarshalMsg(nil)
	if err != nil {
		err = errors.WrapError(errors.ErrMarshalFailed, err)
	}
	return data, err
}

func (l *memoryLogWriter) maybeFlushLogMeta(
	ctx context.Context, checkpointTs, resolvedTs uint64,
) error {
	return l.flushLogMeta(ctx, checkpointTs, resolvedTs, false)
}

func (l *memoryLogWriter) flushLogMeta(
	ctx context.Context, checkpointTs, resolvedTs uint64, forceFlush bool,
) error {
	data, err := l.maybeUpdateMeta(checkpointTs, resolvedTs)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		if !forceFlush {
			return nil
		}
		data, err = l.meta.MarshalMsg(nil)
		if err != nil {
			return errors.WrapError(errors.ErrMarshalFailed, err)
		}
	}

	ctx, cancel := context.WithTimeout(ctx, redo.DefaultTimeout)
	defer cancel()
	return l.flushMetaToS3(ctx, data)
}

func (l *memoryLogWriter) flushMetaToS3(ctx context.Context, data []byte) error {
	start := time.Now()
	metaFile := l.getMetafileName()
	if err := l.extStorage.WriteFile(ctx, metaFile, data); err != nil {
		return errors.WrapError(errors.ErrExternalStorageAPI, err)
	}

	if l.preMetaFile != "" {
		if l.preMetaFile == metaFile {
			// This should only happen when use a constant uuid generator in test.
			return nil
		}
		err := l.extStorage.DeleteFile(ctx, l.preMetaFile)
		if err != nil && !util.IsNotExistInExtStorage(err) {
			return errors.WrapError(errors.ErrExternalStorageAPI, err)
		}
	}
	l.preMetaFile = metaFile
	log.Debug("flush meta to s3",
		zap.String("metaFile", metaFile),
		zap.Any("cost", time.Since(start).Milliseconds()))
	return nil
}

func (l *memoryLogWriter) getMetafileName() string {
	return fmt.Sprintf(redo.RedoMetaFileFormat, l.cfg.CaptureID,
		l.cfg.ChangeFeedID.Namespace, l.cfg.ChangeFeedID.ID,
		redo.RedoMetaFileType, l.uuidGenerator.NewString(), redo.MetaEXT)
}

func (l *memoryLogWriter) getLogFileName(maxCommitTS model.Ts, fileType string) string {
	if l.op != nil && l.op.getLogFileName != nil {
		return l.op.getLogFileName()
	}
	uid := l.uuidGenerator.NewString()
	if model.DefaultNamespace == l.cfg.ChangeFeedID.Namespace {
		return fmt.Sprintf(redo.RedoLogFileFormatV1,
			l.cfg.CaptureID, l.cfg.ChangeFeedID.ID, fileType,
			maxCommitTS, uid, redo.LogEXT)
	}
	return fmt.Sprintf(redo.RedoLogFileFormatV2,
		l.cfg.CaptureID, l.cfg.ChangeFeedID.Namespace, l.cfg.ChangeFeedID.ID,
		fileType, maxCommitTS, uid, redo.LogEXT)
}
