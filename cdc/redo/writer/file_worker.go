//  Copyright 2022 PingCAP, Inc.
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
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// Maximum allocated memory is workerNum*maxLogSize, which is
	// `8*64MB = 512MB` by default.
	defaultFlushWorkerNum = 8
)

type fileCache struct {
	data        []byte
	maxCommitTs model.Ts
	// After memoryWriter become stable, this field would be used to
	// avoid traversing log files.
	minCommitTs model.Ts

	filename string
	flushed  chan struct{}
}

func newFileCache(log *polymorphicRedoLog, buf []byte) *fileCache {
	buf = buf[:0]
	buf = append(buf, log.buffer.Bytes()...)
	return &fileCache{
		data:        buf,
		maxCommitTs: log.commitTs,
		minCommitTs: log.commitTs,
		flushed:     make(chan struct{}),
	}
}

func (f *fileCache) waitFlushed(ctx context.Context) error {
	if f.flushed != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-f.flushed:
		}
	}
	return nil
}

func (f *fileCache) markFlushed() {
	if f.flushed != nil {
		close(f.flushed)
	}
}

func (f *fileCache) appendLog(log *polymorphicRedoLog) {
	f.data = append(f.data, log.buffer.Bytes()...)
	if log.commitTs > f.maxCommitTs {
		f.maxCommitTs = log.commitTs
	}
	if log.commitTs < f.minCommitTs {
		f.minCommitTs = log.commitTs
	}
}

type fileWorkerGroup struct {
	fileType string
	files    []*fileCache
	flushCh  chan *fileCache
	pool     sync.Pool

	workerNum   int
	maxLogSize  int64
	extStorage  storage.ExternalStorage
	genFileName func(model.Ts, string) string

	closed chan struct{}
}

func newFileWorkerGroup(
	filetype string,
	workerNum int, maxLogSize int64,
	extStorage storage.ExternalStorage,
	genFileName func(model.Ts, string) string,
) *fileWorkerGroup {
	if workerNum <= 0 {
		workerNum = defaultFlushWorkerNum
	}

	maxLogSize = maxLogSize * megabyte
	if maxLogSize <= 0 {
		maxLogSize = defaultMaxLogSize
	}

	return &fileWorkerGroup{
		fileType: filetype,
		flushCh:  make(chan *fileCache),
		pool: sync.Pool{
			New: func() interface{} {
				// Use pointer here to prevent static checkers from reporting errors.
				// Ref: https://github.com/dominikh/go-tools/issues/1336.
				buf := make([]byte, 0, maxLogSize)
				return &buf
			},
		},
		workerNum:   workerNum,
		maxLogSize:  maxLogSize,
		genFileName: genFileName,
		extStorage:  extStorage,
		closed:      make(chan struct{}),
	}
}

func (f *fileWorkerGroup) run(ctx context.Context) error {
	defer close(f.closed)
	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < f.workerNum; i++ {
		eg.Go(func() error {
			return f.runWorker(ctx)
		})
	}
	log.Info("redo file workers started", zap.Int("workerNum", f.workerNum))
	return eg.Wait()
}

func (f *fileWorkerGroup) runWorker(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case file := <-f.flushCh:
			// FIXME: fix retry and timeout
			err := f.extStorage.WriteFile(ctx, file.filename, file.data)
			if err != nil {
				return errors.Trace(err)
			}

			file.markFlushed()
			bufPtr := &file.data
			file.data = nil
			f.pool.Put(bufPtr)
		}
	}
}

func (f *fileWorkerGroup) appendFileCache(log *polymorphicRedoLog) {
	bufPtr := f.pool.Get().(*[]byte)
	file := newFileCache(log, *bufPtr)
	f.files = append(f.files, file)
}

func (f *fileWorkerGroup) writeLog(ctx context.Context, log *polymorphicRedoLog) error {
	writeLen := int64(log.buffer.Len())
	if writeLen > f.maxLogSize {
		return errors.ErrFileSizeExceed.GenWithStackByArgs(writeLen, f.maxLogSize)
	}

	if len(f.files) == 0 {
		f.appendFileCache(log)
		return nil
	}

	file := f.files[len(f.files)-1]
	if int64(len(file.data))+writeLen > f.maxLogSize {
		file.filename = f.genFileName(file.maxCommitTs, f.fileType)
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-f.closed:
			return errors.ErrRedoWriterStopped.GenWithStackByArgs("file worker is closed")
		case f.flushCh <- file:
		}

		f.appendFileCache(log)
		return nil
	}

	file.appendLog(log)
	return nil
}

func (f *fileWorkerGroup) flushAll(ctx context.Context) error {
	if len(f.files) == 0 {
		return nil
	}

	file := f.files[len(f.files)-1]
	file.filename = f.genFileName(file.maxCommitTs, f.fileType)
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case <-f.closed:
		return errors.ErrRedoWriterStopped.GenWithStackByArgs("file worker is closed")
	case f.flushCh <- file:
	}
	for _, file := range f.files {
		err := file.waitFlushed(ctx)
		if err != nil {
			return errors.Trace(err)
		}
	}

	f.files = f.files[:0]
	return nil
}
