//  Copyright 2023 PingCAP, Inc.
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
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	defaultEncodingWorkerNum     = 16
	defaultEncodingInputChanSize = 256
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

type polymorphicRedoLog struct {
	event    RedoEvent
	buffer   *bytes.Buffer
	commitTs model.Ts
	finished chan struct{}

	flushFlag bool
	flushed   chan struct{}
}

func newPolymorphicRedoLogWithFlushFlag() *polymorphicRedoLog {
	return &polymorphicRedoLog{
		flushFlag: true,
		flushed:   make(chan struct{}),
	}
}

func (e *polymorphicRedoLog) reset() {
	e.event = nil
	bufferPool.Put(e.buffer)
	e.buffer = nil
	e.commitTs = 0
	e.finished = nil
}

func (e *polymorphicRedoLog) waitFlushed(ctx context.Context, lwClosed chan struct{}) error {
	if e.finished != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-lwClosed:
			return errors.ErrRedoWriterStopped.GenWithStackByArgs("flush log is terminated")
		case <-e.flushed:
		}
	}
	return nil
}

func (e *polymorphicRedoLog) markFlushed() {
	if e.flushed != nil {
		close(e.flushed)
	}
}

// encoding format: lenField(8 bytes) + rawData + padding bytes(force 8 bytes alignment)
func (e *polymorphicRedoLog) encode() (err error) {
	redoLog := e.event.ToRedoLog()
	e.commitTs = redoLog.GetCommitTs()

	rawData, err := redoLog.MarshalMsg(nil)
	if err != nil {
		return err
	}
	uint64buf := make([]byte, 8)
	lenField, padBytes := encodeFrameSize(len(rawData))
	binary.LittleEndian.PutUint64(uint64buf, lenField)

	e.buffer = bufferPool.Get().(*bytes.Buffer)
	e.buffer.Reset()
	_, err = e.buffer.Write(uint64buf)
	if err != nil {
		return err
	}
	_, err = e.buffer.Write(rawData)
	if err != nil {
		return err
	}
	if padBytes != 0 {
		e.buffer.Write(make([]byte, padBytes))
	}

	e.event = nil
	e.markFinished()
	return
}

func (e *polymorphicRedoLog) waitFinished(ctx context.Context) error {
	if e.finished != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-e.finished:
		}
	}
	return nil
}

func (e *polymorphicRedoLog) markFinished() {
	if e.finished != nil {
		close(e.finished)
	}
}

type encodingWorkerGroup struct {
	inputCh    []chan *polymorphicRedoLog
	workerNum  int
	nextWorker atomic.Uint64
	closed     chan struct{}
}

func newEncodingWorkerGroup(workerNum int) *encodingWorkerGroup {
	if workerNum <= 0 {
		workerNum = defaultEncodingWorkerNum
	}
	inputCh := make([]chan *polymorphicRedoLog, workerNum)
	for i := 0; i < workerNum; i++ {
		inputCh[i] = make(chan *polymorphicRedoLog, defaultEncodingInputChanSize)
	}
	return &encodingWorkerGroup{
		inputCh:   inputCh,
		workerNum: workerNum,
		closed:    make(chan struct{}),
	}
}

func (e *encodingWorkerGroup) run(ctx context.Context) error {
	defer close(e.closed)
	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < e.workerNum; i++ {
		index := i
		eg.Go(func() error {
			return e.runWorker(ctx, index)
		})
	}
	log.Info("redo log encoding workers started", zap.Int("workerNum", e.workerNum))
	return eg.Wait()
}

func (e *encodingWorkerGroup) runWorker(ctx context.Context, index int) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case event := <-e.inputCh[index]:
			err := event.encode()
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (e *encodingWorkerGroup) addEvent(ctx context.Context, event *polymorphicRedoLog) error {
	index := e.nextWorker.Add(1) % uint64(e.workerNum)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.closed:
		return errors.ErrRedoWriterStopped.GenWithStackByArgs("encoding worker is closed")
	case e.inputCh[index] <- event:
		return nil
	}
}
