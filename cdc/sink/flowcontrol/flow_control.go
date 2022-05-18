// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package flowcontrol

import (
	"sync"
	"sync/atomic"

	"github.com/edwingeng/deque"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

const (
	maxRowsPerTxn = 1024
	maxSizePerTxn = 1024 * 1024 /* 1MB */
	batchSize     = 100
)

// TableFlowController provides a convenient interface to control the memory consumption of a per table event stream
type TableFlowController struct {
	memoryQuota *tableMemoryQuota

	queueMu struct {
		sync.Mutex
		queue deque.Deque
	}
	// batchGroupCount is the number of txnSizeEntries with same commitTs, which could be:
	// 1. Different txns with same commitTs but different startTs
	// 2. TxnSizeEntry split from the same txns which exceeds max rows or max size
	batchGroupCount uint64
	batchID         uint64

	lastCommitTs uint64
}

type txnSizeEntry struct {
	// txn id
	startTs  uint64
	commitTs uint64
	size     uint64
	rowCount uint64
	batchID  uint64
}

// NewTableFlowController creates a new TableFlowController
func NewTableFlowController(quota uint64) *TableFlowController {
	return &TableFlowController{
		memoryQuota: newTableMemoryQuota(quota),
		queueMu: struct {
			sync.Mutex
			queue deque.Deque
		}{
			queue: deque.NewDeque(),
		},
	}
}

// Consume is called when an event has arrived for being processed by the sink.
// It will handle transaction boundaries automatically, and will not block intra-transaction.
func (c *TableFlowController) Consume(
	msg *model.PolymorphicEvent,
	size uint64,
	callBack func(release func()) error,
) error {
	commitTs := msg.CRTs
	lastCommitTs := atomic.LoadUint64(&c.lastCommitTs)
	blockingCallBack := func() error {
		release := func() {
			c.ReleaseBatch(commitTs, c.batchID)
		}
		err := callBack(release)
		c.batchID++
		c.batchGroupCount = 0
		return err
	}

	if commitTs < lastCommitTs {
		log.Panic("commitTs regressed, report a bug",
			zap.Uint64("commitTs", commitTs),
			zap.Uint64("lastCommitTs", c.lastCommitTs))
	}

	err := c.memoryQuota.consumeWithBlocking(size, blockingCallBack)
	if err != nil {
		return errors.Trace(err)
	}

	c.enqueueSingleMsg(msg, size, blockingCallBack)
	return nil
}

// Release is called when all events committed before resolvedTs has been freed from memory.
func (c *TableFlowController) Release(resolvedTs uint64) {
	var nBytesToRelease uint64

	c.queueMu.Lock()
	for c.queueMu.queue.Len() > 0 {
		if peeked := c.queueMu.queue.Front().(*txnSizeEntry); peeked.commitTs <= resolvedTs {
			nBytesToRelease += peeked.size
			c.queueMu.queue.PopFront()
		} else {
			break
		}
	}
	c.queueMu.Unlock()

	c.memoryQuota.release(nBytesToRelease)
}

// ReleaseBatch
func (c *TableFlowController) ReleaseBatch(resolvedTs, batchID uint64) {
	var nBytesToRelease uint64

	c.queueMu.Lock()
	for c.queueMu.queue.Len() > 0 {
		if peeked := c.queueMu.queue.Front().(*txnSizeEntry); peeked.commitTs < resolvedTs ||
			(peeked.commitTs == resolvedTs && peeked.batchID <= batchID) {
			nBytesToRelease += peeked.size
			c.queueMu.queue.PopFront()
		} else {
			break
		}
	}
	c.queueMu.Unlock()

	c.memoryQuota.release(nBytesToRelease)
}

// Note that msgs received by enqueueSingleMsg must be sorted by commitTs_startTs order.
func (c *TableFlowController) enqueueSingleMsg(
	msg *model.PolymorphicEvent,
	size uint64,
	blockingCallBack func() error,
) {
	commitTs := msg.CRTs
	lastCommitTs := atomic.LoadUint64(&c.lastCommitTs)

	c.queueMu.Lock()
	defer c.queueMu.Unlock()

	var e deque.Elem
	// 1. Processing a new txn with different commitTs.
	if e = c.queueMu.queue.Back(); e == nil || lastCommitTs < commitTs {
		atomic.StoreUint64(&c.lastCommitTs, commitTs)
		c.resetBatch()
		c.queueMu.queue.PushBack(&txnSizeEntry{
			startTs:  msg.StartTs,
			commitTs: commitTs,
			size:     size,
			rowCount: 1,
			batchID:  c.batchID,
		})
		msg.Row.SplitTxn = true
		return
	}

	// Processing txns with the same commitTs.
	txnEntry := e.(*txnSizeEntry)
	if txnEntry.commitTs != lastCommitTs {
		log.Panic("got wrong commitTs from deque, report a bug",
			zap.Uint64("lastCommitTs", c.lastCommitTs),
			zap.Uint64("commitTsInDeque", txnEntry.commitTs))
	}

	// 2. Append row to current txn entry.
	if txnEntry.startTs == msg.Row.StartTs &&
		txnEntry.rowCount < maxRowsPerTxn && txnEntry.size < maxSizePerTxn {
		txnEntry.size += size
		txnEntry.rowCount++
		return
	}

	// 3. Split the txn or handle a new txn with the same commitTs.
	if c.batchGroupCount+1 >= batchSize {
		_ = blockingCallBack()
	}

	c.batchGroupCount++
	c.queueMu.queue.PushBack(&txnSizeEntry{
		startTs:  msg.StartTs,
		commitTs: commitTs,
		size:     size,
		rowCount: 1,
		batchID:  c.batchID,
	})
	msg.Row.SplitTxn = true
}

func (c *TableFlowController) resetBatch() {
	// At least one batch for each txn.
	c.batchID = 1
	// At least one txnSizeEntry for each batch.
	c.batchGroupCount = 1
}

// Abort interrupts any ongoing Consume call
func (c *TableFlowController) Abort() {
	c.memoryQuota.abort()
}

// GetConsumption returns the current memory consumption
func (c *TableFlowController) GetConsumption() uint64 {
	return c.memoryQuota.getConsumption()
}
