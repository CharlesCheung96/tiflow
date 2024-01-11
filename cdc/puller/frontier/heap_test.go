// Copyright 2020 PingCAP, Inc.
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

package frontier

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInsert(t *testing.T) {
	t.Parallel()
	var heap fibonacciHeap
	target := uint64(15000)

	for i := 0; i < 5000; i++ {
		heap.Insert(uint64(10001) + target + 1)
	}
	heap.Insert(target)

	require.Equal(t, target, heap.GetMinKey())
}

func TestUpdateTs(t *testing.T) {
	t.Parallel()
	seed := time.Now().Unix()
	rand.Seed(seed)
	var heap fibonacciHeap
	nodes := make([]*fibonacciHeapNode, 2000)
	expectedMin := uint64(math.MaxUint64)
	for i := range nodes {
		key := 10000 + uint64(rand.Intn(len(nodes)/2))
		nodes[i] = heap.Insert(key)
		if expectedMin > key {
			expectedMin = key
		}
	}

	var key uint64
	for i := range nodes {
		min := heap.GetMinKey()
		require.Equal(t, expectedMin, min, "seed:%d", seed)
		if rand.Intn(2) == 0 {
			key = nodes[i].key + uint64(10000)
			heap.UpdateKey(nodes[i], key)
		} else {
			key = nodes[i].key - uint64(10000)
			heap.UpdateKey(nodes[i], key)
		}
		if expectedMin > key {
			expectedMin = key
		}
	}
}

func TestRandomUpdateTs(t *testing.T) {
	t.Parallel()
	seed := time.Now().Unix()
	rand.Seed(seed)
	var heap fibonacciHeap
	nodes := make([]*fibonacciHeapNode, 20000)
	expectedMin := uint64(math.MaxUint64)
	expectedIdx := 0
	for i := range nodes {
		key := 10000 + uint64(rand.Intn(len(nodes)/2))
		nodes[i] = heap.Insert(key)
		if expectedMin > key {
			expectedMin = key
			expectedIdx = i
		}
	}

	var key uint64
	lastOp := "Init"
	for i := 0; i < 100000; i++ {
		min := heap.GetMinKey()
		if min != expectedMin {
			root := heap.root
			fmt.Printf("root key: %d\n", root.key)

			next := root.left
			nodeCnt := 0
			minKey := root.key
			for next != nil && next != root {
				nodeCnt++
				fmt.Printf("cnt: %d, key: %d\n", nodeCnt, next.key)
				if next.key < min {
					min = next.key
				}
				next = next.left
			}
			fmt.Printf("minKey: %d, expectedMin: %d, expectedMinIdx: %d\n", minKey, expectedMin, nodes[expectedIdx].key)
		}
		require.Equal(t, expectedMin, min,
			"seed:%d, lastOperation: %s, lastKey: %d, expected: %d, actual: %d",
			seed, lastOp, key, expectedMin, min)

		// update a random node
		idx := rand.Intn(len(nodes))
		delta := rand.Uint64() % 10000
		if rand.Intn(2) == 0 {
			if nodes[idx].key > math.MaxUint64-delta {
				t.Fatal("overflow")
			}
			key = nodes[idx].key + delta
			heap.UpdateKey(nodes[idx], key)
			lastOp = "Increase"
		} else {
			if delta > nodes[idx].key {
				delta = nodes[idx].key
			}
			key = nodes[idx].key - delta
			heap.UpdateKey(nodes[idx], key)
			lastOp = "Decrease"
		}
		require.Equal(t, key, nodes[idx].key)
		if expectedMin > key {
			expectedMin = key
			expectedIdx = idx
		}
		if expectedMin == 0 {
			fmt.Printf("expectedMin: %d, expectedMinIdx: %d, key: %d, addr: %+v\n",
				expectedMin, expectedIdx, nodes[expectedIdx].key, nodes[expectedIdx])
		}
		require.Equal(t, expectedMin, nodes[expectedIdx].key,
			"seed:%d, lastOperation: %s, lastKey: %d, expected: %d, actual: %d",
			seed, lastOp, key, expectedMin, min)
	}
}

func TestRemoveNode(t *testing.T) {
	t.Parallel()
	seed := time.Now().Unix()
	rand.Seed(seed)
	var heap fibonacciHeap
	nodes := make([]*fibonacciHeapNode, 2000)
	expectedMin := uint64(math.MaxUint64)
	for i := range nodes {
		nodes[i] = heap.Insert(10000 + uint64(rand.Intn(len(nodes)/2)))
		if nodes[i].key < expectedMin {
			expectedMin = nodes[i].key
		}
	}

	preKey := expectedMin + 1
	for i := range nodes {
		min := heap.GetMinKey()
		if preKey == expectedMin {
			expectedMin = uint64(math.MaxUint64)
			for _, n := range nodes {
				if isRemoved(n) {
					continue
				}
				if expectedMin > n.key {
					expectedMin = n.key
				}
			}
		}
		require.Equal(t, expectedMin, min, "seed:%d", seed)
		preKey = nodes[i].key
		heap.Remove(nodes[i])
	}
	for _, n := range nodes {
		if !isRemoved(n) {
			t.Fatal("all of the node shoule be removed")
		}
	}
}

func isRemoved(n *fibonacciHeapNode) bool {
	return n.left == nil && n.right == nil && n.children == nil && n.parent == nil
}

func (x *fibonacciHeap) Entries(fn func(n *fibonacciHeapNode) bool) {
	heapNodeIterator(x.root, fn)
}

func heapNodeIterator(n *fibonacciHeapNode, fn func(n *fibonacciHeapNode) bool) {
	firstStep := true

	for next := n; next != nil && (next != n || firstStep); next = next.right {
		firstStep = false
		if !fn(next) {
			return
		}
		if next.children != nil {
			heapNodeIterator(next.children, fn)
		}
	}
}
