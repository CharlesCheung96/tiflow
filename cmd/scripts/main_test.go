package main

import (
	"fmt"
	"testing"

	"go.uber.org/goleak"
)

func TestMainParallel(t *testing.T) {
	defer goleak.VerifyNone(t)
	for pa := 1; pa <= 20480; {
		fmt.Printf("========================Para: %v========================\n", pa)
		initUpstream("test_2_million")
		execInsert(pa)
		close()
		if pa < 1024 {
			pa *= 4
		} else if pa == 8192 {
			pa = 10240
		} else if pa >= 1024 {
			pa *= 2
		}
	}
}

func TestMainBatch(t *testing.T) {
	defer goleak.VerifyNone(t)
	initUpstream("test_2_million")
	batchExecInsert(1)
	close()
}
