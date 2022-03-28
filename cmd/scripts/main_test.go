package main

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMainParallel(t *testing.T) {
	defer goleak.VerifyNone(t)
	for pa:=1; pa<
	initUpstream("test_2_million")
	execInsert(1024)
	close()
}

func TestMainBatch(t *testing.T) {
	defer goleak.VerifyNone(t)
	initUpstream("test_2_million")
	batchExecInsert(1)
	close()
}
