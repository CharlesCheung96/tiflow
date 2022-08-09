#!/bin/bash

set -eu

WORK_DIR=$OUT_DIR/$TEST_NAME
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CONFIG="$DOCKER_COMPOSE_DIR/3m3e.yaml"

function run() {
	start_engine_cluster $CONFIG
	# add a delay in case that the cluster is not ready
	sleep 3s
	go test -count=1 -v -run ^TestWorkerExit$ github.com/pingcap/tiflow/engine/test/e2e
}

CONFIG=$(adjust_config $OUT_DIR $TEST_NAME $CONFIG)
trap "stop_engine_cluster $WORK_DIR $CONFIG" EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
