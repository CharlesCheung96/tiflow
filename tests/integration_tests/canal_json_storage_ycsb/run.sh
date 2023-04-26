#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"

stop() {
	stop_tidb_cluster
}

function run() {
	if [ "$SINK_TYPE" != "storage" ]; then
		return
	fi

	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	# Enable tidb extension to generate the commit ts.
	SINK_URI="file://$WORK_DIR/storage_test?flush-interval=5s&enable-tidb-extension=true"
	run_cdc_cli changefeed create --sink-uri="$SINK_URI" --config=$CUR/conf/changefeed.toml

	run_sql "CREATE DATABASE canal_json_storage_ycsb;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE canal_json_storage_ycsb;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=canal_json_storage_ycsb
	go-ycsb run mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=canal_json_storage_ycsb
	run_sql "CREATE table canal_json_storage_ycsb.check1(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_storage_consumer $WORK_DIR $SINK_URI $CUR/conf/changefeed.toml ""
	sleep 8
	check_table_exists "canal_json_storage_ycsb.check1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 600
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 10
}

trap stop EXIT
# disable this test case.
# run $*
# check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
