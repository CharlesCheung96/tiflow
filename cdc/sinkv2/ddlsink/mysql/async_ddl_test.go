// Copyright 2022 PingCAP, Inc.
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

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	dmysql "github.com/go-sql-driver/mysql"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"github.com/stretchr/testify/require"
)

func TestWaitAsynExecDone(t *testing.T) {
	var dbIndex int32 = 0
	GetDBConnImpl = func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			atomic.AddInt32(&dbIndex, 1)
		}()
		if atomic.LoadInt32(&dbIndex) == 0 {
			// test db
			db, err := pmysql.MockTestDB()
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mock.ExpectQuery("select tidb_version()").
			WillReturnRows(sqlmock.NewRows([]string{"tidb_version()"}).AddRow("5.7.25-TiDB-v4.0.0-beta-191-ga1b3e3b"))
		mock.ExpectQuery("select tidb_version()").WillReturnError(&dmysql.MySQLError{
			Number:  1305,
			Message: "FUNCTION test.tidb_version does not exist",
		})

		mock.ExpectQuery(fmt.Sprintf(checkRunningAddIndexSQl, "test", "sbtest0")).WillReturnRows(
			sqlmock.NewRows([]string{"JOB_ID", "JOB_TYPE", "SCHEMA_STATE", "SCHEMA_ID", "TABLE_ID", "STATE", "QUERY"}).
				AddRow("1", "add index", "running", "1", "1", "running", "Create index idx1 on test.sbtest0(a)"),
		)

		mock.ExpectQuery(fmt.Sprintf(checkRunningAddIndexSQl, "test", "sbtest0")).WillReturnRows(
			sqlmock.NewRows(nil),
		)

		mock.ExpectClose()
		return db, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sinkURI, err := url.Parse("mysql://root:@127.0.0.1:4000")
	require.NoError(t, err)
	replicateCfg := config.GetDefaultReplicaConfig()
	ddlSink, err := NewDDLSink(ctx, model.DefaultChangeFeedID("test"), sinkURI, replicateCfg)
	require.NoError(t, err)

	tables := make(map[model.TableName]struct{})
	tables[model.TableName{Schema: "test", Table: "sbtest0"}] = struct{}{}

	done := ddlSink.checkAsyncExecDDLDone(ctx, tables)
	require.False(t, done)

	done = ddlSink.checkAsyncExecDDLDone(ctx, tables)
	require.True(t, done)

	ddlSink.Close()
}

func TestAsyncExecAddIndex(t *testing.T) {
	ddlExecutionTime := time.Millisecond * 3000
	var dbIndex int32 = 0
	GetDBConnImpl = func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			atomic.AddInt32(&dbIndex, 1)
		}()
		if atomic.LoadInt32(&dbIndex) == 0 {
			// test db
			db, err := pmysql.MockTestDB()
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mock.ExpectQuery("select tidb_version()").
			WillReturnRows(sqlmock.NewRows([]string{"tidb_version()"}).AddRow("5.7.25-TiDB-v4.0.0-beta-191-ga1b3e3b"))
		mock.ExpectQuery("select tidb_version()").WillReturnError(&dmysql.MySQLError{
			Number:  1305,
			Message: "FUNCTION test.tidb_version does not exist",
		})
		mock.ExpectBegin()
		mock.ExpectExec("USE `test`;").
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("Create index idx1 on test.t1(a)").
			WillDelayFor(ddlExecutionTime).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		mock.ExpectClose()
		return db, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	sink, err := NewDDLSink(ctx, model.DefaultChangeFeedID(changefeed), sinkURI, rc)

	require.Nil(t, err)

	ddl1 := &model.DDLEvent{
		StartTs:  1000,
		CommitTs: 1010,
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema: "test",
				Table:  "t1",
			},
		},
		Type:  timodel.ActionAddIndex,
		Query: "Create index idx1 on test.t1(a)",
	}
	start := time.Now()
	err = sink.WriteDDLEvent(ctx, ddl1)
	require.Nil(t, err)
	require.True(t, time.Since(start) < ddlExecutionTime)
	require.True(t, time.Since(start) >= 2*time.Second)
	sink.Close()
}
