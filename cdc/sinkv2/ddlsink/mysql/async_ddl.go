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
	"time"

	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

var (
	checkRunningAddIndexSQl = `
SELECT JOB_ID, JOB_TYPE, SCHEMA_STATE, SCHEMA_ID, TABLE_ID, STATE, QUERY
FROM information_schema.ddl_jobs
WHERE DB_NAME = "%s" 
    AND TABLE_NAME = "%s"
    AND JOB_TYPE LIKE "add index%%"
    AND (STATE = "running" OR STATE = "queueing")
LIMIT 1;
`
)

func (m *mysqlDDLSink) shouldAsyncExecDDL(ddl *model.DDLEvent) bool {
	return m.cfg.IsTiDB && ddl.Type == timodel.ActionAddIndex
}

// asyncExecDDL executes ddl in async mode.
// this function only works in TiDB, because TiDB will save ddl jobs
// and execute them asynchronously even if ticdc crashed.
func (m *mysqlDDLSink) asyncExecDDL(ctx context.Context, ddl *model.DDLEvent) error {
	done := make(chan error, 1)
	// Use a longer timeout to ensure the add index ddl is sent to tidb before executing the next ddl.
	tick := time.NewTimer(10 * time.Second)
	defer tick.Stop()
	log.Info("async exec add index ddl start",
		zap.String("changefeedID", m.id.String()),
		zap.Uint64("commitTs", ddl.CommitTs),
		zap.String("ddl", ddl.Query))
	go func() {
		if err := m.execDDLWithMaxRetries(ctx, ddl); err != nil {
			log.Error("async exec add index ddl failed",
				zap.String("changefeedID", m.id.String()),
				zap.Uint64("commitTs", ddl.CommitTs),
				zap.String("ddl", ddl.Query))
			done <- err
			return
		}
		log.Info("async exec add index ddl done",
			zap.String("changefeedID", m.id.String()),
			zap.Uint64("commitTs", ddl.CommitTs),
			zap.String("ddl", ddl.Query))
		done <- nil
	}()

	select {
	case <-ctx.Done():
		// if the ddl is canceled, we just return nil, if the ddl is not received by tidb,
		// the downstream ddl is lost, because the checkpoint ts is forwarded.
		log.Info("async add index ddl exits as canceled",
			zap.String("changefeedID", m.id.String()),
			zap.Uint64("commitTs", ddl.CommitTs),
			zap.String("ddl", ddl.Query))
		return nil
	case err := <-done:
		// if the ddl is executed within 2 seconds, we just return the result to the caller.
		return err
	case <-tick.C:
		// if the ddl is still running, we just return nil,
		// then if the ddl is failed, the downstream ddl is lost.
		// because the checkpoint ts is forwarded.
		log.Info("async add index ddl is still running",
			zap.String("changefeedID", m.id.String()),
			zap.Uint64("commitTs", ddl.CommitTs),
			zap.String("ddl", ddl.Query))
		return nil
	}
}

// Should always wait for async ddl done before executing the next ddl.
func (m *mysqlDDLSink) waitAsynExecDone(ctx context.Context, ddl *model.DDLEvent) {
	if !m.cfg.IsTiDB {
		return
	}

	tables := make(map[model.TableName]struct{})
	if ddl.TableInfo != nil {
		tables[ddl.TableInfo.TableName] = struct{}{}
	}
	if ddl.PreTableInfo != nil {
		tables[ddl.PreTableInfo.TableName] = struct{}{}
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			done := m.checkAsyncExecDDLDone(ctx, tables)
			if done {
				return
			}
		}
	}
}

func (m *mysqlDDLSink) checkAsyncExecDDLDone(ctx context.Context, tables map[model.TableName]struct{}) bool {
	for table := range tables {
		ret := m.db.QueryRowContext(ctx, fmt.Sprintf(checkRunningAddIndexSQl, table.Schema, table.Table))
		if ret.Err() != nil {
			log.Error("check async exec ddl failed",
				zap.String("namespace", m.id.Namespace),
				zap.String("changefeed", m.id.ID),
				zap.Error(ret.Err()))
			continue
		}
		var jobID, jobType, schemaState, schemaID, tableID, state, query string
		if err := ret.Scan(&jobID, &jobType, &schemaState, &schemaID, &tableID, &state, &query); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				log.Error("check async exec ddl failed",
					zap.String("namespace", m.id.Namespace),
					zap.String("changefeed", m.id.ID),
					zap.Error(err))
			}
			continue
		}
		log.Info("async ddl is still running",
			zap.String("namespace", m.id.Namespace),
			zap.String("changefeed", m.id.ID),
			zap.String("table", table.String()),
			zap.String("jobID", jobID),
			zap.String("jobType", jobType),
			zap.String("schemaState", schemaState),
			zap.String("schemaID", schemaID),
			zap.String("tableID", tableID),
			zap.String("state", state),
			zap.String("query", query))
		return false
	}
	return true
}
