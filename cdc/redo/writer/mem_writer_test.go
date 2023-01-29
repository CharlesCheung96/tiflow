//  Copyright 2021 PingCAP, Inc.
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
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/pingcap/tiflow/pkg/uuid"
	"github.com/stretchr/testify/require"
)

func TestWriteLog(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uri, _ := getTestExtStorage(ctx, t)
	lwcfg := &logWriterConfig{
		FileTypeConfig: redo.FileTypeConfig{
			EmitMeta:      false,
			EmitRowEvents: true,
			EmitDDLEvents: false,
		},
		CaptureID:          "test-capture",
		ChangeFeedID:       model.DefaultChangeFeedID("test-changefeed"),
		URI:                uri,
		UseExternalStorage: true,
		MaxLogSize:         10,
	}
	lw, err := newMemoryLogWriter(ctx, lwcfg)
	require.NoError(t, err)

	rows := []RedoEvent{
		nil,
		&model.RowChangedEvent{Table: &model.TableName{TableID: 11}, CommitTs: 11},
		&model.RowChangedEvent{Table: &model.TableName{TableID: 12}, CommitTs: 15},
		&model.RowChangedEvent{Table: &model.TableName{TableID: 12}, CommitTs: 8},
	}
	require.NoError(t, lw.WriteLog(ctx, rows...))
	require.Eventually(t, func() bool {
		if len(lw.eventCh) != 0 {
			log.Warn(fmt.Sprintf("eventCh len %d", len(lw.eventCh)))
		}
		return len(lw.eventCh) == 0
	}, 2*time.Second, 10*time.Millisecond)

	// test flush
	require.NoError(t, lw.FlushLog(ctx, 5, 15))

	lw.Close()
	err = lw.WriteLog(ctx, rows...)
	require.ErrorContains(t, err, "redo log writer stopped")
	err = lw.FlushLog(ctx, 5, 15)
	require.ErrorContains(t, err, "redo log writer stopped")
}

func TestWriteMetaAndDDL(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uri, extStorage := getTestExtStorage(ctx, t)
	lwcfg := &logWriterConfig{
		FileTypeConfig: redo.FileTypeConfig{
			EmitMeta:      true,
			EmitRowEvents: false,
			EmitDDLEvents: true,
		},
		CaptureID:          "test-capture",
		ChangeFeedID:       model.DefaultChangeFeedID("test-changefeed"),
		URI:                uri,
		UseExternalStorage: true,
		MaxLogSize:         10,
	}
	lw, err := newMemoryLogWriter(ctx, lwcfg, WithUUIDGenerator(func() uuid.Generator {
		return uuid.NewConstGenerator("test-uuid")
	}))
	require.NoError(t, err)

	// test meta file
	require.Equal(t, &common.LogMeta{}, lw.meta)
	ret, err := extStorage.FileExists(ctx, lw.getMetafileName())
	require.NoError(t, err)
	require.True(t, ret)

	// test ddl
	ddls := []RedoEvent{
		nil,
		&model.DDLEvent{CommitTs: 1},
		&model.DDLEvent{CommitTs: 10},
		&model.DDLEvent{CommitTs: 8},
	}
	require.NoError(t, lw.WriteDDL(ctx, ddls...))
	require.Eventually(t, func() bool {
		return len(lw.eventCh) == 0
	}, 2*time.Second, 10*time.Millisecond)

	// test flush
	require.NoError(t, lw.FlushLog(ctx, 5, 10))
	require.Equal(t, &common.LogMeta{CheckpointTs: 5, ResolvedTs: 10}, lw.meta)

	lw.Close()
	err = lw.WriteLog(ctx, ddls...)
	require.ErrorContains(t, err, "redo log writer stopped")
	err = lw.FlushLog(ctx, 5, 10)
	require.ErrorContains(t, err, "redo log writer stopped")
}

// checkpoint or meta regress should be ignored correctly.
func TestMetaRegress(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uri, extStorage := getTestExtStorage(ctx, t)
	lw, err := newMemoryLogWriter(ctx, &logWriterConfig{
		FileTypeConfig: redo.FileTypeConfig{
			EmitMeta:      true,
			EmitRowEvents: false,
			EmitDDLEvents: true,
		},
		URI:                uri,
		CaptureID:          "test-capture",
		ChangeFeedID:       model.DefaultChangeFeedID("test-log-writer-regress"),
		UseExternalStorage: true,
	}, WithUUIDGenerator(func() uuid.Generator {
		return uuid.NewConstGenerator("test-uuid")
	}))
	require.NoError(t, err)
	defer lw.Close()

	ret, err := extStorage.FileExists(ctx, lw.getMetafileName())
	require.NoError(t, err)
	require.True(t, ret)

	require.NoError(t, lw.FlushLog(ctx, 2, 4))
	data, err := extStorage.ReadFile(ctx, lw.getMetafileName())
	require.NoError(t, err)
	require.NoError(t, lw.FlushLog(ctx, 1, 3))
	newData, err := extStorage.ReadFile(ctx, lw.getMetafileName())
	require.NoError(t, err)
	require.Equal(t, data, newData)
	require.Equal(t, uint64(2), lw.meta.CheckpointTs)
	require.Equal(t, uint64(4), lw.meta.ResolvedTs)
}

func TestPreCleanupAndDeleteAllLogsInOwner(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uri, extStorage := getTestExtStorage(ctx, t)
	checkFileCounts := func(target int) {
		cnt := 0
		err := extStorage.WalkDir(ctx, nil, func(path string, _ int64) error {
			cnt++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, target, cnt)
	}

	// create delete mark and some files
	changefeedID := model.DefaultChangeFeedID("test-changefeed")
	deleteMarker := getDeletedChangefeedMarker(changefeedID)
	changefeedMatcher := getChangefeedMatcher(changefeedID)

	err := extStorage.WriteFile(ctx, deleteMarker, []byte{'D'})
	require.NoError(t, err)

	for idx := 0; idx < 10; idx++ {
		err = extStorage.WriteFile(ctx, fmt.Sprintf("%s%d%s",
			changefeedMatcher, idx, redo.LogEXT), []byte{'L'})
		require.NoError(t, err)
		err = extStorage.WriteFile(ctx, fmt.Sprintf("%s%d%s",
			changefeedMatcher, idx, redo.MetaEXT), []byte{'M'})
		require.NoError(t, err)
	}
	err = extStorage.WriteFile(ctx, "other_file", []byte{'D'})
	require.NoError(t, err)

	// test pre cleanup external storage
	lwcfg := &logWriterConfig{
		FileTypeConfig: redo.FileTypeConfig{
			EmitMeta:      true,
			EmitRowEvents: false,
			EmitDDLEvents: true,
		},
		CaptureID:          "test-capture",
		ChangeFeedID:       changefeedID,
		URI:                uri,
		UseExternalStorage: true,
		MaxLogSize:         10,
	}
	lw, err := newMemoryLogWriter(ctx, lwcfg)
	require.NoError(t, err)
	defer lw.Close()

	ret, err := extStorage.FileExists(ctx, "other_file")
	require.NoError(t, err)
	require.True(t, ret)
	checkFileCounts(2) // only meta and other_file

	// test delete all logs in owner
	err = lw.DeleteAllLogs(ctx)
	require.NoError(t, err)
	ret, err = extStorage.FileExists(ctx, "other_file")
	require.NoError(t, err)
	require.True(t, ret)
	ret, err = extStorage.FileExists(ctx, deleteMarker)
	require.NoError(t, err)
	require.True(t, ret)
	checkFileCounts(2) // only delete_marker and other_file
}

func getTestExtStorage(ctx context.Context, t *testing.T) (url.URL, storage.ExternalStorage) {
	uriStr := fmt.Sprintf("file://%s", t.TempDir())
	uri, err := storage.ParseRawURL(uriStr)
	require.NoError(t, err)

	extStorage, err := redo.InitExternalStorage(ctx, *uri)
	require.NoError(t, err)
	return *uri, extStorage
}
