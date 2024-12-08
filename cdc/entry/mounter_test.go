// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package entry

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/log"
	ticonfig "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/executor"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/integrity"
	"github.com/pingcap/tiflow/pkg/sink/codec/avro"
	codecCommon "github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

var dummyChangeFeedID = model.DefaultChangeFeedID("dummy_changefeed")

func TestMounterDisableOldValue(t *testing.T) {
	testCases := []struct {
		tableName      string
		createTableDDL string
		// [] for rows, []interface{} for columns.
		values [][]interface{}
		// [] for table partition if there is any,
		// []int for approximateBytes of rows.
		putApproximateBytes [][]int
		delApproximateBytes [][]int
	}{{
		tableName:           "simple",
		createTableDDL:      "create table simple(id int primary key)",
		values:              [][]interface{}{{1}, {2}, {3}, {4}, {5}},
		putApproximateBytes: [][]int{{346, 346, 346, 346, 346}},
		delApproximateBytes: [][]int{{346, 346, 346, 346, 346}},
	}, {
		tableName:           "no_pk",
		createTableDDL:      "create table no_pk(id int not null unique key)",
		values:              [][]interface{}{{1}, {2}, {3}, {4}, {5}},
		putApproximateBytes: [][]int{{345, 345, 345, 345, 345}},
		delApproximateBytes: [][]int{{217, 217, 217, 217, 217}},
	}, {
		tableName:           "many_index",
		createTableDDL:      "create table many_index(id int not null unique key, c1 int unique key, c2 int, INDEX (c2))",
		values:              [][]interface{}{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}, {4, 4, 4}, {5, 5, 5}},
		putApproximateBytes: [][]int{{638, 638, 638, 638, 638}},
		delApproximateBytes: [][]int{{254, 254, 254, 254, 254}},
	}, {
		tableName:           "default_value",
		createTableDDL:      "create table default_value(id int primary key, c1 int, c2 int not null default 5, c3 varchar(20), c4 varchar(20) not null default '666')",
		values:              [][]interface{}{{1}, {2}, {3}, {4}, {5}},
		putApproximateBytes: [][]int{{676, 676, 676, 676, 676}},
		delApproximateBytes: [][]int{{353, 353, 353, 353, 353}},
	}, {
		tableName: "partition_table",
		createTableDDL: `CREATE TABLE partition_table  (
				id INT NOT NULL AUTO_INCREMENT UNIQUE KEY,
				fname VARCHAR(25) NOT NULL,
				lname VARCHAR(25) NOT NULL,
				store_id INT NOT NULL,
				department_id INT NOT NULL,
				INDEX (department_id)
			)

			PARTITION BY RANGE(id)  (
				PARTITION p0 VALUES LESS THAN (5),
				PARTITION p1 VALUES LESS THAN (10),
				PARTITION p2 VALUES LESS THAN (15),
				PARTITION p3 VALUES LESS THAN (20)
			)`,
		values: [][]interface{}{
			{1, "aa", "bb", 12, 12},
			{6, "aac", "bab", 51, 51},
			{11, "aad", "bsb", 71, 61},
			{18, "aae", "bbf", 21, 14},
			{15, "afa", "bbc", 11, 12},
		},
		putApproximateBytes: [][]int{{775}, {777}, {777}, {777, 777}},
		delApproximateBytes: [][]int{{227}, {227}, {227}, {227, 227}},
	}, {
		tableName: "tp_int",
		createTableDDL: `create table tp_int
			(
				id          int auto_increment,
				c_tinyint   tinyint   null,
				c_smallint  smallint  null,
				c_mediumint mediumint null,
				c_int       int       null,
				c_bigint    bigint    null,
				constraint pk
					primary key (id)
			);`,
		values: [][]interface{}{
			{1, 1, 2, 3, 4, 5},
			{2},
			{3, 3, 4, 5, 6, 7},
			{4, 127, 32767, 8388607, 2147483647, 9223372036854775807},
			{5, -128, -32768, -8388608, -2147483648, -9223372036854775808},
		},
		putApproximateBytes: [][]int{{986, 626, 986, 986, 986}},
		delApproximateBytes: [][]int{{346, 346, 346, 346, 346}},
	}, {
		tableName: "tp_text",
		createTableDDL: `create table tp_text
		(
			id           int auto_increment,
			c_tinytext   tinytext      null,
			c_text       text          null,
			c_mediumtext mediumtext    null,
			c_longtext   longtext      null,
			c_varchar    varchar(16)   null,
			c_char       char(16)      null,
			c_tinyblob   tinyblob      null,
			c_blob       blob          null,
			c_mediumblob mediumblob    null,
			c_longblob   longblob      null,
			c_binary     binary(16)    null,
			c_varbinary  varbinary(16) null,
			constraint pk
			primary key (id)
		);`,
		values: [][]interface{}{
			{1},
			{
				2, "89504E470D0A1A0A", "89504E470D0A1A0A", "89504E470D0A1A0A", "89504E470D0A1A0A", "89504E470D0A1A0A",
				"89504E470D0A1A0A",
				string([]byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}),
				string([]byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}),
				string([]byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}),
				string([]byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}),
				string([]byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}),
				string([]byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}),
			},
			{
				3, "bug free", "bug free", "bug free", "bug free", "bug free", "bug free", "bug free", "bug free",
				"bug free", "bug free", "bug free", "bug free",
			},
			{4, "", "", "", "", "", "", "", "", "", "", "", ""},
			{5, "你好", "我好", "大家好", "道路", "千万条", "安全", "第一条", "行车", "不规范", "亲人", "两行泪", "！"},
			{6, "😀", "😃", "😄", "😁", "😆", "😅", "😂", "🤣", "☺️", "😊", "😇", "🙂"},
		},
		putApproximateBytes: [][]int{{1019, 1459, 1411, 1323, 1398, 1369}},
		delApproximateBytes: [][]int{{347, 347, 347, 347, 347, 347}},
	}, {
		tableName: "tp_time",
		createTableDDL: `create table tp_time
		(
			id          int auto_increment,
			c_date      date      null,
			c_datetime  datetime  null,
			c_timestamp timestamp null,
			c_time      time      null,
			c_year      year      null,
			constraint pk
			primary key (id)
		);`,
		values: [][]interface{}{
			{1},
			{2, "2020-02-20", "2020-02-20 02:20:20", "2020-02-20 02:20:20", "02:20:20", "2020"},
		},
		putApproximateBytes: [][]int{{627, 819}},
		delApproximateBytes: [][]int{{347, 347}},
	}, {
		tableName: "tp_real",
		createTableDDL: `create table tp_real
	(
		id        int auto_increment,
		c_float   float   null,
		c_double  double  null,
		c_decimal decimal null,
		constraint pk
		primary key (id)
	);`,
		values: [][]interface{}{
			{1},
			{2, "2020.0202", "2020.0303", "2020.0404"},
		},
		putApproximateBytes: [][]int{{563, 551}},
		delApproximateBytes: [][]int{{347, 347}},
	}, {
		tableName: "tp_other",
		createTableDDL: `create table tp_other
		(
			id     int auto_increment,
			c_enum enum ('a','b','c') null,
			c_set  set ('a','b','c')  null,
			c_bit  bit(64)            null,
			c_json json               null,
			constraint pk
			primary key (id)
		);`,
		values: [][]interface{}{
			{1},
			{2, "a", "a,c", 888, `{"aa":"bb"}`},
		},
		putApproximateBytes: [][]int{{636, 624}},
		delApproximateBytes: [][]int{{348, 348}},
	}, {
		tableName:      "clustered_index1",
		createTableDDL: "CREATE TABLE clustered_index1 (id VARCHAR(255) PRIMARY KEY, data INT);",
		values: [][]interface{}{
			{"hhh"},
			{"你好😘", 666},
			{"世界🤪", 888},
		},
		putApproximateBytes: [][]int{{383, 446, 446}},
		delApproximateBytes: [][]int{{311, 318, 318}},
	}, {
		tableName:      "clustered_index2",
		createTableDDL: "CREATE TABLE clustered_index2 (id VARCHAR(255), data INT, ddaa date, PRIMARY KEY (id, data, ddaa), UNIQUE KEY (id, data, ddaa));",
		values: [][]interface{}{
			{"你好😘", 666, "2020-11-20"},
			{"世界🤪", 888, "2020-05-12"},
		},
		putApproximateBytes: [][]int{{592, 592}},
		delApproximateBytes: [][]int{{592, 592}},
	}}
	for _, tc := range testCases {
		testMounterDisableOldValue(t, tc)
	}
}

func testMounterDisableOldValue(t *testing.T, tc struct {
	tableName           string
	createTableDDL      string
	values              [][]interface{}
	putApproximateBytes [][]int
	delApproximateBytes [][]int
},
) {
	store, err := mockstore.NewMockStore()
	require.Nil(t, err)
	defer store.Close() //nolint:errcheck
	ticonfig.UpdateGlobal(func(conf *ticonfig.Config) {
		// we can update the tidb config here
	})
	session.SetSchemaLease(time.Second)
	session.DisableStats4Test()
	domain, err := session.BootstrapSession(store)
	require.Nil(t, err)
	defer domain.Close()
	domain.SetStatsUpdating(true)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_enable_clustered_index=1;")
	tk.MustExec("use test;")

	tk.MustExec(tc.createTableDDL)

	f, err := filter.NewFilter(config.GetDefaultReplicaConfig(), "")
	require.Nil(t, err)
	jobs, err := getAllHistoryDDLJob(store, f)
	require.Nil(t, err)

	scheamStorage, err := NewSchemaStorage(nil, 0, false, dummyChangeFeedID, util.RoleTester, f)
	require.Nil(t, err)
	for _, job := range jobs {
		err := scheamStorage.HandleDDLJob(job)
		require.Nil(t, err)
	}
	tableInfo, ok := scheamStorage.GetLastSnapshot().TableByName("test", tc.tableName)
	require.True(t, ok)
	if tableInfo.IsCommonHandle {
		// we can check this log to make sure if the clustered-index is enabled
		log.Info("this table is enable the clustered index", zap.String("tableName", tableInfo.Name.L))
	}

	for _, params := range tc.values {

		insertSQL := prepareInsertSQL(t, tableInfo, len(params))
		tk.MustExec(insertSQL, params...)
	}

	ver, err := store.CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	scheamStorage.AdvanceResolvedTs(ver.Ver)
	config := config.GetDefaultReplicaConfig()
	filter, err := filter.NewFilter(config, "")
	require.Nil(t, err)
	mounter := NewMounter(scheamStorage,
		model.DefaultChangeFeedID("c1"), time.UTC, filter, config.Integrity).(*mounter)
	mounter.tz = time.Local
	ctx := context.Background()

	// [TODO] check size and readd rowBytes
	mountAndCheckRowInTable := func(tableID int64, _ []int, f func(key []byte, value []byte) *model.RawKVEntry) int {
		var rows int
		walkTableSpanInStore(t, store, tableID, func(key []byte, value []byte) {
			rawKV := f(key, value)
			row, err := mounter.unmarshalAndMountRowChanged(ctx, rawKV)
			require.Nil(t, err)
			if row == nil {
				return
			}
			rows++
			require.Equal(t, row.TableInfo.GetTableName(), tc.tableName)
			require.Equal(t, row.TableInfo.GetSchemaName(), "test")
			// [TODO] check size and reopen this check
			// require.Equal(t, rowBytes[rows-1], row.ApproximateBytes(), row)
			t.Log("ApproximateBytes", tc.tableName, rows-1, row.ApproximateBytes())
			// TODO: test column flag, column type and index columns
			if len(row.Columns) != 0 {
				checkSQL, params := prepareCheckSQL(t, tc.tableName, row.GetColumns())
				result := tk.MustQuery(checkSQL, params...)
				result.Check([][]interface{}{{"1"}})
			}
			if len(row.PreColumns) != 0 {
				checkSQL, params := prepareCheckSQL(t, tc.tableName, row.GetPreColumns())
				result := tk.MustQuery(checkSQL, params...)
				result.Check([][]interface{}{{"1"}})
			}
		})
		return rows
	}
	mountAndCheckRow := func(rowsBytes [][]int, f func(key []byte, value []byte) *model.RawKVEntry) int {
		partitionInfo := tableInfo.GetPartitionInfo()
		if partitionInfo == nil {
			return mountAndCheckRowInTable(tableInfo.ID, rowsBytes[0], f)
		}
		var rows int
		for i, p := range partitionInfo.Definitions {
			rows += mountAndCheckRowInTable(p.ID, rowsBytes[i], f)
		}
		return rows
	}

	rows := mountAndCheckRow(tc.putApproximateBytes, func(key []byte, value []byte) *model.RawKVEntry {
		return &model.RawKVEntry{
			OpType:  model.OpTypePut,
			Key:     key,
			Value:   value,
			StartTs: ver.Ver - 1,
			CRTs:    ver.Ver,
		}
	})
	require.Equal(t, rows, len(tc.values))

	rows = mountAndCheckRow(tc.delApproximateBytes, func(key []byte, value []byte) *model.RawKVEntry {
		return &model.RawKVEntry{
			OpType:  model.OpTypeDelete,
			Key:     key,
			Value:   nil, // delete event doesn't include a value when old-value is disabled
			StartTs: ver.Ver - 1,
			CRTs:    ver.Ver,
		}
	})
	require.Equal(t, rows, len(tc.values))
}

func prepareInsertSQL(t *testing.T, tableInfo *model.TableInfo, columnLens int) string {
	var sb strings.Builder
	_, err := sb.WriteString("INSERT INTO " + tableInfo.Name.O + "(")
	require.Nil(t, err)
	for i := 0; i < columnLens; i++ {
		col := tableInfo.Columns[i]
		if i != 0 {
			_, err = sb.WriteString(", ")
			require.Nil(t, err)
		}
		_, err = sb.WriteString(col.Name.O)
		require.Nil(t, err)
	}
	_, err = sb.WriteString(") VALUES (")
	require.Nil(t, err)
	for i := 0; i < columnLens; i++ {
		if i != 0 {
			_, err = sb.WriteString(", ")
			require.Nil(t, err)
		}
		_, err = sb.WriteString("?")
		require.Nil(t, err)
	}
	_, err = sb.WriteString(")")
	require.Nil(t, err)
	return sb.String()
}

func prepareCheckSQL(t *testing.T, tableName string, cols []*model.Column) (string, []interface{}) {
	var sb strings.Builder
	_, err := sb.WriteString("SELECT count(1) FROM " + tableName + " WHERE ")
	require.Nil(t, err)
	params := make([]interface{}, 0, len(cols))
	for i, col := range cols {
		// Since float type has precision problem, so skip it to avoid compare float number.
		if col == nil || col.Type == mysql.TypeFloat {
			continue
		}
		if i != 0 {
			_, err = sb.WriteString(" AND ")
			require.Nil(t, err)
		}
		if col.Value == nil {
			_, err = sb.WriteString(col.Name + " IS NULL")
			require.Nil(t, err)
			continue
		}
		// convert types for tk.MustQuery
		if bytes, ok := col.Value.([]byte); ok {
			col.Value = string(bytes)
		}
		params = append(params, col.Value)
		if col.Type == mysql.TypeJSON {
			_, err = sb.WriteString(col.Name + " = CAST(? AS JSON)")
		} else {
			_, err = sb.WriteString(col.Name + " = ?")
		}
		require.Nil(t, err)
	}
	return sb.String(), params
}

func walkTableSpanInStore(t *testing.T, store tidbkv.Storage, tableID int64, f func(key []byte, value []byte)) {
	txn, err := store.Begin()
	require.Nil(t, err)
	defer txn.Rollback() //nolint:errcheck
	startKey, endKey := spanz.GetTableRange(tableID)
	kvIter, err := txn.Iter(startKey, endKey)
	require.Nil(t, err)
	defer kvIter.Close()
	for kvIter.Valid() {
		f(kvIter.Key(), kvIter.Value())
		err = kvIter.Next()
		require.Nil(t, err)
	}
}

// We use OriginDefaultValue instead of DefaultValue in the ut, pls ref to
// https://github.com/pingcap/tiflow/issues/4048
// Ref: https://github.com/pingcap/tidb/blob/d2c352980a43bb593db81fd1db996f47af596d91/table/column.go#L489
func TestGetDefaultZeroValue(t *testing.T) {
	// Check following MySQL type, ref to:
	// https://github.com/pingcap/tidb/blob/master/parser/mysql/type.go

	// mysql flag null
	ftNull := types.NewFieldType(mysql.TypeUnspecified)

	// mysql.TypeTiny + notnull
	ftTinyIntNotNull := types.NewFieldType(mysql.TypeTiny)
	ftTinyIntNotNull.AddFlag(mysql.NotNullFlag)

	// mysql.TypeTiny + notnull +  unsigned
	ftTinyIntNotNullUnSigned := types.NewFieldType(mysql.TypeTiny)
	ftTinyIntNotNullUnSigned.SetFlag(mysql.NotNullFlag)
	ftTinyIntNotNullUnSigned.AddFlag(mysql.UnsignedFlag)

	// mysql.TypeTiny + null
	ftTinyIntNull := types.NewFieldType(mysql.TypeTiny)

	// mysql.TypeShort + notnull
	ftShortNotNull := types.NewFieldType(mysql.TypeShort)
	ftShortNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeLong + notnull
	ftLongNotNull := types.NewFieldType(mysql.TypeLong)
	ftLongNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeLonglong + notnull
	ftLongLongNotNull := types.NewFieldType(mysql.TypeLonglong)
	ftLongLongNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeInt24 + notnull
	ftInt24NotNull := types.NewFieldType(mysql.TypeInt24)
	ftInt24NotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeFloat + notnull
	ftTypeFloatNotNull := types.NewFieldType(mysql.TypeFloat)
	ftTypeFloatNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeFloat + notnull + unsigned
	ftTypeFloatNotNullUnSigned := types.NewFieldType(mysql.TypeFloat)
	ftTypeFloatNotNullUnSigned.SetFlag(mysql.NotNullFlag | mysql.UnsignedFlag)

	// mysql.TypeFloat + null
	ftTypeFloatNull := types.NewFieldType(mysql.TypeFloat)

	// mysql.TypeDouble + notnull
	ftTypeDoubleNotNull := types.NewFieldType(mysql.TypeDouble)
	ftTypeDoubleNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeNewDecimal + notnull
	ftTypeNewDecimalNull := types.NewFieldType(mysql.TypeNewDecimal)
	ftTypeNewDecimalNull.SetFlen(5)
	ftTypeNewDecimalNull.SetDecimal(2)

	// mysql.TypeNewDecimal + notnull
	ftTypeNewDecimalNotNull := types.NewFieldType(mysql.TypeNewDecimal)
	ftTypeNewDecimalNotNull.SetFlag(mysql.NotNullFlag)
	ftTypeNewDecimalNotNull.SetFlen(5)
	ftTypeNewDecimalNotNull.SetDecimal(2)

	// mysql.TypeNull
	ftTypeNull := types.NewFieldType(mysql.TypeNull)

	// mysql.TypeTimestamp + notnull
	ftTypeTimestampNotNull := types.NewFieldType(mysql.TypeTimestamp)
	ftTypeTimestampNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeTimestamp + notnull
	ftTypeTimestampNull := types.NewFieldType(mysql.TypeTimestamp)

	// mysql.TypeDate + notnull
	ftTypeDateNotNull := types.NewFieldType(mysql.TypeDate)
	ftTypeDateNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeDuration + notnull
	ftTypeDurationNotNull := types.NewFieldType(mysql.TypeDuration)
	ftTypeDurationNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeDatetime + notnull
	ftTypeDatetimeNotNull := types.NewFieldType(mysql.TypeDatetime)
	ftTypeDatetimeNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeYear + notnull
	ftTypeYearNotNull := types.NewFieldType(mysql.TypeYear)
	ftTypeYearNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeNewDate + notnull
	ftTypeNewDateNotNull := types.NewFieldType(mysql.TypeNewDate)
	ftTypeNewDateNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeVarchar + notnull
	ftTypeVarcharNotNull := types.NewFieldType(mysql.TypeVarchar)
	ftTypeVarcharNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeTinyBlob + notnull
	ftTypeTinyBlobNotNull := types.NewFieldType(mysql.TypeTinyBlob)
	ftTypeTinyBlobNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeMediumBlob + notnull
	ftTypeMediumBlobNotNull := types.NewFieldType(mysql.TypeMediumBlob)
	ftTypeMediumBlobNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeLongBlob + notnull
	ftTypeLongBlobNotNull := types.NewFieldType(mysql.TypeLongBlob)
	ftTypeLongBlobNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeBlob + notnull
	ftTypeBlobNotNull := types.NewFieldType(mysql.TypeBlob)
	ftTypeBlobNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeVarString + notnull
	ftTypeVarStringNotNull := types.NewFieldType(mysql.TypeVarString)
	ftTypeVarStringNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeString + notnull
	ftTypeStringNotNull := types.NewFieldType(mysql.TypeString)
	ftTypeStringNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeBit + notnull
	ftTypeBitNotNull := types.NewFieldType(mysql.TypeBit)
	ftTypeBitNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeJSON + notnull
	ftTypeJSONNotNull := types.NewFieldType(mysql.TypeJSON)
	ftTypeJSONNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeEnum + notnull + nodefault
	ftTypeEnumNotNull := types.NewFieldType(mysql.TypeEnum)
	ftTypeEnumNotNull.SetFlag(mysql.NotNullFlag)
	ftTypeEnumNotNull.SetElems([]string{"e0", "e1"})

	// mysql.TypeEnum + null
	ftTypeEnumNull := types.NewFieldType(mysql.TypeEnum)

	// mysql.TypeSet + notnull
	ftTypeSetNotNull := types.NewFieldType(mysql.TypeSet)
	ftTypeSetNotNull.SetFlag(mysql.NotNullFlag)
	ftTypeSetNotNull.SetElems([]string{"1", "e"})

	// mysql.TypeGeometry + notnull
	ftTypeGeometryNotNull := types.NewFieldType(mysql.TypeGeometry)
	ftTypeGeometryNotNull.SetFlag(mysql.NotNullFlag)

	testCases := []struct {
		Name    string
		ColInfo timodel.ColumnInfo
		Res     interface{}
	}{
		{
			Name:    "mysql flag null",
			ColInfo: timodel.ColumnInfo{FieldType: *ftNull},
			Res:     nil,
		},
		{
			Name:    "mysql.TypeTiny + notnull + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTinyIntNotNull.Clone()},
			Res:     int64(0),
		},
		{
			Name: "mysql.TypeTiny + notnull + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: "-128",
				FieldType:          *ftTinyIntNotNull,
			},
			Res: int64(-128),
		},
		{
			Name:    "mysql.TypeTiny + notnull + default + unsigned",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTinyIntNotNullUnSigned},
			Res:     uint64(0),
		},
		{
			Name:    "mysql.TypeTiny + notnull + unsigned",
			ColInfo: timodel.ColumnInfo{OriginDefaultValue: "127", FieldType: *ftTinyIntNotNullUnSigned},
			Res:     uint64(127),
		},
		{
			Name: "mysql.TypeTiny + null + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: "-128",
				FieldType:          *ftTinyIntNull,
			},
			Res: int64(-128),
		},
		{
			Name:    "mysql.TypeTiny + null + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTinyIntNull},
			Res:     nil,
		},
		{
			Name:    "mysql.TypeShort, others testCases same as tiny",
			ColInfo: timodel.ColumnInfo{FieldType: *ftShortNotNull},
			Res:     int64(0),
		},
		{
			Name:    "mysql.TypeLong, others testCases same as tiny",
			ColInfo: timodel.ColumnInfo{FieldType: *ftLongNotNull},
			Res:     int64(0),
		},
		{
			Name:    "mysql.TypeLonglong, others testCases same as tiny",
			ColInfo: timodel.ColumnInfo{FieldType: *ftLongLongNotNull},
			Res:     int64(0),
		},
		{
			Name:    "mysql.TypeInt24, others testCases same as tiny",
			ColInfo: timodel.ColumnInfo{FieldType: *ftInt24NotNull},
			Res:     int64(0),
		},
		{
			Name:    "mysql.TypeFloat + notnull + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeFloatNotNull},
			Res:     float32(0),
		},
		{
			Name: "mysql.TypeFloat + notnull + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: float32(-3.1415),
				FieldType:          *ftTypeFloatNotNull,
			},
			Res: float32(-3.1415),
		},
		{
			Name: "mysql.TypeFloat + notnull + default + unsigned",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: float32(3.1415),
				FieldType:          *ftTypeFloatNotNullUnSigned,
			},
			Res: float32(3.1415),
		},
		{
			Name: "mysql.TypeFloat + notnull + unsigned",
			ColInfo: timodel.ColumnInfo{
				FieldType: *ftTypeFloatNotNullUnSigned,
			},
			Res: float32(0),
		},
		{
			Name: "mysql.TypeFloat + null + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: float32(-3.1415),
				FieldType:          *ftTypeFloatNull,
			},
			Res: float32(-3.1415),
		},
		{
			Name: "mysql.TypeFloat + null + nodefault",
			ColInfo: timodel.ColumnInfo{
				FieldType: *ftTypeFloatNull,
			},
			Res: nil,
		},
		{
			Name:    "mysql.TypeDouble, other testCases same as float",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeDoubleNotNull},
			Res:     float64(0),
		},
		{
			Name:    "mysql.TypeNewDecimal + notnull + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeNewDecimalNotNull},
			Res:     "0", // related with Flen and Decimal
		},
		{
			Name:    "mysql.TypeNewDecimal + null + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeNewDecimalNull},
			Res:     nil,
		},
		{
			Name:    "mysql.TypeNull",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeNull},
			Res:     nil,
		},
		{
			Name:    "mysql.TypeTimestamp + notnull + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeTimestampNotNull},
			Res:     "0000-00-00 00:00:00",
		},
		{
			Name:    "mysql.TypeDate, other testCases same as TypeTimestamp",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeDateNotNull},
			Res:     "0000-00-00",
		},
		{
			Name:    "mysql.TypeDuration, other testCases same as TypeTimestamp",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeDurationNotNull},
			Res:     "00:00:00",
		},
		{
			Name:    "mysql.TypeDatetime, other testCases same as TypeTimestamp",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeDatetimeNotNull},
			Res:     "0000-00-00 00:00:00",
		},
		{
			Name:    "mysql.TypeYear + notnull + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeYearNotNull},
			Res:     int64(0),
		},
		{
			Name: "mysql.TypeYear + notnull + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: "2021",
				FieldType:          *ftTypeYearNotNull,
			},
			Res: int64(2021),
		},
		{
			Name:    "mysql.TypeNewDate",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeNewDateNotNull},
			Res:     nil, // [TODO] seems not support by TiDB, need check
		},
		{
			Name:    "mysql.TypeVarchar + notnull + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeVarcharNotNull},
			Res:     []byte{},
		},
		{
			Name: "mysql.TypeVarchar + notnull + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: "e0",
				FieldType:          *ftTypeVarcharNotNull,
			},
			Res: []byte("e0"),
		},
		{
			Name:    "mysql.TypeTinyBlob",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeTinyBlobNotNull},
			Res:     []byte{},
		},
		{
			Name:    "mysql.TypeMediumBlob",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeMediumBlobNotNull},
			Res:     []byte{},
		},
		{
			Name:    "mysql.TypeLongBlob",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeLongBlobNotNull},
			Res:     []byte{},
		},
		{
			Name:    "mysql.TypeBlob",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeBlobNotNull},
			Res:     []byte{},
		},
		{
			Name:    "mysql.TypeVarString",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeVarStringNotNull},
			Res:     []byte{},
		},
		{
			Name:    "mysql.TypeString",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeStringNotNull},
			Res:     []byte{},
		},
		{
			Name:    "mysql.TypeBit",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeBitNotNull},
			Res:     uint64(0),
		},
		// BLOB, TEXT, GEOMETRY or JSON column can't have a default value
		{
			Name:    "mysql.TypeJSON",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeJSONNotNull},
			Res:     "null",
		},
		{
			Name:    "mysql.TypeEnum + notnull + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeEnumNotNull},
			// TypeEnum value will be a string and then translate to []byte
			// NotNull && no default will choose first element
			Res: uint64(1),
		},
		{
			Name: "mysql.TypeEnum + null",
			ColInfo: timodel.ColumnInfo{
				FieldType: *ftTypeEnumNull,
			},
			Res: nil,
		},
		{
			Name:    "mysql.TypeSet + notnull",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeSetNotNull},
			Res:     uint64(0),
		},
		{
			Name:    "mysql.TypeGeometry",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeGeometryNotNull},
			Res:     nil, // not support yet
		},
	}

	tz, err := util.GetTimezone(config.GetGlobalServerConfig().TZ)
	require.NoError(t, err)
	for _, tc := range testCases {
		_, val, _, _, _ := getDefaultOrZeroValue(&tc.ColInfo, tz)
		require.Equal(t, tc.Res, val, tc.Name)
	}

	colInfo := timodel.ColumnInfo{
		OriginDefaultValue: "-3.14", // no float
		FieldType:          *ftTypeNewDecimalNotNull,
	}
	_, val, _, _, _ := getDefaultOrZeroValue(&colInfo, tz)
	decimal := new(types.MyDecimal)
	err = decimal.FromString([]byte("-3.14"))
	require.NoError(t, err)
	require.Equal(t, decimal.String(), val, "mysql.TypeNewDecimal + notnull + default")

	colInfo = timodel.ColumnInfo{
		OriginDefaultValue: "2020-11-19 12:12:12",
		FieldType:          *ftTypeTimestampNotNull,
	}
	_, val, _, _, _ = getDefaultOrZeroValue(&colInfo, tz)
	expected, err := types.ParseTimeFromFloatString(
		types.DefaultStmtNoWarningContext,
		"2020-11-19 20:12:12", colInfo.FieldType.GetType(), colInfo.FieldType.GetDecimal())
	require.NoError(t, err)
	require.Equal(t, expected.String(), val, "mysql.TypeTimestamp + notnull + default")

	colInfo = timodel.ColumnInfo{
		OriginDefaultValue: "2020-11-19 12:12:12",
		FieldType:          *ftTypeTimestampNull,
	}
	_, val, _, _, _ = getDefaultOrZeroValue(&colInfo, tz)
	expected, err = types.ParseTimeFromFloatString(
		types.DefaultStmtNoWarningContext,
		"2020-11-19 20:12:12", colInfo.FieldType.GetType(), colInfo.FieldType.GetDecimal())
	require.NoError(t, err)
	require.Equal(t, expected.String(), val, "mysql.TypeTimestamp + null + default")

	colInfo = timodel.ColumnInfo{
		OriginDefaultValue: "e1",
		FieldType:          *ftTypeEnumNotNull,
	}
	_, val, _, _, _ = getDefaultOrZeroValue(&colInfo, tz)
	expectedEnum, err := types.ParseEnumName(colInfo.FieldType.GetElems(), "e1", colInfo.FieldType.GetCollate())
	require.NoError(t, err)
	require.Equal(t, expectedEnum.Value, val, "mysql.TypeEnum + notnull + default")

	colInfo = timodel.ColumnInfo{
		OriginDefaultValue: "1,e",
		FieldType:          *ftTypeSetNotNull,
	}
	_, val, _, _, _ = getDefaultOrZeroValue(&colInfo, tz)
	expectedSet, err := types.ParseSetName(colInfo.FieldType.GetElems(), "1,e", colInfo.FieldType.GetCollate())
	require.NoError(t, err)
	require.Equal(t, expectedSet.Value, val, "mysql.TypeSet + notnull + default")
}

func TestE2ERowLevelChecksum(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Integrity.IntegrityCheckLevel = integrity.CheckLevelCorrectness
	replicaConfig.Integrity.CorruptionHandleLevel = integrity.CorruptionHandleLevelError
	helper := NewSchemaTestHelperWithReplicaConfig(t, replicaConfig)
	defer helper.Close()

	// upstream TiDB enable checksum functionality
	helper.Tk().MustExec("set global tidb_enable_row_level_checksum = 1")
	helper.Tk().MustExec("use test")

	createTableSQL := `create table t (
   id          int primary key auto_increment,

   c_tinyint   tinyint   null,
   c_smallint  smallint  null,
   c_mediumint mediumint null,
   c_int       int       null,
   c_bigint    bigint    null,

   c_unsigned_tinyint   tinyint   unsigned null,
   c_unsigned_smallint  smallint  unsigned null,
   c_unsigned_mediumint mediumint unsigned null,
   c_unsigned_int       int       unsigned null,
   c_unsigned_bigint    bigint    unsigned null,

   c_float   float   null,
   c_double  double  null,
   c_decimal decimal null,
   c_decimal_2 decimal(10, 4) null,

   c_unsigned_float     float unsigned   null,
   c_unsigned_double    double unsigned  null,
   c_unsigned_decimal   decimal unsigned null,
   c_unsigned_decimal_2 decimal(10, 4) unsigned null,

   c_date      date      null,
   c_datetime  datetime  null,
   c_timestamp timestamp null,
   c_time      time      null,
   c_year      year      null,

   c_tinytext   tinytext      null,
   c_text       text          null,
   c_mediumtext mediumtext    null,
   c_longtext   longtext      null,

   c_tinyblob   tinyblob      null,
   c_blob       blob          null,
   c_mediumblob mediumblob    null,
   c_longblob   longblob      null,

   c_char       char(16)      null,
   c_varchar    varchar(16)   null,
   c_binary     binary(16)    null,
   c_varbinary  varbinary(16) null,

   c_enum enum ('a','b','c') null,
   c_set  set ('a','b','c')  null,
   c_bit  bit(64)            null,
   c_json json               null,

-- gbk dmls
   name varchar(128) CHARACTER SET gbk,
   country char(32) CHARACTER SET gbk,
   city varchar(64),
   description text CHARACTER SET gbk,
   image tinyblob
);`
	_ = helper.DDL2Event(createTableSQL)

	insertDataSQL := `insert into t values (
     2,
     1, 2, 3, 4, 5,
     1, 2, 3, 4, 5,
     2020.0202, 2020.0303,
  	 2020.0404, 2021.1208,
     3.1415, 2.7182, 8000, 179394.233,
     '2020-02-20', '2020-02-20 02:20:20', '2020-02-20 02:20:20', '02:20:20', '2020',
     '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A',
     x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
     '89504E470D0A1A0A', '89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
     'b', 'b,c', b'1000001', '{
"key1": "value1",
"key2": "value2",
"key3": "123"
}',
     '测试', "中国", "上海", "你好,世界", 0xC4E3BAC3CAC0BDE7
);`

	event := helper.DML2Event(insertDataSQL, "test", "t")
	require.NotNil(t, event)
	require.False(t, event.Checksum.Corrupted)

	// avro encoder enable checksum functionality.
	codecConfig := codecCommon.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true
	codecConfig.EnableRowChecksum = true
	codecConfig.AvroDecimalHandlingMode = "string"
	codecConfig.AvroBigintUnsignedHandlingMode = "string"

	ctx := context.Background()
	avroEncoder, err := avro.SetupEncoderAndSchemaRegistry4Testing(ctx, codecConfig)
	defer avro.TeardownEncoderAndSchemaRegistry4Testing()
	require.NoError(t, err)

	topic := "test.t"

	err = avroEncoder.AppendRowChangedEvent(ctx, topic, event, func() {})
	require.NoError(t, err)
	msg := avroEncoder.Build()
	require.Len(t, msg, 1)

	schemaM, err := avro.NewConfluentSchemaManager(
		ctx, "http://127.0.0.1:8081", nil)
	require.NoError(t, err)

	// decoder enable checksum functionality.
	decoder := avro.NewDecoder(codecConfig, schemaM, topic, nil)
	err = decoder.AddKeyValue(msg[0].Key, msg[0].Value)
	require.NoError(t, err)

	messageType, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, model.MessageTypeRow, messageType)

	event, err = decoder.NextRowChangedEvent()
	// no error, checksum verification passed.
	require.NoError(t, err)
}

func TestVerifyChecksumHasNullFields(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Integrity.IntegrityCheckLevel = integrity.CheckLevelCorrectness
	replicaConfig.Integrity.CorruptionHandleLevel = integrity.CorruptionHandleLevelError

	helper := NewSchemaTestHelperWithReplicaConfig(t, replicaConfig)
	defer helper.Close()

	helper.Tk().MustExec("set global tidb_enable_row_level_checksum = 1")
	helper.Tk().MustExec("use test")

	_ = helper.DDL2Event(`CREATE table t (a int primary key, b int, c int)`)
	event := helper.DML2Event(`INSERT INTO t VALUES (1, NULL, NULL)`, "test", "t")
	require.NotNil(t, event)

	event = helper.DML2Event(`INSERT INTO t VALUES (2, 2, NULL)`, "test", "t")
	require.NotNil(t, event)

	event = helper.DML2Event(`INSERT INTO t VALUES (3, NULL, 3)`, "test", "t")
	require.NotNil(t, event)
}

func TestChecksumAfterAlterSetDefaultValue(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Integrity.IntegrityCheckLevel = integrity.CheckLevelCorrectness
	replicaConfig.Integrity.CorruptionHandleLevel = integrity.CorruptionHandleLevelError

	helper := NewSchemaTestHelperWithReplicaConfig(t, replicaConfig)
	defer helper.Close()

	helper.Tk().MustExec("set global tidb_enable_row_level_checksum = 1")
	helper.Tk().MustExec("use test")

	_ = helper.DDL2Event("create table t (a int primary key, b int default 1)")
	event := helper.DML2Event("insert into t (a) values (1)", "test", "t")
	require.NotNil(t, event)

	tableInfo, ok := helper.schemaStorage.GetLastSnapshot().TableByName("test", "t")
	require.True(t, ok)
	_, oldValue := helper.getLastKeyValue(tableInfo.ID)

	_ = helper.DDL2Event("alter table t modify column b int default 2")
	helper.Tk().MustExec("update t set b = 10 where a = 1")
	key, value := helper.getLastKeyValue(tableInfo.ID)

	ts := helper.schemaStorage.GetLastSnapshot().CurrentTs()
	rawKV := &model.RawKVEntry{
		OpType:   model.OpTypePut,
		Key:      key,
		Value:    value,
		OldValue: oldValue,
		StartTs:  ts - 1,
		CRTs:     ts + 1,
	}
	polymorphicEvent := model.NewPolymorphicEvent(rawKV)
	err := helper.mounter.DecodeEvent(context.Background(), polymorphicEvent)
	require.NoError(t, err)
	require.NotNil(t, polymorphicEvent.Row)
}

func TestTimezoneDefaultValue(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Integrity.IntegrityCheckLevel = integrity.CheckLevelCorrectness
	replicaConfig.Integrity.CorruptionHandleLevel = integrity.CorruptionHandleLevelError

	helper := NewSchemaTestHelperWithReplicaConfig(t, replicaConfig)
	defer helper.Close()

	helper.Tk().MustExec("set global tidb_enable_row_level_checksum = 1")
	helper.Tk().MustExec("use test")

	_ = helper.DDL2Event(`create table test.t(a int primary key, b timestamp default '2023-02-09 13:00:00')`)
	insertEvent := helper.DML2Event(`insert into test.t(a) values (1)`, "test", "t")
	require.NotNil(t, insertEvent)
	require.Equal(t, "2023-02-09 13:00:00", insertEvent.Columns[1].Value.(string))
}

func TestChecksumAfterAddColumns(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Integrity.IntegrityCheckLevel = integrity.CheckLevelCorrectness
	replicaConfig.Integrity.CorruptionHandleLevel = integrity.CorruptionHandleLevelError

	helper := NewSchemaTestHelperWithReplicaConfig(t, replicaConfig)
	defer helper.Close()

	helper.Tk().MustExec("set global tidb_enable_row_level_checksum = 1")
	helper.Tk().MustExec("use test")

	_ = helper.DDL2Event("create table t (a int primary key)")
	event := helper.DML2Event("insert into t values (1)", "test", "t")
	require.NotNil(t, event)

	_ = helper.DDL2Event("alter table t add column b int default 1")

	tableInfo, ok := helper.schemaStorage.GetLastSnapshot().TableByName("test", "t")
	require.True(t, ok)
	_, oldValue := helper.getLastKeyValue(tableInfo.ID)

	helper.Tk().MustExec("update t set b = 10 where a = 1")
	key, value := helper.getLastKeyValue(tableInfo.ID)

	ts := helper.schemaStorage.GetLastSnapshot().CurrentTs()
	rawKV := &model.RawKVEntry{
		OpType:   model.OpTypePut,
		Key:      key,
		Value:    value,
		OldValue: oldValue,
		StartTs:  ts - 1,
		CRTs:     ts + 1,
	}
	polymorphicEvent := model.NewPolymorphicEvent(rawKV)
	err := helper.mounter.DecodeEvent(context.Background(), polymorphicEvent)
	require.NoError(t, err)
	require.NotNil(t, polymorphicEvent.Row)
}

func TestChecksumAfterDropColumns(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Integrity.IntegrityCheckLevel = integrity.CheckLevelCorrectness
	replicaConfig.Integrity.CorruptionHandleLevel = integrity.CorruptionHandleLevelError

	helper := NewSchemaTestHelperWithReplicaConfig(t, replicaConfig)
	defer helper.Close()

	helper.Tk().MustExec("set global tidb_enable_row_level_checksum = 1")
	helper.Tk().MustExec("use test")

	_ = helper.DDL2Event("create table t (a int primary key, b int, c int)")
	event := helper.DML2Event("insert into t values (1, 2, 3)", "test", "t")
	require.NotNil(t, event)

	tableInfo, ok := helper.schemaStorage.GetLastSnapshot().TableByName("test", "t")
	require.True(t, ok)
	_, oldValue := helper.getLastKeyValue(tableInfo.ID)

	_ = helper.DDL2Event("alter table t drop column b")

	helper.Tk().MustExec("update t set c = 10 where a = 1")
	key, value := helper.getLastKeyValue(tableInfo.ID)

	ts := helper.schemaStorage.GetLastSnapshot().CurrentTs()
	rawKV := &model.RawKVEntry{
		OpType:   model.OpTypePut,
		Key:      key,
		Value:    value,
		OldValue: oldValue,
		StartTs:  ts - 1,
		CRTs:     ts + 1,
	}
	polymorphicEvent := model.NewPolymorphicEvent(rawKV)
	err := helper.mounter.DecodeEvent(context.Background(), polymorphicEvent)
	require.NoError(t, err)
	require.NotNil(t, polymorphicEvent.Row)
}

func TestVerifyChecksumNonIntegerPrimaryKey(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Integrity.IntegrityCheckLevel = integrity.CheckLevelCorrectness
	replicaConfig.Integrity.CorruptionHandleLevel = integrity.CorruptionHandleLevelError

	helper := NewSchemaTestHelperWithReplicaConfig(t, replicaConfig)
	defer helper.Close()

	helper.Tk().MustExec("set global tidb_enable_row_level_checksum = 1")
	helper.Tk().MustExec("use test")

	// primary key is not integer type, so all column encoded into value bytes
	_ = helper.DDL2Event(`CREATE table t (a varchar(10) primary key, b int)`)
	event := helper.DML2Event(`INSERT INTO t VALUES ("abc", 3)`, "test", "t")
	require.NotNil(t, event)
}

func TestVerifyChecksumTime(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Integrity.IntegrityCheckLevel = integrity.CheckLevelCorrectness
	replicaConfig.Integrity.CorruptionHandleLevel = integrity.CorruptionHandleLevelError

	helper := NewSchemaTestHelperWithReplicaConfig(t, replicaConfig)
	defer helper.Close()

	helper.Tk().MustExec("set global tidb_enable_row_level_checksum = 1")
	helper.Tk().MustExec("use test")

	helper.Tk().MustExec("set global time_zone = '-5:00'")
	_ = helper.DDL2Event(`CREATE table TBL2 (a int primary key, b TIMESTAMP)`)
	event := helper.DML2Event(`INSERT INTO TBL2 VALUES (3, '2023-02-09 13:00:00')`, "test", "TBL2")
	require.NotNil(t, event)

	_ = helper.DDL2Event("create table t (a timestamp primary key, b int)")
	event = helper.DML2Event("insert into t values ('2023-02-09 13:00:00', 3)", "test", "t")
	require.NotNil(t, event)
}

func TestDecodeRow(t *testing.T) {
	helper := NewSchemaTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("set @@tidb_enable_clustered_index=1;")
	helper.Tk().MustExec("use test;")

	changefeed := model.DefaultChangeFeedID("changefeed-test-decode-row")

	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.NoError(t, err)

	cfg := config.GetDefaultReplicaConfig()

	filter, err := filter.NewFilter(cfg, "")
	require.NoError(t, err)

	schemaStorage, err := NewSchemaStorage(helper.Storage(),
		ver.Ver, false, changefeed, util.RoleTester, filter)
	require.NoError(t, err)

	// apply ddl to schemaStorage
	ddl := "create table test.student(id int primary key, name char(50), age int, gender char(10))"
	job := helper.DDL2Job(ddl)
	err = schemaStorage.HandleDDLJob(job)
	require.NoError(t, err)

	ts := schemaStorage.GetLastSnapshot().CurrentTs()

	schemaStorage.AdvanceResolvedTs(ver.Ver)

	mounter := NewMounter(schemaStorage, changefeed, time.Local, filter, cfg.Integrity).(*mounter)

	helper.Tk().MustExec(`insert into student values(1, "dongmen", 20, "male")`)
	helper.Tk().MustExec(`update student set age = 27 where id = 1`)

	ctx := context.Background()
	decodeAndCheckRowInTable := func(tableID int64, f func(key []byte, value []byte) *model.RawKVEntry) {
		walkTableSpanInStore(t, helper.Storage(), tableID, func(key []byte, value []byte) {
			rawKV := f(key, value)

			row, err := mounter.unmarshalAndMountRowChanged(ctx, rawKV)
			require.NoError(t, err)
			require.NotNil(t, row)

			if row.Columns != nil {
				require.NotNil(t, mounter.decoder)
			}

			if row.PreColumns != nil {
				require.NotNil(t, mounter.preDecoder)
			}
		})
	}

	toRawKV := func(key []byte, value []byte) *model.RawKVEntry {
		return &model.RawKVEntry{
			OpType:  model.OpTypePut,
			Key:     key,
			Value:   value,
			StartTs: ts - 1,
			CRTs:    ts + 1,
		}
	}

	tableInfo, ok := schemaStorage.GetLastSnapshot().TableByName("test", "student")
	require.True(t, ok)

	decodeAndCheckRowInTable(tableInfo.ID, toRawKV)
	decodeAndCheckRowInTable(tableInfo.ID, toRawKV)

	job = helper.DDL2Job("drop table student")
	err = schemaStorage.HandleDDLJob(job)
	require.NoError(t, err)
}

// TestDecodeEventIgnoreRow tests a PolymorphicEvent.Row is nil
// if this event should be filter out by filter.
func TestDecodeEventIgnoreRow(t *testing.T) {
	helper := NewSchemaTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test;")

	ddls := []string{
		"create table test.student(id int primary key, name char(50), age int, gender char(10))",
		"create table test.computer(id int primary key, brand char(50), price int)",
		"create table test.poet(id int primary key, name char(50), works char(100))",
	}

	cfID := model.DefaultChangeFeedID("changefeed-test-ignore-event")

	cfg := config.GetDefaultReplicaConfig()
	cfg.Filter.Rules = []string{"test.student", "test.computer"}
	f, err := filter.NewFilter(cfg, "")
	require.Nil(t, err)
	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)

	schemaStorage, err := NewSchemaStorage(helper.Storage(),
		ver.Ver, false, cfID, util.RoleTester, f)
	require.Nil(t, err)
	// apply ddl to schemaStorage
	for _, ddl := range ddls {
		job := helper.DDL2Job(ddl)
		err = schemaStorage.HandleDDLJob(job)
		require.Nil(t, err)
	}

	ts := schemaStorage.GetLastSnapshot().CurrentTs()
	schemaStorage.AdvanceResolvedTs(ver.Ver)
	mounter := NewMounter(schemaStorage, cfID, time.Local, f, cfg.Integrity).(*mounter)

	type testCase struct {
		schema  string
		table   string
		columns []interface{}
		ignored bool
	}

	testCases := []testCase{
		{
			schema:  "test",
			table:   "student",
			columns: []interface{}{1, "dongmen", 20, "male"},
			ignored: false,
		},
		{
			schema:  "test",
			table:   "computer",
			columns: []interface{}{1, "apple", 19999},
			ignored: false,
		},
		// This case should be ignored by its table name.
		{
			schema:  "test",
			table:   "poet",
			columns: []interface{}{1, "李白", "静夜思"},
			ignored: true,
		},
	}

	ignoredTables := make([]string, 0)
	tables := make([]string, 0)
	for _, tc := range testCases {
		tableInfo, ok := schemaStorage.GetLastSnapshot().TableByName(tc.schema, tc.table)
		require.True(t, ok)
		// TODO: add other dml event type
		insertSQL := prepareInsertSQL(t, tableInfo, len(tc.columns))
		if tc.ignored {
			ignoredTables = append(ignoredTables, tc.table)
		} else {
			tables = append(tables, tc.table)
		}
		helper.tk.MustExec(insertSQL, tc.columns...)
	}
	ctx := context.Background()

	decodeAndCheckRowInTable := func(tableID int64, f func(key []byte, value []byte) *model.RawKVEntry) int {
		var rows int
		walkTableSpanInStore(t, helper.Storage(), tableID, func(key []byte, value []byte) {
			rawKV := f(key, value)
			pEvent := model.NewPolymorphicEvent(rawKV)
			err := mounter.DecodeEvent(ctx, pEvent)
			require.Nil(t, err)
			if pEvent.Row == nil {
				return
			}
			row := pEvent.Row
			rows++
			require.Equal(t, row.TableInfo.GetSchemaName(), "test")
			// Now we only allow filter dml event by table, so we only check row's table.
			require.NotContains(t, ignoredTables, row.TableInfo.GetTableName())
			require.Contains(t, tables, row.TableInfo.GetTableName())
		})
		return rows
	}

	toRawKV := func(key []byte, value []byte) *model.RawKVEntry {
		return &model.RawKVEntry{
			OpType:  model.OpTypePut,
			Key:     key,
			Value:   value,
			StartTs: ts - 1,
			CRTs:    ts + 1,
		}
	}

	for _, tc := range testCases {
		tableInfo, ok := schemaStorage.GetLastSnapshot().TableByName(tc.schema, tc.table)
		require.True(t, ok)
		decodeAndCheckRowInTable(tableInfo.ID, toRawKV)
	}
}

func TestBuildTableInfo(t *testing.T) {
	cases := []struct {
		origin              string
		recovered           string
		recoveredWithNilCol string
	}{
		{
			"CREATE TABLE t1 (c INT PRIMARY KEY)",
			"CREATE TABLE `t1` (\n" +
				"  `c` int(0) NOT NULL,\n" +
				"  PRIMARY KEY (`c`(0)) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			"CREATE TABLE `t1` (\n" +
				"  `c` int(0) NOT NULL,\n" +
				"  PRIMARY KEY (`c`(0)) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
		},
		{
			"CREATE TABLE t1 (" +
				" c INT UNSIGNED," +
				" c2 VARCHAR(10) NOT NULL," +
				" c3 BIT(10) NOT NULL," +
				" UNIQUE KEY (c2, c3)" +
				")",
			// CDC discards field length.
			"CREATE TABLE `t1` (\n" +
				"  `c` int(0) unsigned DEFAULT NULL,\n" +
				"  `c2` varchar(0) NOT NULL,\n" +
				"  `c3` bit(0) NOT NULL,\n" +
				"  UNIQUE KEY `idx_0` (`c2`(0),`c3`(0))\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			"CREATE TABLE `t1` (\n" +
				"  `omitted` unspecified GENERATED ALWAYS AS (pass_generated_check) VIRTUAL,\n" +
				"  `c2` varchar(0) NOT NULL,\n" +
				"  `c3` bit(0) NOT NULL,\n" +
				"  UNIQUE KEY `idx_0` (`c2`(0),`c3`(0))\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
		},
		{
			"CREATE TABLE t1 (" +
				" c INT UNSIGNED," +
				" gen INT AS (c+1) VIRTUAL," +
				" c2 VARCHAR(10) NOT NULL," +
				" gen2 INT AS (c+2) STORED," +
				" c3 BIT(10) NOT NULL," +
				" PRIMARY KEY (c, c2)" +
				")",
			// CDC discards virtual generated column, and generating expression of stored generated column.
			"CREATE TABLE `t1` (\n" +
				"  `c` int(0) unsigned NOT NULL,\n" +
				"  `c2` varchar(0) NOT NULL,\n" +
				"  `gen2` int(0) GENERATED ALWAYS AS (pass_generated_check) STORED,\n" +
				"  `c3` bit(0) NOT NULL,\n" +
				"  PRIMARY KEY (`c`(0),`c2`(0)) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			"CREATE TABLE `t1` (\n" +
				"  `c` int(0) unsigned NOT NULL,\n" +
				"  `c2` varchar(0) NOT NULL,\n" +
				"  `omitted` unspecified GENERATED ALWAYS AS (pass_generated_check) VIRTUAL,\n" +
				"  `omitted` unspecified GENERATED ALWAYS AS (pass_generated_check) VIRTUAL,\n" +
				"  PRIMARY KEY (`c`(0),`c2`(0)) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
		},
		{
			"CREATE TABLE `t1` (" +
				"  `a` int(11) NOT NULL," +
				"  `b` int(11) DEFAULT NULL," +
				"  `c` int(11) DEFAULT NULL," +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */," +
				"  UNIQUE KEY `b` (`b`)" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			"CREATE TABLE `t1` (\n" +
				"  `a` int(0) NOT NULL,\n" +
				"  `b` int(0) DEFAULT NULL,\n" +
				"  `c` int(0) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`(0)) /*T![clustered_index] CLUSTERED */,\n" +
				"  UNIQUE KEY `idx_1` (`b`(0))\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			"CREATE TABLE `t1` (\n" +
				"  `a` int(0) NOT NULL,\n" +
				"  `omitted` unspecified GENERATED ALWAYS AS (pass_generated_check) VIRTUAL,\n" +
				"  `omitted` unspecified GENERATED ALWAYS AS (pass_generated_check) VIRTUAL,\n" +
				"  PRIMARY KEY (`a`(0)) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
		},
		{ // This case is to check the primary key is correctly identified by BuildTiDBTableInfo
			"CREATE TABLE your_table (" +
				" id INT NOT NULL," +
				" name VARCHAR(50) NOT NULL," +
				" email VARCHAR(100) NOT NULL," +
				" age INT NOT NULL ," +
				" address VARCHAR(200) NOT NULL," +
				" PRIMARY KEY (id, name)," +
				" UNIQUE INDEX idx_unique_1 (id, email, age)," +
				" UNIQUE INDEX idx_unique_2 (name, email, address)" +
				" );",
			"CREATE TABLE `your_table` (\n" +
				"  `id` int(0) NOT NULL,\n" +
				"  `name` varchar(0) NOT NULL,\n" +
				"  `email` varchar(0) NOT NULL,\n" +
				"  `age` int(0) NOT NULL,\n" +
				"  `address` varchar(0) NOT NULL,\n" +
				"  PRIMARY KEY (`id`(0),`name`(0)) /*T![clustered_index] CLUSTERED */,\n" +
				"  UNIQUE KEY `idx_1` (`id`(0),`email`(0),`age`(0)),\n" +
				"  UNIQUE KEY `idx_2` (`name`(0),`email`(0),`address`(0))\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			"CREATE TABLE `your_table` (\n" +
				"  `id` int(0) NOT NULL,\n" +
				"  `name` varchar(0) NOT NULL,\n" +
				"  `omitted` unspecified GENERATED ALWAYS AS (pass_generated_check) VIRTUAL,\n" +
				"  `omitted` unspecified GENERATED ALWAYS AS (pass_generated_check) VIRTUAL,\n" +
				"  `omitted` unspecified GENERATED ALWAYS AS (pass_generated_check) VIRTUAL,\n" +
				"  PRIMARY KEY (`id`(0),`name`(0)) /*T![clustered_index] CLUSTERED */,\n" +
				"  UNIQUE KEY `idx_1` (`id`(0),`omitted`(0),`omitted`(0)),\n" +
				"  UNIQUE KEY `idx_2` (`name`(0),`omitted`(0),`omitted`(0))\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
		},
	}
	tz, err := util.GetTimezone(config.GetGlobalServerConfig().TZ)
	require.NoError(t, err)
	p := parser.New()
	for i, c := range cases {
		stmt, err := p.ParseOneStmt(c.origin, "", "")
		require.NoError(t, err)
		originTI, err := ddl.BuildTableInfoFromAST(metabuild.NewContext(), stmt.(*ast.CreateTableStmt))
		require.NoError(t, err)
		cdcTableInfo := model.WrapTableInfo(0, "test", 0, originTI)
		colDatas, _, _, err := datum2Column(cdcTableInfo, map[int64]types.Datum{}, tz)
		require.NoError(t, err)
		e := model.RowChangedEvent{
			TableInfo: cdcTableInfo,
			Columns:   colDatas,
		}
		cols := e.GetColumns()
		recoveredTI := model.BuildTiDBTableInfo(cdcTableInfo.TableName.Table, cols, cdcTableInfo.IndexColumnsOffset)
		handle := sqlmodel.GetWhereHandle(recoveredTI, recoveredTI)
		require.NotNil(t, handle.UniqueNotNullIdx)
		require.Equal(t, c.recovered, showCreateTable(t, recoveredTI))
		// make sure BuildTiDBTableInfo indentify the correct primary key
		if i == 5 {
			inexes := recoveredTI.Indices
			primaryCount := 0
			for i := range inexes {
				if inexes[i].Primary {
					primaryCount++
				}
			}
			require.Equal(t, 1, primaryCount)
			require.Equal(t, 2, len(handle.UniqueNotNullIdx.Columns))
		}
		// mimic the columns are set to nil when old value feature is disabled
		for i := range cols {
			if !cols[i].Flag.IsHandleKey() {
				cols[i] = nil
			}
		}
		recoveredTI = model.BuildTiDBTableInfo(cdcTableInfo.TableName.Table, cols, cdcTableInfo.IndexColumnsOffset)
		handle = sqlmodel.GetWhereHandle(recoveredTI, recoveredTI)
		require.NotNil(t, handle.UniqueNotNullIdx)
		require.Equal(t, c.recoveredWithNilCol, showCreateTable(t, recoveredTI))
	}
}

var tiCtx = mock.NewContext()

func showCreateTable(t *testing.T, ti *timodel.TableInfo) string {
	result := bytes.NewBuffer(make([]byte, 0, 512))
	err := executor.ConstructResultOfShowCreateTable(tiCtx, ti, autoid.Allocators{}, result)
	require.NoError(t, err)
	return result.String()
}

func TestNewDMRowChange(t *testing.T) {
	cases := []struct {
		origin    string
		recovered string
	}{
		{
			"CREATE TABLE t1 (id INT," +
				" a1 INT NOT NULL," +
				" a3 INT NOT NULL," +
				" UNIQUE KEY dex1(a1, a3));",
			"CREATE TABLE `t1` (\n" +
				"  `id` int(0) DEFAULT NULL,\n" +
				"  `a1` int(0) NOT NULL,\n" +
				"  `a3` int(0) NOT NULL,\n" +
				"  UNIQUE KEY `idx_0` (`a1`(0),`a3`(0))\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
		},
	}
	p := parser.New()
	for _, c := range cases {
		stmt, err := p.ParseOneStmt(c.origin, "", "")
		require.NoError(t, err)
		originTI, err := ddl.BuildTableInfoFromAST(metabuild.NewContext(), stmt.(*ast.CreateTableStmt))
		require.NoError(t, err)
		cdcTableInfo := model.WrapTableInfo(0, "test", 0, originTI)
		cols := []*model.Column{
			{
				Name: "id", Type: 3, Charset: "binary", Flag: 65, Value: 1, Default: nil,
			},
			{
				Name: "a1", Type: 3, Charset: "binary", Flag: 51, Value: 1, Default: nil,
			},
			{
				Name: "a3", Type: 3, Charset: "binary", Flag: 51, Value: 2, Default: nil,
			},
		}
		recoveredTI := model.BuildTiDBTableInfo(cdcTableInfo.TableName.Table, cols, cdcTableInfo.IndexColumnsOffset)
		require.Equal(t, c.recovered, showCreateTable(t, recoveredTI))
		tableName := &model.TableName{Schema: "db", Table: "t1"}
		rowChange := sqlmodel.NewRowChange(tableName, nil, []interface{}{1, 1, 2}, nil, recoveredTI, nil, nil)
		sqlGot, argsGot := rowChange.GenSQL(sqlmodel.DMLDelete)
		require.Equal(t, "DELETE FROM `db`.`t1` WHERE `a1` = ? AND `a3` = ? LIMIT 1", sqlGot)
		require.Equal(t, []interface{}{1, 2}, argsGot)

		sqlGot, argsGot = sqlmodel.GenDeleteSQL(rowChange, rowChange)
		require.Equal(t, "DELETE FROM `db`.`t1` WHERE (`a1` = ? AND `a3` = ?) OR (`a1` = ? AND `a3` = ?)", sqlGot)
		require.Equal(t, []interface{}{1, 2, 1, 2}, argsGot)
	}
}

func TestFormatColVal(t *testing.T) {
	t.Parallel()

	ftTypeFloatNotNull := types.NewFieldType(mysql.TypeFloat)
	ftTypeFloatNotNull.SetFlag(mysql.NotNullFlag)
	col := &timodel.ColumnInfo{FieldType: *ftTypeFloatNotNull}

	var datum types.Datum

	datum.SetFloat32(123.99)
	value, _, _, err := formatColVal(datum, col)
	require.NoError(t, err)
	require.EqualValues(t, float32(123.99), value)

	datum.SetFloat32(float32(math.NaN()))
	value, _, warn, err := formatColVal(datum, col)
	require.NoError(t, err)
	require.Equal(t, float32(0), value)
	require.NotZero(t, warn)

	datum.SetFloat32(float32(math.Inf(1)))
	value, _, warn, err = formatColVal(datum, col)
	require.NoError(t, err)
	require.Equal(t, float32(0), value)
	require.NotZero(t, warn)

	datum.SetFloat32(float32(math.Inf(-1)))
	value, _, warn, err = formatColVal(datum, col)
	require.NoError(t, err)
	require.Equal(t, float32(0), value)
	require.NotZero(t, warn)

	vector, _ := types.ParseVectorFloat32("[1,2,3,4,5]")
	ftTypeVector := types.NewFieldType(mysql.TypeTiDBVectorFloat32)
	col = &timodel.ColumnInfo{FieldType: *ftTypeVector}
	datum.SetVectorFloat32(vector)
	value, _, warn, err = formatColVal(datum, col)
	require.NoError(t, err)
	require.Equal(t, vector, value)
	require.Zero(t, warn)
}

func TestXxx(t *testing.T) {
	job := &timodel.Job{}

	// v := "CAQIAAgGAgIyCAgCBjExMggKAshHeyJpZCI6MTEzLCJ0eXBlIjoyMSwic2NoZW1hX2lkIjoyLCJ0YWJsZV9pZCI6MTEyLCJzY2hlbWFfbmFtZSI6InRlc3QiLCJ0YWJsZV9uYW1lIjoidl90MiIsInN0YXRlIjo0LCJ3YXJuaW5nIjpudWxsLCJlcnIiOm51bGwsImVycl9jb3VudCI6MCwicm93X2NvdW50IjowLCJyYXdfYXJncyI6eyJ0YWJsZV9pbmZvIjp7ImlkIjoxMTIsIm5hbWUiOnsiTyI6InZfdDIiLCJMIjoidl90MiJ9LCJjaGFyc2V0IjoidXRmOG1iNCIsImNvbGxhdGUiOiJ1dGY4bWI0XzA5MDBfYWlfY2kiLCJjb2xzIjpbeyJpZCI6MSwibmFtZSI6eyJPIjoiaWQiLCJMIjoiaWQifSwib2Zmc2V0IjowLCJvcmlnaW5fZGVmYXVsdCI6bnVsbCwib3JpZ2luX2RlZmF1bHRfYml0IjpudWxsLCJkZWZhdWx0IjpudWxsLCJkZWZhdWx0X2JpdCI6bnVsbCwiZGVmYXVsdF9pc19leHByIjpmYWxzZSwiZ2VuZXJhdGVkX2V4cHJfc3RyaW5nIjoiIiwiZ2VuZXJhdGVkX3N0b3JlZCI6ZmFsc2UsImRlcGVuZGVuY2VzIjpudWxsLCJ0eXBlIjp7IlRwIjowLCJGbGFnIjowLCJGbGVuIjowLCJEZWNpbWFsIjowLCJDaGFyc2V0IjoiIiwiQ29sbGF0ZSI6IiIsIkVsZW1zIjpudWxsLCJFbGVtc0lzQmluYXJ5TGl0IjpudWxsLCJBcnJheSI6ZmFsc2V9LCJzdGF0ZSI6NSwiY29tbWVudCI6IiIsImhpZGRlbiI6ZmFsc2UsImNoYW5nZV9zdGF0ZV9pbmZvIjpudWxsLCJ2ZXJzaW9uIjowfSx7ImlkIjoyLCJuYW1lIjp7Ik8iOiJrIiwiTCI6ImsifSwib2Zmc2V0IjoxLCJvcmlnaW5fZGVmYXVsdCI6bnVsbCwib3JpZ2luX2RlZmF1bHRfYml0IjpudWxsLCJkZWZhdWx0IjpudWxsLCJkZWZhdWx0X2JpdCI6bnVsbCwiZGVmYXVsdF9pc19leHByIjpmYWxzZSwiZ2VuZXJhdGVkX2V4cHJfc3RyaW5nIjoiIiwiZ2VuZXJhdGVkX3N0b3JlZCI6ZmFsc2UsImRlcGVuZGVuY2VzIjpudWxsLCJ0eXBlIjp7IlRwIjowLCJGbGFnIjowLCJGbGVuIjowLCJEZWNpbWFsIjowLCJDaGFyc2V0IjoiIiwiQ29sbGF0ZSI6IiIsIkVsZW1zIjpudWxsLCJFbGVtc0lzQmluYXJ5TGl0IjpudWxsLCJBcnJheSI6ZmFsc2V9LCJzdGF0ZSI6NSwiY29tbWVudCI6IiIsImhpZGRlbiI6ZmFsc2UsImNoYW5nZV9zdGF0ZV9pbmZvIjpudWxsLCJ2ZXJzaW9uIjowfV0sImluZGV4X2luZm8iOm51bGwsImNvbnN0cmFpbnRfaW5mbyI6bnVsbCwiZmtfaW5mbyI6bnVsbCwic3RhdGUiOjUsInBrX2lzX2hhbmRsZSI6ZmFsc2UsImlzX2NvbW1vbl9oYW5kbGUiOmZhbHNlLCJjb21tb25faGFuZGxlX3ZlcnNpb24iOjAsImNvbW1lbnQiOiIiLCJhdXRvX2luY19pZCI6MCwiYXV0b19pZF9jYWNoZSI6MCwiYXV0b19yYW5kX2lkIjowLCJtYXhfY29sX2lkIjoyLCJtYXhfaWR4X2lkIjowLCJtYXhfZmtfaWQiOjAsIm1heF9jc3RfaWQiOjAsInVwZGF0ZV90aW1lc3RhbXAiOjQ1NDQxODg2MTQwMzQwNjM0NiwiU2hhcmRSb3dJREJpdHMiOjAsIm1heF9zaGFyZF9yb3dfaWRfYml0cyI6MCwiYXV0b19yYW5kb21fYml0cyI6MCwiYXV0b19yYW5kb21fcmFuZ2VfYml0cyI6MCwicHJlX3NwbGl0X3JlZ2lvbnMiOjAsInBhcnRpdGlvbiI6bnVsbCwiY29tcHJlc3Npb24iOiIiLCJ2aWV3Ijp7InZpZXdfYWxnb3JpdGhtIjowLCJ2aWV3X2RlZmluZXIiOnsiVXNlcm5hbWUiOiJyb290IiwiSG9zdG5hbWUiOiIxMjcuMC4wLjEiLCJDdXJyZW50VXNlciI6ZmFsc2UsIkF1dGhVc2VybmFtZSI6InJvb3QiLCJBdXRoSG9zdG5hbWUiOiIlIiwiQXV0aFBsdWdpbiI6Im15c3FsX25hdGl2ZV9wYXNzd29yZCJ9LCJ2aWV3X3NlY3VyaXR5IjowLCJ2aWV3X3NlbGVjdCI6IlNFTEVDVCBgaWRgIEFTIGBpZGAsYGtgIEFTIGBrYCBGUk9NIGB0ZXN0YC5gdDFgIiwidmlld19jaGVja29wdGlvbiI6MSwidmlld19jb2xzIjpudWxsfSwic2VxdWVuY2UiOm51bGwsIkxvY2siOm51bGwsInZlcnNpb24iOjUsInRpZmxhc2hfcmVwbGljYSI6bnVsbCwiaXNfY29sdW1uYXIiOmZhbHNlLCJ0ZW1wX3RhYmxlX3R5cGUiOjAsImNhY2hlX3RhYmxlX3N0YXR1cyI6MCwicG9saWN5X3JlZl9pbmZvIjpudWxsLCJzdGF0c19vcHRpb25zIjpudWxsLCJleGNoYW5nZV9wYXJ0aXRpb25faW5mbyI6bnVsbCwidHRsX2luZm8iOm51bGwsInJldmlzaW9uIjowfSwiZmtfY2hlY2siOnRydWV9LCJzY2hlbWFfc3RhdGUiOjUsInNuYXBzaG90X3ZlciI6MCwicmVhbF9zdGFydF90cyI6NDU0NDE4ODYxNDAzNDA2MzQ2LCJzdGFydF90cyI6NDU0NDE4ODYxNDAzNDA2MzQxLCJkZXBlbmRlbmN5X2lkIjowLCJxdWVyeSI6IkNSRUFURSBWSUVXIHZfdDIgQVMgU0VMRUNUIGBpZGAsIGBrYCBGUk9NIGB0MWAiLCJiaW5sb2ciOnsiU2NoZW1hVmVyc2lvbiI6NTYsIkRCSW5mbyI6bnVsbCwiVGFibGVJbmZvIjp7ImlkIjoxMTIsIm5hbWUiOnsiTyI6InZfdDIiLCJMIjoidl90MiJ9LCJjaGFyc2V0IjoidXRmOG1iNCIsImNvbGxhdGUiOiJ1dGY4bWI0XzA5MDBfYWlfY2kiLCJjb2xzIjpbeyJpZCI6MSwibmFtZSI6eyJPIjoiaWQiLCJMIjoiaWQifSwib2Zmc2V0IjowLCJvcmlnaW5fZGVmYXVsdCI6bnVsbCwib3JpZ2luX2RlZmF1bHRfYml0IjpudWxsLCJkZWZhdWx0IjpudWxsLCJkZWZhdWx0X2JpdCI6bnVsbCwiZGVmYXVsdF9pc19leHByIjpmYWxzZSwiZ2VuZXJhdGVkX2V4cHJfc3RyaW5nIjoiIiwiZ2VuZXJhdGVkX3N0b3JlZCI6ZmFsc2UsImRlcGVuZGVuY2VzIjpudWxsLCJ0eXBlIjp7IlRwIjowLCJGbGFnIjowLCJGbGVuIjowLCJEZWNpbWFsIjowLCJDaGFyc2V0IjoiIiwiQ29sbGF0ZSI6IiIsIkVsZW1zIjpudWxsLCJFbGVtc0lzQmluYXJ5TGl0IjpudWxsLCJBcnJheSI6ZmFsc2V9LCJzdGF0ZSI6NSwiY29tbWVudCI6IiIsImhpZGRlbiI6ZmFsc2UsImNoYW5nZV9zdGF0ZV9pbmZvIjpudWxsLCJ2ZXJzaW9uIjowfSx7ImlkIjoyLCJuYW1lIjp7Ik8iOiJrIiwiTCI6ImsifSwib2Zmc2V0IjoxLCJvcmlnaW5fZGVmYXVsdCI6bnVsbCwib3JpZ2luX2RlZmF1bHRfYml0IjpudWxsLCJkZWZhdWx0IjpudWxsLCJkZWZhdWx0X2JpdCI6bnVsbCwiZGVmYXVsdF9pc19leHByIjpmYWxzZSwiZ2VuZXJhdGVkX2V4cHJfc3RyaW5nIjoiIiwiZ2VuZXJhdGVkX3N0b3JlZCI6ZmFsc2UsImRlcGVuZGVuY2VzIjpudWxsLCJ0eXBlIjp7IlRwIjowLCJGbGFnIjowLCJGbGVuIjowLCJEZWNpbWFsIjowLCJDaGFyc2V0IjoiIiwiQ29sbGF0ZSI6IiIsIkVsZW1zIjpudWxsLCJFbGVtc0lzQmluYXJ5TGl0IjpudWxsLCJBcnJheSI6ZmFsc2V9LCJzdGF0ZSI6NSwiY29tbWVudCI6IiIsImhpZGRlbiI6ZmFsc2UsImNoYW5nZV9zdGF0ZV9pbmZvIjpudWxsLCJ2ZXJzaW9uIjowfV0sImluZGV4X2luZm8iOm51bGwsImNvbnN0cmFpbnRfaW5mbyI6bnVsbCwiZmtfaW5mbyI6bnVsbCwic3RhdGUiOjUsInBrX2lzX2hhbmRsZSI6ZmFsc2UsImlzX2NvbW1vbl9oYW5kbGUiOmZhbHNlLCJjb21tb25faGFuZGxlX3ZlcnNpb24iOjAsImNvbW1lbnQiOiIiLCJhdXRvX2luY19pZCI6MCwiYXV0b19pZF9jYWNoZSI6MCwiYXV0b19yYW5kX2lkIjowLCJtYXhfY29sX2lkIjoyLCJtYXhfaWR4X2lkIjowLCJtYXhfZmtfaWQiOjAsIm1heF9jc3RfaWQiOjAsInVwZGF0ZV90aW1lc3RhbXAiOjQ1NDQxODg2MTQwMzQwNjM0NiwiU2hhcmRSb3dJREJpdHMiOjAsIm1heF9zaGFyZF9yb3dfaWRfYml0cyI6MCwiYXV0b19yYW5kb21fYml0cyI6MCwiYXV0b19yYW5kb21fcmFuZ2VfYml0cyI6MCwicHJlX3NwbGl0X3JlZ2lvbnMiOjAsInBhcnRpdGlvbiI6bnVsbCwiY29tcHJlc3Npb24iOiIiLCJ2aWV3Ijp7InZpZXdfYWxnb3JpdGhtIjowLCJ2aWV3X2RlZmluZXIiOnsiVXNlcm5hbWUiOiJyb290IiwiSG9zdG5hbWUiOiIxMjcuMC4wLjEiLCJDdXJyZW50VXNlciI6ZmFsc2UsIkF1dGhVc2VybmFtZSI6InJvb3QiLCJBdXRoSG9zdG5hbWUiOiIlIiwiQXV0aFBsdWdpbiI6Im15c3FsX25hdGl2ZV9wYXNzd29yZCJ9LCJ2aWV3X3NlY3VyaXR5IjowLCJ2aWV3X3NlbGVjdCI6IlNFTEVDVCBgaWRgIEFTIGBpZGAsYGtgIEFTIGBrYCBGUk9NIGB0ZXN0YC5gdDFgIiwidmlld19jaGVja29wdGlvbiI6MSwidmlld19jb2xzIjpudWxsfSwic2VxdWVuY2UiOm51bGwsIkxvY2siOm51bGwsInZlcnNpb24iOjUsInRpZmxhc2hfcmVwbGljYSI6bnVsbCwiaXNfY29sdW1uYXIiOmZhbHNlLCJ0ZW1wX3RhYmxlX3R5cGUiOjAsImNhY2hlX3RhYmxlX3N0YXR1cyI6MCwicG9saWN5X3JlZl9pbmZvIjpudWxsLCJzdGF0c19vcHRpb25zIjpudWxsLCJleGNoYW5nZV9wYXJ0aXRpb25faW5mbyI6bnVsbCwidHRsX2luZm8iOm51bGwsInJldmlzaW9uIjowfSwiRmluaXNoZWRUUyI6MCwiTXVsdGlwbGVUYWJsZUluZm9zIjpudWxsfSwidmVyc2lvbiI6MiwicmVvcmdfbWV0YSI6bnVsbCwibXVsdGlfc2NoZW1hX2luZm8iOm51bGwsInByaW9yaXR5IjowLCJzZXFfbnVtIjowLCJjaGFyc2V0IjoiIiwiY29sbGF0ZSI6IiIsImFkbWluX29wZXJhdG9yIjowLCJ0cmFjZV9pbmZvIjp7ImNvbm5lY3Rpb25faWQiOjgxNzg4OTI4Niwic2Vzc2lvbl9hbGlhcyI6IiJ9LCJiZHJfcm9sZSI6IiIsImNkY193cml0ZV9zb3VyY2UiOjAsImxvY2FsX21vZGUiOmZhbHNlLCJzcWxfbW9kZSI6MTQzNjU0OTE1Mn0IDAgqCA4IAA=="
	hv := "7B226964223A3131332C2274797065223A32312C22736368656D615F6964223A322C227461626C655F6964223A3131322C22736368656D615F6E616D65223A2274657374222C227461626C655F6E616D65223A22765F7432222C227374617465223A362C227761726E696E67223A6E756C6C2C22657272223A6E756C6C2C226572725F636F756E74223A302C22726F775F636F756E74223A302C227261775F61726773223A6E756C6C2C22736368656D615F7374617465223A352C22736E617073686F745F766572223A302C227265616C5F73746172745F7473223A3435343431383836313430333430363334362C2273746172745F7473223A3435343431383836313430333430363334312C22646570656E64656E63795F6964223A302C227175657279223A22435245415445205649455720765F74322041532053454C45435420606964602C20606B602046524F4D2060743160222C2262696E6C6F67223A7B22536368656D6156657273696F6E223A35362C224442496E666F223A6E756C6C2C225461626C65496E666F223A7B226964223A3131322C226E616D65223A7B224F223A22765F7432222C224C223A22765F7432227D2C2263686172736574223A22757466386D6234222C22636F6C6C617465223A22757466386D62345F303930305F61695F6369222C22636F6C73223A5B7B226964223A312C226E616D65223A7B224F223A226964222C224C223A226964227D2C226F6666736574223A302C226F726967696E5F64656661756C74223A6E756C6C2C226F726967696E5F64656661756C745F626974223A6E756C6C2C2264656661756C74223A6E756C6C2C2264656661756C745F626974223A6E756C6C2C2264656661756C745F69735F65787072223A66616C73652C2267656E6572617465645F657870725F737472696E67223A22222C2267656E6572617465645F73746F726564223A66616C73652C22646570656E64656E636573223A6E756C6C2C2274797065223A7B225470223A302C22466C6167223A302C22466C656E223A302C22446563696D616C223A302C2243686172736574223A22222C22436F6C6C617465223A22222C22456C656D73223A6E756C6C2C22456C656D73497342696E6172794C6974223A6E756C6C2C224172726179223A66616C73657D2C227374617465223A352C22636F6D6D656E74223A22222C2268696464656E223A66616C73652C226368616E67655F73746174655F696E666F223A6E756C6C2C2276657273696F6E223A307D2C7B226964223A322C226E616D65223A7B224F223A226B222C224C223A226B227D2C226F6666736574223A312C226F726967696E5F64656661756C74223A6E756C6C2C226F726967696E5F64656661756C745F626974223A6E756C6C2C2264656661756C74223A6E756C6C2C2264656661756C745F626974223A6E756C6C2C2264656661756C745F69735F65787072223A66616C73652C2267656E6572617465645F657870725F737472696E67223A22222C2267656E6572617465645F73746F726564223A66616C73652C22646570656E64656E636573223A6E756C6C2C2274797065223A7B225470223A302C22466C6167223A302C22466C656E223A302C22446563696D616C223A302C2243686172736574223A22222C22436F6C6C617465223A22222C22456C656D73223A6E756C6C2C22456C656D73497342696E6172794C6974223A6E756C6C2C224172726179223A66616C73657D2C227374617465223A352C22636F6D6D656E74223A22222C2268696464656E223A66616C73652C226368616E67655F73746174655F696E666F223A6E756C6C2C2276657273696F6E223A307D5D2C22696E6465785F696E666F223A6E756C6C2C22636F6E73747261696E745F696E666F223A6E756C6C2C22666B5F696E666F223A6E756C6C2C227374617465223A352C22706B5F69735F68616E646C65223A66616C73652C2269735F636F6D6D6F6E5F68616E646C65223A66616C73652C22636F6D6D6F6E5F68616E646C655F76657273696F6E223A302C22636F6D6D656E74223A22222C226175746F5F696E635F6964223A302C226175746F5F69645F6361636865223A302C226175746F5F72616E645F6964223A302C226D61785F636F6C5F6964223A322C226D61785F6964785F6964223A302C226D61785F666B5F6964223A302C226D61785F6373745F6964223A302C227570646174655F74696D657374616D70223A3435343431383836313430333430363334362C225368617264526F77494442697473223A302C226D61785F73686172645F726F775F69645F62697473223A302C226175746F5F72616E646F6D5F62697473223A302C226175746F5F72616E646F6D5F72616E67655F62697473223A302C227072655F73706C69745F726567696F6E73223A302C22706172746974696F6E223A6E756C6C2C22636F6D7072657373696F6E223A22222C2276696577223A7B22766965775F616C676F726974686D223A302C22766965775F646566696E6572223A7B22557365726E616D65223A22726F6F74222C22486F73746E616D65223A223132372E302E302E31222C2243757272656E7455736572223A66616C73652C2241757468557365726E616D65223A22726F6F74222C2241757468486F73746E616D65223A2225222C2241757468506C7567696E223A226D7973716C5F6E61746976655F70617373776F7264227D2C22766965775F7365637572697479223A302C22766965775F73656C656374223A2253454C454354206069646020415320606964602C606B6020415320606B602046524F4D206074657374602E60743160222C22766965775F636865636B6F7074696F6E223A312C22766965775F636F6C73223A6E756C6C7D2C2273657175656E6365223A6E756C6C2C224C6F636B223A6E756C6C2C2276657273696F6E223A352C227469666C6173685F7265706C696361223A6E756C6C2C2269735F636F6C756D6E6172223A66616C73652C2274656D705F7461626C655F74797065223A302C2263616368655F7461626C655F737461747573223A302C22706F6C6963795F7265665F696E666F223A6E756C6C2C2273746174735F6F7074696F6E73223A6E756C6C2C2265786368616E67655F706172746974696F6E5F696E666F223A6E756C6C2C2274746C5F696E666F223A6E756C6C2C227265766973696F6E223A307D2C2246696E69736865645453223A3435343431383836313431343431363338382C224D756C7469706C655461626C65496E666F73223A6E756C6C7D2C2276657273696F6E223A322C2272656F72675F6D657461223A6E756C6C2C226D756C74695F736368656D615F696E666F223A6E756C6C2C227072696F72697479223A302C227365715F6E756D223A322C2263686172736574223A22222C22636F6C6C617465223A22222C2261646D696E5F6F70657261746F72223A302C2274726163655F696E666F223A7B22636F6E6E656374696F6E5F6964223A3831373838393238362C2273657373696F6E5F616C696173223A22227D2C226264725F726F6C65223A22222C226364635F77726974655F736F75726365223A302C226C6F63616C5F6D6F6465223A66616C73652C2273716C5F6D6F6465223A313433363534393135327D"
	v, _ := hex.DecodeString(hv)
	// output v to file
	ioutil.WriteFile("job.hex", v, 0644)
	// hex.Decode()
	json.Unmarshal(v, job)
	fmt.Println(job)
	t.Fatal()
}
