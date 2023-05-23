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

package avro

import (
	"context"
	"math/rand"
	"testing"
	"time"

	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestDecodeEvent(t *testing.T) {
	o := &Options{
		EnableTiDBExtension:        true,
		DecimalHandlingMode:        "precise",
		BigintUnsignedHandlingMode: "long",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	encoder, err := setupEncoderAndSchemaRegistry(ctx, o)
	defer teardownEncoderAndSchemaRegistry()
	require.NoError(t, err)
	require.NotNil(t, encoder)

	cols := make([]*model.Column, 0)
	colInfos := make([]rowcodec.ColInfo, 0)

	cols = append(
		cols,
		&model.Column{
			Name:  "id",
			Value: int64(1),
			Type:  mysql.TypeLong,
			Flag:  model.HandleKeyFlag,
		},
	)
	colInfos = append(
		colInfos,
		rowcodec.ColInfo{
			ID:            1000,
			IsPKHandle:    true,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeLong),
		},
	)

	for _, v := range avroTestColumns {
		cols = append(cols, &v.col)
		colInfos = append(colInfos, v.colInfo)

		colNew := v.col
		colNew.Name = colNew.Name + "nullable"
		colNew.Value = nil
		colNew.Flag.SetIsNullable()

		colInfoNew := v.colInfo
		colInfoNew.ID += int64(len(avroTestColumns))

		cols = append(cols, &colNew)
		colInfos = append(colInfos, colInfoNew)
	}

	input := &avroEncodeInput{
		cols,
		colInfos,
	}

	rand.New(rand.NewSource(time.Now().Unix())).Shuffle(len(input.columns), func(i, j int) {
		input.columns[i], input.columns[j] = input.columns[j], input.columns[i]
		input.colInfos[i], input.colInfos[j] = input.colInfos[j], input.colInfos[i]
	})

	// insert event
	event := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "test",
			Table:  "avro",
		},
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema: "test",
				Table:  "avro",
			},
		},
		Columns:  input.columns,
		ColInfos: input.colInfos,
	}

	topic := "avro-test-topic"
	err = encoder.AppendRowChangedEvent(ctx, topic, event, func() {})
	require.NoError(t, err)

	messages := encoder.Build()
	require.Len(t, messages, 1)
	message := messages[0]

	keySchemaM, valueSchemaM, err := NewKeyAndValueSchemaManagers(
		ctx, "http://127.0.0.1:8081", nil)
	require.NoError(t, err)

	tz, err := util.GetLocalTimezone()
	require.NoError(t, err)
	decoder := NewDecoder(o, keySchemaM, valueSchemaM, topic, tz)
	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	messageType, exist, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, exist)
	require.Equal(t, model.MessageTypeRow, messageType)

	decodedEvent, err := decoder.NextRowChangedEvent()
	require.NoError(t, err)
	require.NotNil(t, decodedEvent)
}

func TestDecodeDDLEvent(t *testing.T) {
	t.Parallel()

	o := &Options{
		EnableTiDBExtension:  true,
		EnableWatermarkEvent: true,
	}

	encoder := &BatchEncoder{
		namespace: model.DefaultNamespace,
		result:    make([]*common.Message, 0, 1),
		Options:   o,
	}

	message, err := encoder.EncodeDDLEvent(&model.DDLEvent{
		StartTs:  1020,
		CommitTs: 1030,
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema:      "test",
				Table:       "t1",
				TableID:     0,
				IsPartition: false,
			},
		},
		Type:  timodel.ActionAddColumn,
		Query: "ALTER TABLE test.t1 ADD COLUMN a int",
	})
	require.NoError(t, err)
	require.NotNil(t, message)

	topic := "test-topic"
	tz, err := util.GetLocalTimezone()
	require.NoError(t, err)
	decoder := NewDecoder(o, nil, nil, topic, tz)
	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	messageType, exist, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, exist)
	require.Equal(t, model.MessageTypeDDL, messageType)

	decodedEvent, err := decoder.NextDDLEvent()
	require.NoError(t, err)
	require.NotNil(t, decodedEvent)
	require.Equal(t, uint64(1020), decodedEvent.StartTs)
	require.Equal(t, uint64(1030), decodedEvent.CommitTs)
	require.Equal(t, timodel.ActionAddColumn, decodedEvent.Type)
	require.Equal(t, "ALTER TABLE test.t1 ADD COLUMN a int", decodedEvent.Query)
	require.Equal(t, "test", decodedEvent.TableInfo.TableName.Schema)
	require.Equal(t, "t1", decodedEvent.TableInfo.TableName.Table)
	require.Equal(t, int64(0), decodedEvent.TableInfo.TableName.TableID)
	require.False(t, decodedEvent.TableInfo.TableName.IsPartition)
}

func TestDecodeResolvedEvent(t *testing.T) {
	t.Parallel()

	o := &Options{
		EnableTiDBExtension:  true,
		EnableWatermarkEvent: true,
	}

	encoder := &BatchEncoder{
		namespace: model.DefaultNamespace,
		Options:   o,
		result:    make([]*common.Message, 0, 1),
	}

	resolvedTs := uint64(1591943372224)
	message, err := encoder.EncodeCheckpointEvent(resolvedTs)
	require.NoError(t, err)
	require.NotNil(t, message)

	topic := "test-topic"
	tz, err := util.GetLocalTimezone()
	require.NoError(t, err)
	decoder := NewDecoder(o, nil, nil, topic, tz)
	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	messageType, exist, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, exist)
	require.Equal(t, model.MessageTypeResolved, messageType)

	obtained, err := decoder.NextResolvedEvent()
	require.NoError(t, err)
	require.Equal(t, resolvedTs, obtained)
}
