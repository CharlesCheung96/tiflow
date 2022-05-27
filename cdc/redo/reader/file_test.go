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

package reader

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestReaderNewReader(t *testing.T) {
	_, err := newReaders(context.Background(), nil)
	require.NotNil(t, err)

	dir := t.TempDir()
	require.Panics(t, func() {
		_, err = newReaders(context.Background(), &readerConfig{dir: dir})
	})
}

func TestFileReaderRead(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uri, err := url.Parse(fmt.Sprintf("file://%s", dir))
	require.NoError(t, err)
	cfg := &readerConfig{
		dir:                t.TempDir(),
		startTs:            10,
		endTs:              12,
		fileType:           redo.RedoRowLogFileType,
		uri:                *uri,
		useExternalStorage: true,
	}
	// log file with maxCommitTs<=startTs, fileter when download file
	genLogFile(ctx, t, dir, redo.RedoRowLogFileType, 1, cfg.startTs)
	// normal log file, include [10, 11, 12] and [11, 12, ... 20]
	genLogFile(ctx, t, dir, redo.RedoRowLogFileType, cfg.startTs, cfg.endTs+2)
	genLogFile(ctx, t, dir, redo.RedoRowLogFileType, cfg.endTs-1, 20)
	// log file with minCommitTs>endTs, filtered when sort file
	genLogFile(ctx, t, dir, redo.RedoRowLogFileType, 2000, 2023)

	log.Info("start to read redo log files")
	readers, err := newReaders(ctx, cfg)
	require.NoError(t, err)
	require.Equal(t, 2, len(readers))
	defer readers[0].Close() //nolint:errcheck

	for _, r := range readers {
		log, err := r.Read()
		require.NoError(t, err)
		require.EqualValues(t, 11, log.RedoRow.Row.CommitTs)
		log, err = r.Read()
		require.NoError(t, err)
		require.EqualValues(t, 12, log.RedoRow.Row.CommitTs)
		log, err = r.Read()
		require.Nil(t, log)
		require.ErrorIs(t, err, io.EOF)
		require.NoError(t, r.Close())
	}
}

func TestDecodeLog(t *testing.T) {
	dir := "./log"
	files, _ := ioutil.ReadDir(dir)
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), redo.LogEXT) &&
			!strings.HasSuffix(file.Name(), redo.TmpEXT) {
			continue
		}

		f, err := os.Open(filepath.Join(dir, file.Name()))
		fmt.Printf("handling %v...", file.Name())
		if err != nil {
			log.Panic("", zap.Error(err))
		}

		result := make([]byte, 0)
		r := &reader{
			br:       bufio.NewReader(f),
			fileName: file.Name(),
			closer:   f,
		}
		defer r.Close()

		for {
			rl, err := r.Read()
			if err != nil {
				if err != io.EOF {
					log.Panic("", zap.Error(err))
				}
				break
			}
			rb, err := json.Marshal(rl)
			if err != nil {
				log.Panic("", zap.Error(err))
			}
			result = append(result, rb...)
			result = append(result, byte(','), byte('\n'))
		}

		outPath := filepath.Join(dir, file.Name()+".json")
		err = ioutil.WriteFile(outPath, result, 0644)
		if err != nil {
			log.Panic("", zap.Error(err))
		}
	}
}

func TestDecodeMeta(t *testing.T) {
	dir := "./log"
	files, _ := ioutil.ReadDir(dir)
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), redo.MetaEXT) {
			continue
		}

		f, err := os.Open(filepath.Join(dir, file.Name()))
		fmt.Printf("handling %v...", file.Name())
		if err != nil {
			log.Panic("", zap.Error(err))
		}

		meta := &common.LogMeta{}
		data, _ := os.ReadFile(f.Name())
		_, err = meta.UnmarshalMsg(data)
		if err != nil {
			log.Panic("")
		}
		result, _ := json.Marshal(meta)

		outPath := filepath.Join(dir, file.Name()+".json")
		err = ioutil.WriteFile(outPath, result, 0644)
		if err != nil {
			log.Panic("", zap.Error(err))
		}
	}
}
