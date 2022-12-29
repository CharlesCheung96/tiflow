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

package common

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util"
)

const (
	// RedoLogFileFormatV1 was used before v6.1.0, which doesn't contain namespace information
	// layout: captureID_changefeedID_fileType_maxEventCommitTs_uuid.fileExtName
	RedoLogFileFormatV1 = "%s_%s_%s_%d_%s%s"
	// RedoLogFileFormatV2 is available since v6.1.0, which contains namespace information
	// layout: captureID_namespace_changefeedID_fileType_maxEventCommitTs_uuid.fileExtName
	RedoLogFileFormatV2 = "%s_%s_%s_%s_%d_%s%s"
)

// InitExternalStorage init an external storage.
var InitExternalStorage = func(ctx context.Context, uri url.URL) (storage.ExternalStorage, error) {
	if ConsistentStorage(uri.Scheme) == ConsistentStorageS3 && len(uri.Host) == 0 {
		// TODO: this branch is compatible with previous s3 logic and will be removed
		// in the future.
		return nil, errors.WrapChangefeedUnretryableErr(errors.ErrS3StorageInitialize,
			errors.Errorf("please specify the bucket for %+v", uri))
	}
	s, err := util.GetExternalStorage(ctx, uri.String(), nil)
	if err != nil {
		return nil, errors.WrapChangefeedUnretryableErr(errors.ErrS3StorageInitialize, err)
	}
	return s, nil
}

// logFormat2ParseFormat converts redo log file name format to the space separated
// format, which can be read and parsed by sscanf. Besides remove the suffix `%s`
// which is used as file name extension, since we will parse extension first.
func logFormat2ParseFormat(fmtStr string) string {
	return strings.TrimSuffix(strings.ReplaceAll(fmtStr, "_", " "), "%s")
}

// ParseLogFileName extract the commitTs, fileType from log fileName
func ParseLogFileName(name string) (uint64, string, error) {
	ext := filepath.Ext(name)
	if ext == MetaEXT {
		return 0, RedoMetaFileType, nil
	}

	// if .sort, the name should be like
	// fmt.Sprintf("%s_%s_%s_%d_%s_%d%s", w.cfg.captureID,
	// w.cfg.changeFeedID.Namespace,w.cfg.changeFeedID.ID,
	// w.cfg.fileType, w.commitTS.Load(), uuid, LogEXT)+SortLogEXT
	if ext == SortLogEXT {
		name = strings.TrimSuffix(name, SortLogEXT)
		ext = filepath.Ext(name)
	}
	if ext != LogEXT && ext != TmpEXT {
		return 0, "", nil
	}

	var commitTs uint64
	var captureID, namespace, changefeedID, fileType, uid string
	// if the namespace is not default, the log looks like:
	// fmt.Sprintf("%s_%s_%s_%s_%d_%s%s", w.cfg.captureID,
	// w.cfg.changeFeedID.Namespace,w.cfg.changeFeedID.ID,
	// w.cfg.fileType, w.commitTS.Load(), uuid, redo.LogEXT)
	// otherwise it looks like:
	// fmt.Sprintf("%s_%s_%s_%d_%s%s", w.cfg.captureID,
	// w.cfg.changeFeedID.ID,
	// w.cfg.fileType, w.commitTS.Load(), uuid, redo.LogEXT)
	var (
		vars      []any
		formatStr string
	)
	if len(strings.Split(name, "_")) == 6 {
		formatStr = logFormat2ParseFormat(RedoLogFileFormatV2)
		vars = []any{&captureID, &namespace, &changefeedID, &fileType, &commitTs, &uid}
	} else {
		formatStr = logFormat2ParseFormat(RedoLogFileFormatV1)
		vars = []any{&captureID, &changefeedID, &fileType, &commitTs, &uid}
	}
	name = strings.ReplaceAll(name, "_", " ")
	_, err := fmt.Sscanf(name, formatStr, vars...)
	if err != nil {
		return 0, "", errors.Annotatef(err, "bad log name: %s", name)
	}
	return commitTs, fileType, nil
}

// FilterChangefeedFiles return the files that match to the changefeed.
func FilterChangefeedFiles(files []string, changefeedID model.ChangeFeedID) []string {
	var (
		matcher string
		res     []string
	)

	if changefeedID.Namespace == "default" {
		matcher = fmt.Sprintf("_%s_", changefeedID.ID)
	} else {
		matcher = fmt.Sprintf("_%s_%s_", changefeedID.Namespace, changefeedID.ID)
	}
	for _, file := range files {
		if strings.Contains(file, matcher) {
			res = append(res, file)
		}
	}
	return res
}
