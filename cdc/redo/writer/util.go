// Copyright 2023 PingCAP, Inc.
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

package writer

import (
	"context"
	"fmt"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
)

var getAllFilesInS3 = func(
	ctx context.Context, extStorage storage.ExternalStorage,
) ([]string, error) {
	files := []string{}
	err := extStorage.WalkDir(ctx, &storage.WalkOption{}, func(path string, _ int64) error {
		files = append(files, path)
		return nil
	})
	if err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err)
	}

	return files, nil
}

func getChangefeedMatcher(changeFeedID model.ChangeFeedID) string {
	if changeFeedID.Namespace == "default" {
		return fmt.Sprintf("_%s_", changeFeedID.ID)
	}
	return fmt.Sprintf("_%s_%s_", changeFeedID.Namespace, changeFeedID.ID)
}

func getDeletedChangefeedMarker(changeFeedID model.ChangeFeedID) string {
	if changeFeedID.Namespace == model.DefaultNamespace {
		return fmt.Sprintf("delete_%s", changeFeedID.ID)
	}
	return fmt.Sprintf("delete_%s_%s", changeFeedID.Namespace, changeFeedID.ID)
}
