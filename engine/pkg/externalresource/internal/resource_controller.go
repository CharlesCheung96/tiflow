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

package internal

import (
	"context"

	"github.com/pingcap/tiflow/engine/model"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
)

// ResourceController is an interface providing relevant operations related
// to one resource type.
type ResourceController interface {
	// GCSingleResource remove a persisted resource.
	GCSingleResource(context.Context, *resModel.ResourceMeta) error
	// GCExecutors removes all temporary resources created by the offlined exectors.
	GCExecutor(context.Context, []*resModel.ResourceMeta, model.ExecutorID) error
}
