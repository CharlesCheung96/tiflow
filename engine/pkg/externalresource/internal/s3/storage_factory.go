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

package s3

import (
	"context"
	"fmt"
	"net/url"

	"github.com/pingcap/errors"
	brStorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
)

// ExternalStorageFactory represents a factory used to create
// brStorage.ExternalStorage.
// Implementing mock or stub ExternalStorageFactory will make
// unit testing easier.
type ExternalStorageFactory interface {
	newS3ExternalStorageForScope(
		ctx context.Context,
		bucket BucketName,
		scope internal.ResourceScope,
	) (brStorage.ExternalStorage, error)

	newS3ExternalStorageFromURI(
		ctx context.Context,
		uri string,
	) (brStorage.ExternalStorage, error)

	scopeURI(bucket BucketName, scope internal.ResourceScope) string
}

// ExternalStorageFactoryImpl implements ExternalStorageFactory.
// It is exported for testing purposes.
type ExternalStorageFactoryImpl struct {
	// Prefix is an optional prefix in the S3 file path.
	// It can be useful when a shared bucket is used for testing purposes.
	Prefix string

	// Options provide necessary information such as endpoints and access key
	// for creating an s3 client.
	Options *brStorage.S3BackendOptions
}

// NewExternalStorageFactory creates a new ExternalStorageFactory with
// s3 options.
func NewExternalStorageFactory(
	options *brStorage.S3BackendOptions,
) *ExternalStorageFactoryImpl {
	return &ExternalStorageFactoryImpl{
		Options: options,
	}
}

// NewExternalStorageFactoryWithPrefix is exported for integration tests.
func NewExternalStorageFactoryWithPrefix(
	prefix string,
	options *brStorage.S3BackendOptions,
) *ExternalStorageFactoryImpl {
	return &ExternalStorageFactoryImpl{Prefix: prefix, Options: options}
}

func (f *ExternalStorageFactoryImpl) newS3ExternalStorageForScope(
	ctx context.Context,
	bucket BucketName,
	scope internal.ResourceScope,
) (brStorage.ExternalStorage, error) {
	uri := f.scopeURI(bucket, scope)
	return GetExternalStorageFromURI(ctx, uri, *f.Options)
}

func (f *ExternalStorageFactoryImpl) scopeURI(
	bucket BucketName, scope internal.ResourceScope,
) string {
	// full uri path is `s3://bucket/prefix/executorID/workerID`
	uri := fmt.Sprintf("s3://%s", url.QueryEscape(bucket))
	if f.Prefix != "" {
		uri += "/" + f.Prefix
	}
	if scope.Executor == "" {
		return uri
	}
	uri += fmt.Sprintf("/%s", url.QueryEscape(string(scope.Executor)))
	if scope.WorkerID != "" {
		uri += fmt.Sprintf("/%s", url.QueryEscape(scope.WorkerID))
	}
	return uri
}

func (f *ExternalStorageFactoryImpl) newS3ExternalStorageFromURI(
	ctx context.Context,
	uri string,
) (brStorage.ExternalStorage, error) {
	return GetExternalStorageFromURI(ctx, uri, *f.Options)
}

// GetExternalStorageFromURI creates a new brStorage.ExternalStorage from a uri.
func GetExternalStorageFromURI(
	ctx context.Context, uri string, s3Opts brStorage.S3BackendOptions,
) (brStorage.ExternalStorage, error) {
	opts := &brStorage.BackendOptions{
		S3: s3Opts,
	}
	backEnd, err := brStorage.ParseBackend(uri, opts)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Note that we may have network I/O here.
	// TODO: use proper retry policy.
	ret, err := brStorage.New(ctx, backEnd, &brStorage.ExternalStorageOptions{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ret, nil
}
