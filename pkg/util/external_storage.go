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

package util

import (
	"context"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// InitS3storage init a storage used for s3,
// s3URI should be like s3URI="s3://logbucket/test-changefeed?endpoint=http://$S3_ENDPOINT/"
var InitS3storage = func(ctx context.Context, uri url.URL) (storage.ExternalStorage, error) {
	if len(uri.Host) == 0 {
		return nil, errors.WrapChangefeedUnretryableErr(errors.ErrS3StorageInitialize, errors.Errorf("please specify the bucket for s3 in %v", uri))
	}

	prefix := strings.Trim(uri.Path, "/")
	s3 := &backuppb.S3{Bucket: uri.Host, Prefix: prefix}
	options := &storage.BackendOptions{}
	storage.ExtractQueryParameters(&uri, &options.S3)
	if err := options.S3.Apply(s3); err != nil {
		return nil, errors.WrapChangefeedUnretryableErr(errors.ErrS3StorageInitialize, err)
	}

	// we should set this to true, since br set it by default in parseBackend
	s3.ForcePathStyle = true
	backend := &backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_S3{S3: s3},
	}
	s3storage, err := storage.New(ctx, backend, &storage.ExternalStorageOptions{
		SendCredentials: false,
		HTTPClient:      nil,
		S3Retryer:       DefaultS3Retryer(),
	})
	if err != nil {
		return nil, errors.WrapChangefeedUnretryableErr(errors.ErrS3StorageInitialize, err)
	}

	return s3storage, nil
}

// retryerWithLog wraps the client.DefaultRetryer, and logs when retrying.
type retryerWithLog struct {
	client.DefaultRetryer
}

func isDeadlineExceedError(err error) bool {
	return strings.Contains(err.Error(), "context deadline exceeded")
}

func (rl retryerWithLog) ShouldRetry(r *request.Request) bool {
	if isDeadlineExceedError(r.Error) {
		return false
	}
	return rl.DefaultRetryer.ShouldRetry(r)
}

func (rl retryerWithLog) RetryRules(r *request.Request) time.Duration {
	backoffTime := rl.DefaultRetryer.RetryRules(r)
	if backoffTime > 0 {
		log.Warn("failed to request s3, retrying", zap.Error(r.Error), zap.Duration("backoff", backoffTime))
	}
	return backoffTime
}

// DefaultS3Retryer is the default s3 retryer, maybe this function
// should be extracted to another place.
func DefaultS3Retryer() request.Retryer {
	return retryerWithLog{
		DefaultRetryer: client.DefaultRetryer{
			NumMaxRetries:    3,
			MinRetryDelay:    1 * time.Second,
			MinThrottleDelay: 2 * time.Second,
		},
	}
}
