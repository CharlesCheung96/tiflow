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

package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/pkg/client"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal/local"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal/s3"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/engine/pkg/rpcerror"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	derrors "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/ratelimit"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultTimeout                 = 3 * time.Second
	defaultClosedWOrkerChannelSize = 10000
)

type closedWorker struct {
	workerID resModel.WorkerID
	jobID    resModel.JobID
}

// DefaultBroker must implement Broker.
var _ Broker = (*DefaultBroker)(nil)

// DefaultBroker implements the Broker interface
type DefaultBroker struct {
	executorID resModel.ExecutorID
	client     client.ResourceManagerClient

	fileManagers map[resModel.ResourceType]internal.FileManager
	// TODO: add monitor for closedWorkerCh
	closedWorkerCh chan closedWorker

	dummyHandler Handle
	cancel       context.CancelFunc
}

// NewBroker creates a new Impl instance
func NewBroker(
	config *resModel.Config,
	executorID resModel.ExecutorID,
	client client.ResourceManagerClient,
) (*DefaultBroker, error) {
	log.Info("Create new resource broker",
		zap.String("executor-id", string(executorID)),
		zap.Any("config", config))

	broker := &DefaultBroker{
		executorID:     executorID,
		client:         client,
		fileManagers:   make(map[resModel.ResourceType]internal.FileManager),
		closedWorkerCh: make(chan closedWorker, defaultClosedWOrkerChannelSize),
	}

	ctx, cancel := context.WithCancel(context.Background())
	go broker.runGCClosedWorker(ctx)
	broker.cancel = cancel

	// Initialize local file managers
	if config == nil || !config.LocalEnabled() {
		log.Panic("local file manager must be supported by resource broker")
	}
	broker.fileManagers[resModel.ResourceTypeLocalFile] = local.NewLocalFileManager(executorID, config.Local)

	// Initialize s3 file managers
	if !config.S3Enabled() {
		log.Info("S3 config is not complete, will not use s3 as external storage")
		return broker, nil
	}

	broker.fileManagers[resModel.ResourceTypeS3] = s3.NewFileManagerWithConfig(executorID, config.S3)
	if err := broker.createDummyS3Resource(); err != nil {
		return nil, err
	}

	return broker, nil
}

// OpenStorage implements Broker.OpenStorage
func (b *DefaultBroker) OpenStorage(
	ctx context.Context,
	projectInfo tenant.ProjectInfo,
	workerID resModel.WorkerID,
	jobID resModel.JobID,
	resID resModel.ResourceID,
) (Handle, error) {
	// Note the semantics of PasreResourceID:
	// If resourceID is `/local/my-resource`, then tp == resModel.ResourceTypeLocalFile
	// and resName == "my-resource".
	tp, resName, err := resModel.PasreResourceID(resID)
	if err != nil {
		return nil, err
	}

	fm, ok := b.fileManagers[tp]
	if !ok {
		log.Panic("unexpected resource type", zap.String("type", string(tp)))
	}

	record, exists, err := b.checkForExistingResource(ctx,
		resModel.ResourceKey{JobID: jobID, ID: resID})
	if err != nil {
		return nil, err
	}

	var desc internal.ResourceDescriptor
	if !exists {
		desc, err = b.createResource(ctx, fm, projectInfo, workerID, resName)
	} else {
		desc, err = b.getPersistResource(ctx, fm, record, resName)
	}
	if err != nil {
		return nil, err
	}

	log.Info(fmt.Sprintf("Using %s storage with path", string(tp)),
		zap.String("path", desc.URI()))
	return newResourceHandle(jobID, b.executorID, fm, desc, exists, b.client)
}

func (b *DefaultBroker) createResource(
	ctx context.Context, fm internal.FileManager,
	projectInfo tenant.ProjectInfo, workerID resModel.WorkerID,
	resName resModel.ResourceName,
) (internal.ResourceDescriptor, error) {
	ident := internal.ResourceIdent{
		Name: resName,
		ResourceScope: internal.ResourceScope{
			ProjectInfo: projectInfo,
			Executor:    b.executorID, /* executor id where resource is created */
			WorkerID:    workerID,     /* creator id*/
		},
	}
	desc, err := fm.CreateResource(ctx, ident)
	if err != nil {
		//nolint:errcheck
		_ = fm.RemoveResource(ctx, ident)
		return nil, err
	}
	return desc, nil
}

// OnWorkerClosed implements Broker.OnWorkerClosed
func (b *DefaultBroker) OnWorkerClosed(ctx context.Context, workerID resModel.WorkerID, jobID resModel.JobID) {
	timer := time.NewTimer(defaultTimeout)
	select {
	case b.closedWorkerCh <- closedWorker{workerID: workerID, jobID: jobID}:
		return
	case <-timer.C:
		log.Error("closed worker channel is full, broker may be stuck")
	}
}

func (b *DefaultBroker) runGCClosedWorker(ctx context.Context) {
	// We run a gc loop at the max frequency of once per second.
	rl := ratelimit.New(1 /* once per second */)
	for {
		rl.Take()
		select {
		case <-ctx.Done():
			return
		case w := <-b.closedWorkerCh:
			scope := internal.ResourceScope{
				Executor: b.executorID,
				WorkerID: w.workerID,
			}
			for _, fm := range b.fileManagers {
				err := fm.RemoveTemporaryFiles(ctx, scope)
				if err != nil {
					// TODO when we have a cloud-based error collection service, we need
					// to report this.
					// However, since an error here is unlikely to indicate a correctness
					// problem, we do not take further actions.
					log.Warn("Failed to remove temporary files for worker",
						zap.String("worker-id", w.workerID),
						zap.String("job-id", w.jobID),
						zap.Error(err))
					// handle this worker later
					b.OnWorkerClosed(ctx, w.workerID, w.jobID)
				}
			}
		}
	}

}

// RemoveResource implements pb.BrokerServiceServer.
func (b *DefaultBroker) RemoveResource(
	ctx context.Context,
	request *pb.RemoveLocalResourceRequest,
) (*pb.RemoveLocalResourceResponse, error) {
	tp, resName, err := resModel.PasreResourceID(request.GetResourceId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if tp != resModel.ResourceTypeLocalFile {
		return nil, status.Error(codes.InvalidArgument,
			fmt.Sprintf("unexpected resource type %s", tp))
	}

	fm := b.fileManagers[tp]
	if request.GetCreatorId() == "" {
		return nil, status.Error(codes.InvalidArgument, "empty creatorID")
	}

	ident := internal.ResourceIdent{
		Name: resName,
		ResourceScope: internal.ResourceScope{
			Executor: b.executorID,
			WorkerID: request.GetCreatorId(),
		},
	}
	err = fm.RemoveResource(ctx, ident)
	if err != nil {
		if derrors.ErrResourceDoesNotExist.Equal(err) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Unknown, err.Error())
	}

	return &pb.RemoveLocalResourceResponse{}, nil
}

func (b *DefaultBroker) checkForExistingResource(
	ctx context.Context,
	resourceKey resModel.ResourceKey,
) (*resModel.ResourceMeta, bool, error) {
	request := &pb.QueryResourceRequest{
		ResourceKey: &pb.ResourceKey{
			JobId:      resourceKey.JobID,
			ResourceId: resourceKey.ID,
		},
	}
	resp, err := b.client.QueryResource(ctx, request)
	if err == nil {
		return &resModel.ResourceMeta{
			ID:       resourceKey.ID,
			Job:      resp.GetJobId(),
			Worker:   resp.GetCreatorWorkerId(),
			Executor: resModel.ExecutorID(resp.GetCreatorExecutor()),
			Deleted:  false,
		}, true, nil
	}

	code, ok := rpcerror.GRPCStatusCode(err)
	if !ok {
		// If the error is not derived from a grpc status, we should throw it.
		return nil, false, errors.Trace(err)
	}

	switch code {
	case codes.NotFound:
		// Indicates that there is no existing resource with the same name.
		return nil, false, nil
	default:
		return nil, false, errors.Trace(err)
	}
}

func (b *DefaultBroker) getPersistResource(
	ctx context.Context, fm internal.FileManager,
	record *resModel.ResourceMeta,
	resName resModel.ResourceName,
) (internal.ResourceDescriptor, error) {
	ident := internal.ResourceIdent{
		Name: resName,
		ResourceScope: internal.ResourceScope{
			ProjectInfo: tenant.NewProjectInfo("", record.ProjectID),
			Executor:    record.Executor, /* executor id where the resource is persisted */
			WorkerID:    record.Worker,   /* creator id*/
		},
	}
	return fm.GetPersistedResource(ctx, ident)
}

func (b *DefaultBroker) createDummyS3Resource() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// TODO: add retry
	s3FileManager, ok := b.fileManagers[resModel.ResourceTypeS3]
	if !ok {
		return errors.New("S3 file manager not found")
	}

	desc, err := s3FileManager.CreateResource(ctx, s3.GetDummyIdent(b.executorID))
	if err != nil {
		return err
	}

	handler, err := newResourceHandle(fmt.Sprintf(s3.DummyJobID, b.executorID), b.executorID,
		s3FileManager, desc, false, b.client)
	if err != nil {
		return err
	}

	handler.Persist(ctx)
	if err != nil {
		return err
	}

	b.dummyHandler = handler
	return nil
}

func (b *DefaultBroker) Close() {
	b.cancel()

	if fm, ok := b.fileManagers[resModel.ResourceTypeS3]; ok {
		defer fm.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Hour)
		defer cancel()

		err := fm.RemoveTemporaryFiles(ctx, internal.ResourceScope{
			Executor: b.executorID,
			WorkerID: "", /* empty workID means remove all temp files in executor */
		})
		if err != nil {
			// Ignore this error since gcCoordinator will clean up this temp files.
			log.Warn("failed to remove temporary files in executor",
				zap.String("executorID", string(b.executorID)), zap.Error(err))
			return
		}

		// Remove s3 dummy file meta
		if b.dummyHandler != nil {
			_ = b.dummyHandler.Discard(ctx)
		}
	}
}

// PreCheckConfig checks the configuration of external storage.
func PreCheckConfig(config resModel.Config) error {
	if config.LocalEnabled() {
		if err := local.PreCheckConfig(config.Local); err != nil {
			return err
		}
	}
	if config.S3Enabled() {
		if err := s3.PreCheckConfig(config.S3); err != nil {
			return err
		}
	}
	return nil
}
