// Copyright 2021 PingCAP, Inc.
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

package v1

import (
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/api"
	"github.com/pingcap/tiflow/cdc/api/middleware"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// OpenAPI provides capture APIs.
type OpenAPI struct {
	capture capture.Capture
	// use for unit test only
	testStatusProvider owner.StatusProvider
}

// NewOpenAPI creates a new OpenAPI.
func NewOpenAPI(c capture.Capture) OpenAPI {
	return OpenAPI{capture: c}
}

// NewOpenAPI4Test return a OpenAPI for test
func NewOpenAPI4Test(c capture.Capture, p owner.StatusProvider) OpenAPI {
	return OpenAPI{capture: c, testStatusProvider: p}
}

func (h *OpenAPI) statusProvider() owner.StatusProvider {
	if h.testStatusProvider != nil {
		return h.testStatusProvider
	}
	return h.capture.StatusProvider()
}

// RegisterOpenAPIRoutes registers routes for OpenAPI
func RegisterOpenAPIRoutes(router *gin.Engine, api OpenAPI) {
	v1 := router.Group("/api/v1")

	v1.Use(middleware.CheckServerReadyMiddleware(api.capture))
	v1.Use(middleware.LogMiddleware())
	v1.Use(middleware.ErrorHandleMiddleware())

	// common API
	v1.GET("/status", api.ServerStatus)
	v1.GET("/health", api.Health)
	v1.POST("/log", SetLogLevel)

	controllerMiddleware := middleware.ForwardToControllerMiddleware(api.capture)
	changefeedOwnerMiddleware := middleware.
		ForwardToChangefeedOwnerMiddleware(api.capture, getChangefeedFromRequest)
	authenticateMiddleware := middleware.AuthenticateMiddleware(api.capture)

	// changefeed API
	changefeedGroup := v1.Group("/changefeeds")
	changefeedGroup.GET("", controllerMiddleware, api.ListChangefeed)
	changefeedGroup.GET("/:changefeed_id", changefeedOwnerMiddleware, api.GetChangefeed)
	changefeedGroup.POST("", controllerMiddleware, authenticateMiddleware, api.CreateChangefeed)
	changefeedGroup.PUT("/:changefeed_id", changefeedOwnerMiddleware, authenticateMiddleware, api.UpdateChangefeed)
	changefeedGroup.POST("/:changefeed_id/pause", changefeedOwnerMiddleware, authenticateMiddleware, api.PauseChangefeed)
	changefeedGroup.POST("/:changefeed_id/resume", changefeedOwnerMiddleware, authenticateMiddleware, api.ResumeChangefeed)
	changefeedGroup.DELETE("/:changefeed_id", controllerMiddleware, authenticateMiddleware, api.RemoveChangefeed)
	changefeedGroup.POST("/:changefeed_id/tables/rebalance_table", changefeedOwnerMiddleware, authenticateMiddleware, api.RebalanceTables)
	changefeedGroup.POST("/:changefeed_id/tables/move_table", changefeedOwnerMiddleware, authenticateMiddleware, api.MoveTable)

	// owner API
	ownerGroup := v1.Group("/owner")
	ownerGroup.POST("/resign", controllerMiddleware, api.ResignController)

	// processor API
	processorGroup := v1.Group("/processors")
	processorGroup.GET("", controllerMiddleware, api.ListProcessor)
	processorGroup.GET("/:changefeed_id/:capture_id",
		changefeedOwnerMiddleware, api.GetProcessor)

	// capture API
	captureGroup := v1.Group("/captures")
	captureGroup.Use(controllerMiddleware)
	captureGroup.GET("", api.ListCapture)
	captureGroup.PUT("/drain", api.DrainCapture)
}

// ListChangefeed lists all changgefeeds in cdc cluster
// @Summary List changefeed
// @Description list all changefeeds in cdc cluster
// @Tags changefeed
// @Accept json
// @Produce json
// @Param state query string false "state"
// @Success 200 {array} model.ChangefeedCommonInfo
// @Failure 500 {object} model.HTTPError
// @Router /api/v1/changefeeds [get]
func (h *OpenAPI) ListChangefeed(c *gin.Context) {
	ctx := c.Request.Context()
	state := c.Query(api.ApiOpVarChangefeedState)
	controller, err := h.capture.GetController()
	if err != nil {
		_ = c.Error(err)
		return
	}
	// get all changefeed status
	statuses, err := controller.GetAllChangeFeedCheckpointTs(ctx)
	if err != nil {
		_ = c.Error(err)
		return
	}
	// get all changefeed infos
	infos, err := controller.GetAllChangeFeedInfo(ctx)
	if err != nil {
		// this call will return a parsedError generated by the error we passed in
		// so it is no need to check the parsedError
		_ = c.Error(err)
		return
	}

	resps := make([]*model.ChangefeedCommonInfo, 0)
	changefeeds := make([]model.ChangeFeedID, 0)

	for cfID := range infos {
		changefeeds = append(changefeeds, cfID)
	}
	sort.Slice(changefeeds, func(i, j int) bool {
		if changefeeds[i].Namespace == changefeeds[j].Namespace {
			return changefeeds[i].ID < changefeeds[j].ID
		}

		return changefeeds[i].Namespace < changefeeds[j].Namespace
	})

	for _, cfID := range changefeeds {
		cfInfo, exist := infos[cfID]
		if !exist {
			// If a changefeed info does not exist, skip it
			continue
		}

		if !cfInfo.State.IsNeeded(state) {
			continue
		}

		resp := &model.ChangefeedCommonInfo{
			UpstreamID: cfInfo.UpstreamID,
			Namespace:  cfID.Namespace,
			ID:         cfID.ID,
		}

		resp.FeedState = cfInfo.State
		resp.RunningError = cfInfo.Error

		resp.CheckpointTSO = statuses[cfID]
		tm := oracle.GetTimeFromTS(resp.CheckpointTSO)
		resp.CheckpointTime = model.JSONTime(tm)

		resps = append(resps, resp)
	}
	c.IndentedJSON(http.StatusOK, resps)
}

// GetChangefeed get detailed info of a changefeed
// @Summary Get changefeed
// @Description get detail information of a changefeed
// @Tags changefeed
// @Accept json
// @Produce json
// @Param changefeed_id  path  string  true  "changefeed_id"
// @Success 200 {object} model.ChangefeedDetail
// @Failure 500,400 {object} model.HTTPError
// @Router /api/v1/changefeeds/{changefeed_id} [get]
func (h *OpenAPI) GetChangefeed(c *gin.Context) {
	ctx := c.Request.Context()
	changefeedID := model.DefaultChangeFeedID(c.Param(api.ApiOpVarChangefeedID))
	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedID.ID))
		return
	}

	info, err := h.statusProvider().GetChangeFeedInfo(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	status, err := h.statusProvider().GetChangeFeedStatus(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	taskStatus := make([]model.CaptureTaskStatus, 0)
	if info.State == model.StateNormal {
		processorInfos, err := h.statusProvider().GetAllTaskStatuses(ctx, changefeedID)
		if err != nil {
			_ = c.Error(err)
			return
		}
		for captureID, status := range processorInfos {
			tables := make([]int64, 0)
			for tableID := range status.Tables {
				tables = append(tables, tableID)
			}
			taskStatus = append(taskStatus,
				model.CaptureTaskStatus{
					CaptureID: captureID, Tables: tables,
					Operation: status.Operation,
				})
		}
	}
	sinkURI, err := util.MaskSinkURI(info.SinkURI)
	if err != nil {
		log.Error("failed to mask sink URI", zap.Error(err))
	}

	changefeedDetail := &model.ChangefeedDetail{
		UpstreamID:     info.UpstreamID,
		Namespace:      changefeedID.Namespace,
		ID:             changefeedID.ID,
		SinkURI:        sinkURI,
		CreateTime:     model.JSONTime(info.CreateTime),
		StartTs:        info.StartTs,
		TargetTs:       info.TargetTs,
		CheckpointTSO:  status.CheckpointTs,
		CheckpointTime: model.JSONTime(oracle.GetTimeFromTS(status.CheckpointTs)),
		ResolvedTs:     status.ResolvedTs,
		Engine:         info.Engine,
		FeedState:      info.State,
		TaskStatus:     taskStatus,
		CreatorVersion: info.CreatorVersion,
	}

	c.IndentedJSON(http.StatusOK, changefeedDetail)
}

// CreateChangefeed creates a changefeed
// @Summary Create changefeed
// @Description create a new changefeed
// @Tags changefeed
// @Accept json
// @Produce json
// @Param changefeed body model.ChangefeedConfig true "changefeed config"
// @Success 202
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v1/changefeeds [post]
func (h *OpenAPI) CreateChangefeed(c *gin.Context) {
	// c does not have a cancel() func and its Done() method always return nil,
	// so we should not use c as a context.
	// Ref:https://github.com/gin-gonic/gin/blob/92eeaa4ebbadec2376e2ca5f5749888da1a42e24/context.go#L1157
	ctx := c.Request.Context()
	var changefeedConfig model.ChangefeedConfig
	if err := c.BindJSON(&changefeedConfig); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.Wrap(err))
		return
	}

	upManager, err := h.capture.GetUpstreamManager()
	if err != nil {
		_ = c.Error(err)
		return
	}
	up, err := upManager.GetDefaultUpstream()
	if err != nil {
		_ = c.Error(err)
		return
	}

	ctrl, err := h.capture.GetController()
	if err != nil {
		_ = c.Error(err)
		return
	}
	info, err := verifyCreateChangefeedConfig(ctx, changefeedConfig,
		up, ctrl, h.capture.GetEtcdClient())
	if err != nil {
		_ = c.Error(err)
		return
	}
	upstreamInfo := &model.UpstreamInfo{
		ID:            up.ID,
		PDEndpoints:   strings.Join(up.PdEndpoints, ","),
		KeyPath:       up.SecurityConfig.KeyPath,
		CertPath:      up.SecurityConfig.CertPath,
		CAPath:        up.SecurityConfig.CAPath,
		CertAllowedCN: up.SecurityConfig.CertAllowedCN,
	}
	err = ctrl.CreateChangefeed(
		ctx, upstreamInfo,
		info)
	if err != nil {
		_ = c.Error(err)
		return
	}

	log.Info("Create changefeed successfully!",
		zap.String("id", changefeedConfig.ID),
		zap.String("changefeed", info.String()))
	c.Status(http.StatusAccepted)
}

// PauseChangefeed pauses a changefeed
// @Summary Pause a changefeed
// @Description Pause a changefeed
// @Tags changefeed
// @Accept json
// @Produce json
// @Param changefeed_id  path  string  true  "changefeed_id"
// @Success 202
// @Failure 500,400 {object} model.HTTPError
// @Router /api/v1/changefeeds/{changefeed_id}/pause [post]
func (h *OpenAPI) PauseChangefeed(c *gin.Context) {
	ctx := c.Request.Context()

	changefeedID := model.DefaultChangeFeedID(c.Param(api.ApiOpVarChangefeedID))
	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedID.ID))
		return
	}
	// check if the changefeed exists
	_, err := h.statusProvider().GetChangeFeedStatus(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	job := model.AdminJob{
		CfID: changefeedID,
		Type: model.AdminStop,
	}

	if err := api.HandleOwnerJob(ctx, h.capture, job); err != nil {
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusAccepted)
}

// ResumeChangefeed resumes a changefeed
// @Summary Resume a changefeed
// @Description Resume a changefeed
// @Tags changefeed
// @Accept json
// @Produce json
// @Param changefeed_id path string true "changefeed_id"
// @Success 202
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v1/changefeeds/{changefeed_id}/resume [post]
func (h *OpenAPI) ResumeChangefeed(c *gin.Context) {
	ctx := c.Request.Context()
	changefeedID := model.DefaultChangeFeedID(c.Param(api.ApiOpVarChangefeedID))
	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedID.ID))
		return
	}
	// check if the changefeed exists
	_, err := h.capture.StatusProvider().GetChangeFeedStatus(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	job := model.AdminJob{
		CfID: changefeedID,
		Type: model.AdminResume,
	}

	if err := api.HandleOwnerJob(ctx, h.capture, job); err != nil {
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusAccepted)
}

// UpdateChangefeed updates a changefeed
// @Summary Update a changefeed
// @Description Update a changefeed
// @Tags changefeed
// @Accept json
// @Produce json
// @Param changefeed_id  path  string  true  "changefeed_id"
// @Param changefeedConfig body model.ChangefeedConfig true "changefeed config"
// @Success 202
// @Failure 500,400 {object} model.HTTPError
// @Router /api/v1/changefeeds/{changefeed_id} [put]
func (h *OpenAPI) UpdateChangefeed(c *gin.Context) {
	ctx := c.Request.Context()
	changefeedID := model.DefaultChangeFeedID(c.Param(api.ApiOpVarChangefeedID))

	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedID.ID))
		return
	}

	owner, err := h.capture.GetOwner()
	if err != nil {
		_ = c.Error(err)
		return
	}

	info, err := h.statusProvider().GetChangeFeedInfo(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	switch info.State {
	case model.StateFailed, model.StateStopped:
	default:
		_ = c.Error(
			cerror.ErrChangefeedUpdateRefused.GenWithStackByArgs(
				"can only update changefeed config when it is stopped or failed",
			),
		)
		return
	}

	info.ID = changefeedID.ID
	info.Namespace = changefeedID.Namespace

	// can only update target-ts, sink-uri
	// filter_rules, ignore_txn_start_ts, mounter_worker_num, sink_config
	var changefeedConfig model.ChangefeedConfig
	if err = c.BindJSON(&changefeedConfig); err != nil {
		_ = c.Error(err)
		return
	}

	newInfo, err := VerifyUpdateChangefeedConfig(ctx, changefeedConfig, info)
	if err != nil {
		_ = c.Error(err)
		return
	}

	err = owner.UpdateChangefeed(ctx, newInfo)
	if err != nil {
		_ = c.Error(err)
		return
	}

	c.Status(http.StatusAccepted)
}

// RemoveChangefeed removes a changefeed
// @Summary Remove a changefeed
// @Description Remove a changefeed
// @Tags changefeed
// @Accept json
// @Produce json
// @Param changefeed_id path string true "changefeed_id"
// @Success 202
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v1/changefeeds/{changefeed_id} [delete]
func (h *OpenAPI) RemoveChangefeed(c *gin.Context) {
	ctx := c.Request.Context()
	changefeedID := model.DefaultChangeFeedID(c.Param(api.ApiOpVarChangefeedID))
	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedID.ID))
		return
	}
	// check if the changefeed exists
	ctrl, err := h.capture.GetController()
	if err != nil {
		_ = c.Error(err)
		return
	}
	exist, err := ctrl.IsChangefeedExists(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}
	if !exist {
		_ = c.Error(cerror.ErrChangeFeedNotExists.GenWithStackByArgs(changefeedID))
		return
	}

	job := model.AdminJob{
		CfID: changefeedID,
		Type: model.AdminRemove,
	}

	if err := api.HandleOwnerJob(ctx, h.capture, job); err != nil {
		_ = c.Error(err)
		return
	}

	// Owner needs at least tow ticks to remove a changefeed,
	// we need to wait for it.
	err = retry.Do(ctx, func() error {
		exist, err = ctrl.IsChangefeedExists(ctx, changefeedID)
		if err != nil {
			if strings.Contains(err.Error(), "ErrChangeFeedNotExists") {
				return nil
			}
			return err
		}
		if !exist {
			return nil
		}
		return cerror.ErrChangeFeedDeletionUnfinished.GenWithStackByArgs(changefeedID)
	},
		retry.WithMaxTries(100),         // max retry duration is 1 minute
		retry.WithBackoffBaseDelay(600), // default owner tick interval is 200ms
		retry.WithIsRetryableErr(cerror.IsRetryableError))

	if err != nil {
		_ = c.Error(err)
		return
	}

	c.Status(http.StatusAccepted)
}

// RebalanceTables rebalances tables
// @Summary rebalance tables
// @Description rebalance all tables of a changefeed
// @Tags changefeed
// @Accept json
// @Produce json
// @Param changefeed_id path string true "changefeed_id"
// @Success 202
// @Failure 500,400 {object} model.HTTPError
// @Router /api/v1/changefeeds/{changefeed_id}/tables/rebalance_table [post]
func (h *OpenAPI) RebalanceTables(c *gin.Context) {
	ctx := c.Request.Context()
	changefeedID := model.DefaultChangeFeedID(c.Param(api.ApiOpVarChangefeedID))

	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedID.ID))
		return
	}
	// check if the changefeed exists
	_, err := h.statusProvider().GetChangeFeedStatus(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	if err := api.HandleOwnerBalance(ctx, h.capture, changefeedID); err != nil {
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusAccepted)
}

// MoveTable moves a table to target capture
// @Summary move table
// @Description move one table to the target capture
// @Tags changefeed
// @Accept json
// @Produce json
// @Param changefeed_id path string true "changefeed_id"
// @Param MoveTable body model.MoveTableReq true "move table request"
// @Success 202
// @Failure 500,400 {object} model.HTTPError
// @Router /api/v1/changefeeds/{changefeed_id}/tables/move_table [post]
func (h *OpenAPI) MoveTable(c *gin.Context) {
	ctx := c.Request.Context()
	changefeedID := model.DefaultChangeFeedID(c.Param(api.ApiOpVarChangefeedID))
	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedID.ID))
		return
	}
	// check if the changefeed exists
	_, err := h.statusProvider().GetChangeFeedStatus(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	data := model.MoveTableReq{}
	err = c.BindJSON(&data)
	if err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.Wrap(err))
		return
	}

	if err := model.ValidateChangefeedID(data.CaptureID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid capture_id: %s", data.CaptureID))
		return
	}

	err = api.HandleOwnerScheduleTable(
		ctx, h.capture, changefeedID, data.CaptureID, data.TableID)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusAccepted)
}

// ResignController makes the current controller resign
// @Summary notify the ticdc cluster controller to resign
// @Description notify the current controller to resign
// @Tags owner
// @Accept json
// @Produce json
// @Success 202
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v1/owner/resign [post]
func (h *OpenAPI) ResignController(c *gin.Context) {
	o, _ := h.capture.GetController()
	if o != nil {
		o.AsyncStop()
	}

	c.Status(http.StatusAccepted)
}

// GetProcessor gets the detailed info of a processor
// @Summary Get processor detail information
// @Description get the detail information of a processor
// @Tags processor
// @Accept json
// @Produce json
// @Param   changefeed_id   path    string  true  "changefeed ID"
// @Param   capture_id   path    string  true  "capture ID"
// @Success 200 {object} model.ProcessorDetail
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v1/processors/{changefeed_id}/{capture_id} [get]
func (h *OpenAPI) GetProcessor(c *gin.Context) {
	ctx := c.Request.Context()

	changefeedID := model.DefaultChangeFeedID(c.Param(api.ApiOpVarChangefeedID))
	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedID.ID))
		return
	}

	captureID := c.Param(api.ApiOpVarCaptureID)
	if err := model.ValidateChangefeedID(captureID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid capture_id: %s", captureID))
		return
	}

	info, err := h.capture.StatusProvider().GetChangeFeedInfo(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}
	if info.State != model.StateNormal {
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam,
			fmt.Errorf("changefeed in abnormal state: %s, "+
				"can't get processors of an abnormal changefeed",
				string(info.State))))
	}
	// check if this captureID exist
	procInfos, err := h.capture.StatusProvider().GetProcessors(ctx)
	if err != nil {
		_ = c.Error(err)
		return
	}
	var found bool
	for _, info := range procInfos {
		if info.CaptureID == captureID {
			found = true
			break
		}
	}
	if !found {
		_ = c.Error(cerror.ErrCaptureNotExist.GenWithStackByArgs(captureID))
		return
	}

	statuses, err := h.capture.StatusProvider().GetAllTaskStatuses(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}
	status, captureExist := statuses[captureID]

	// Note: for the case that no tables are attached to a newly created changefeed,
	//       we just do not report an error.
	var processorDetail model.ProcessorDetail
	if captureExist {
		tables := make([]int64, 0)
		for tableID := range status.Tables {
			tables = append(tables, tableID)
		}
		processorDetail.Tables = tables
	}
	c.IndentedJSON(http.StatusOK, &processorDetail)
}

// ListProcessor lists all processors in the TiCDC cluster
// @Summary List processors
// @Description list all processors in the TiCDC cluster
// @Tags processor
// @Accept json
// @Produce json
// @Success 200 {array} model.ProcessorCommonInfo
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v1/processors [get]
func (h *OpenAPI) ListProcessor(c *gin.Context) {
	ctx := c.Request.Context()
	controller, err := h.capture.GetController()
	if err != nil {
		_ = c.Error(err)
		return
	}
	infos, err := controller.GetProcessors(ctx)
	if err != nil {
		_ = c.Error(err)
		return
	}
	resps := make([]*model.ProcessorCommonInfo, len(infos))
	for i, info := range infos {
		resp := &model.ProcessorCommonInfo{
			Namespace: info.CfID.Namespace,
			CfID:      info.CfID.ID,
			CaptureID: info.CaptureID,
		}
		resps[i] = resp
	}
	c.IndentedJSON(http.StatusOK, resps)
}

// ListCapture lists all captures
// @Summary List captures
// @Description list all captures in cdc cluster
// @Tags capture
// @Accept json
// @Produce json
// @Success 200 {array} model.Capture
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v1/captures [get]
func (h *OpenAPI) ListCapture(c *gin.Context) {
	ctx := c.Request.Context()
	controller, err := h.capture.GetController()
	if err != nil {
		_ = c.Error(err)
		return
	}
	captureInfos, err := controller.GetCaptures(ctx)
	if err != nil {
		_ = c.Error(err)
		return
	}
	info, err := h.capture.Info()
	if err != nil {
		_ = c.Error(err)
		return
	}
	ownerID := info.ID

	captures := make([]*model.Capture, 0, len(captureInfos))
	for _, c := range captureInfos {
		isOwner := c.ID == ownerID
		captures = append(captures,
			&model.Capture{
				ID:            c.ID,
				IsOwner:       isOwner,
				AdvertiseAddr: c.AdvertiseAddr,
				ClusterID:     h.capture.GetEtcdClient().GetClusterID(),
			})
	}

	c.IndentedJSON(http.StatusOK, captures)
}

// DrainCapture remove all tables at the given capture.
// @Summary Drain captures
// @Description Drain all tables at the target captures in cdc cluster
// @Tags capture
// @Accept json
// @Produce json
// @Success 200,202
// @Failure 503,500,400 {object} model.HTTPError
// @Router	/api/v1/captures/drain [put]
func (h *OpenAPI) DrainCapture(c *gin.Context) {
	var req model.DrainCaptureRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.Wrap(err))
		return
	}

	ctx := c.Request.Context()
	captures, err := h.statusProvider().GetCaptures(ctx)
	if err != nil {
		_ = c.Error(err)
		return
	}

	// drain capture only work if there is at least two alive captures,
	// it cannot work properly if it has only one capture.
	if len(captures) <= 1 {
		_ = c.Error(cerror.ErrSchedulerRequestFailed.
			GenWithStackByArgs("only one capture alive"))
		return
	}

	target := req.CaptureID
	checkCaptureFound := func() bool {
		// make sure the target capture exist
		for _, capture := range captures {
			if capture.ID == target {
				return true
			}
		}
		return false
	}

	if !checkCaptureFound() {
		_ = c.Error(cerror.ErrCaptureNotExist.GenWithStackByArgs(target))
		return
	}

	// only owner handle api request, so this must be the owner.
	ownerInfo, err := h.capture.Info()
	if err != nil {
		_ = c.Error(err)
		return
	}

	if ownerInfo.ID == target {
		_ = c.Error(cerror.ErrSchedulerRequestFailed.
			GenWithStackByArgs("cannot drain the owner"))
		return
	}

	resp, err := api.HandleOwnerDrainCapture(ctx, h.capture, target)
	if err != nil {
		_ = c.AbortWithError(http.StatusServiceUnavailable, err)
		return
	}

	c.JSON(http.StatusAccepted, resp)
}

// ServerStatus gets the status of server(capture)
// @Summary Get server status
// @Description get the status of a server(capture)
// @Tags common
// @Accept json
// @Produce json
// @Success 200 {object} model.ServerStatus
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v1/status [get]
func (h *OpenAPI) ServerStatus(c *gin.Context) {
	info, err := h.capture.Info()
	if err != nil {
		_ = c.Error(err)
		return
	}
	status := model.ServerStatus{
		Version:   version.ReleaseVersion,
		GitHash:   version.GitHash,
		Pid:       os.Getpid(),
		ID:        info.ID,
		ClusterID: h.capture.GetEtcdClient().GetClusterID(),
		IsOwner:   h.capture.IsController(),
		Liveness:  h.capture.Liveness(),
	}
	c.IndentedJSON(http.StatusOK, status)
}

// Health check if cdc cluster is health
// @Summary Check if CDC cluster is health
// @Description check if CDC cluster is health
// @Tags common
// @Accept json
// @Produce json
// @Success 200
// @Failure 500 {object} model.HTTPError
// @Router	/api/v1/health [get]
func (h *OpenAPI) Health(c *gin.Context) {
	if !h.capture.IsController() {
		middleware.ForwardToControllerMiddleware(h.capture)(c)
		return
	}

	ctx := c.Request.Context()
	health, err := h.statusProvider().IsHealthy(ctx)
	if err != nil {
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
		return
	}
	if !health {
		err = cerror.ErrClusterIsUnhealthy.FastGenByArgs()
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
		return
	}
	c.Status(http.StatusOK)
}

// SetLogLevel changes TiCDC log level dynamically.
// @Summary Change TiCDC log level
// @Description change TiCDC log level dynamically
// @Tags common
// @Accept json
// @Produce json
// @Param log_level body string true "log level"
// @Success 200
// @Failure 400 {object} model.HTTPError
// @Router	/api/v1/log [post]
func SetLogLevel(c *gin.Context) {
	// get json data from request body
	data := struct {
		Level string `json:"log_level"`
	}{}
	err := c.BindJSON(&data)
	if err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid log level: %s", err.Error()))
		return
	}

	err = logutil.SetLogLevel(data.Level)
	if err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("fail to change log level: %s", data.Level))
		return
	}
	log.Warn("log level changed", zap.String("level", data.Level))
	c.Status(http.StatusOK)
}

// getChangefeedFromRequest returns the changefeed that parse from request
func getChangefeedFromRequest(ctx *gin.Context) model.ChangeFeedID {
	return model.ChangeFeedID{
		Namespace: model.DefaultNamespace,
		ID:        ctx.Param(api.ApiOpVarChangefeedID),
	}
}
