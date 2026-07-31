package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/relay/channel/task/taskcommon"
	relaycommon "github.com/QuantumNous/new-api/relay/common"

	"github.com/bytedance/gopkg/util/gopool"
	"github.com/samber/lo"
)

// TaskPollingAdaptor 定义轮询所需的最小适配器接口，避免 service -> relay 的循环依赖
type TaskPollingAdaptor interface {
	Init(info *relaycommon.RelayInfo)
	FetchTask(baseURL string, key string, body map[string]any, proxy string) (*http.Response, error)
	ParseTaskResult(body []byte) (*relaycommon.TaskInfo, error)
	// AdjustBillingOnComplete 在任务到达终态（成功/失败）时由轮询循环调用。
	// 返回正数触发差额结算（补扣/退还），返回 0 保持预扣费金额不变。
	AdjustBillingOnComplete(task *model.Task, taskResult *relaycommon.TaskInfo) int
}

// LocalTaskExecutor handles tasks whose upstream request is synchronous. The
// submit endpoint has already persisted the request payload and task row; this
// method runs later in the task worker.
type LocalTaskExecutor interface {
	ExecuteLocalTask(ctx context.Context, task *model.Task, ch *model.Channel) (*relaycommon.TaskInfo, []byte, error)
}

// RetryableLocalTaskError marks a transient synchronous-upstream failure that
// the local task worker may retry once without changing task or billing state.
type RetryableLocalTaskError struct {
	Err error
}

func (e *RetryableLocalTaskError) Error() string {
	return e.Err.Error()
}

func (e *RetryableLocalTaskError) Unwrap() error {
	return e.Err
}

// GetTaskAdaptorFunc 由 main 包注入，用于获取指定平台的任务适配器。
// 打破 service -> relay -> relay/channel -> service 的循环依赖。
var GetTaskAdaptorFunc func(platform constant.TaskPlatform) TaskPollingAdaptor

const (
	synchronousImageTaskTimeout = 300 * time.Second
	synchronousImageRetryDelay  = 2 * time.Second
	synchronousImageHeartbeat   = 30 * time.Second
	synchronousImageStaleAfter  = 90 * time.Second
)

var synchronousImageTaskSlotsOnce sync.Once
var synchronousImageTaskSlotPool chan struct{}

func synchronousImageTaskSlots() chan struct{} {
	synchronousImageTaskSlotsOnce.Do(func() {
		limit := constant.AsyncImageWorkerSlots
		if limit <= 0 {
			limit = constant.DefaultAsyncImageWorkerSlots
		}
		synchronousImageTaskSlotPool = make(chan struct{}, limit)
	})
	return synchronousImageTaskSlotPool
}

// sweepTimedOutTasks 在主轮询之前独立清理超时任务。
// 每次最多处理 100 条，剩余的下个周期继续处理。
// 使用 per-task CAS (UpdateWithStatus) 防止覆盖被正常轮询已推进的任务。
func sweepTimedOutTasks(ctx context.Context) {
	if constant.TaskTimeoutMinutes <= 0 {
		return
	}
	cutoff := time.Now().Unix() - int64(constant.TaskTimeoutMinutes)*60
	tasks := model.GetTimedOutUnfinishedTasks(cutoff, 100)
	if len(tasks) == 0 {
		return
	}

	reason := fmt.Sprintf("任务超时（%d分钟）", constant.TaskTimeoutMinutes)
	legacyReason := "任务超时（旧系统遗留任务，不进行退款，请联系管理员）"
	now := time.Now().Unix()
	timedOutCount := 0

	for _, task := range tasks {
		isLegacy := task.SubmitTime > 0 && task.SubmitTime < model.TaskRefundLegacyCutoff

		oldStatus := task.Status
		payloadFile := task.PrivateData.RequestPayloadFile
		task.Status = model.TaskStatusFailure
		task.Progress = "100%"
		task.FinishTime = now
		task.PrivateData.RequestBody = nil
		task.PrivateData.RequestPayloadFile = ""
		task.PrivateData.RequestContentType = ""
		if isLegacy {
			task.FailReason = legacyReason
			// 旧系统任务明确不退款，随终态 CAS 一并清掉 quota，
			// 避免留下可再次退款的计费状态。
			task.Quota = 0
		} else {
			task.FailReason = reason
		}

		won, err := task.UpdateWithStatus(oldStatus)
		if err != nil {
			logger.LogError(ctx, fmt.Sprintf("sweepTimedOutTasks CAS update error for task %s: %v", task.TaskID, err))
			continue
		}
		if !won {
			logger.LogInfo(ctx, fmt.Sprintf("sweepTimedOutTasks: task %s already transitioned, skip", task.TaskID))
			continue
		}
		timedOutCount++
		if err := RemoveAsyncImagePayload(payloadFile); err != nil {
			logger.LogWarn(ctx, fmt.Sprintf("remove timed-out image task payload %s: %v", task.TaskID, err))
		}
		if !isLegacy && task.Quota != 0 {
			RefundTaskQuota(ctx, task, reason)
		}
	}

	if timedOutCount > 0 {
		logger.LogInfo(ctx, fmt.Sprintf("sweepTimedOutTasks: timed out %d tasks", timedOutCount))
	}
}

func failInterruptedSynchronousImageTasks(ctx context.Context) int {
	cutoff := time.Now().Add(-synchronousImageStaleAfter).Unix()
	limit := constant.TaskQueryLimit
	if limit <= 0 {
		limit = 1000
	}
	const reason = "同步生图任务因 NewAPI 重启或 worker 中断，结果无法确认"
	tasks, err := model.FailStaleInProgressLocalImageTasks(cutoff, limit, reason)
	if err != nil {
		logger.LogError(ctx, fmt.Sprintf("fail interrupted synchronous image tasks: %v", err))
		return 0
	}
	if len(tasks) == 0 {
		return 0
	}

	for _, failed := range tasks {
		if err := RemoveAsyncImagePayload(failed.PayloadFile); err != nil {
			logger.LogWarn(ctx, fmt.Sprintf("remove interrupted image task payload %s: %v", failed.Task.TaskID, err))
		}
		if failed.Task.Quota != 0 {
			RefundTaskQuota(ctx, failed.Task, reason)
		}
	}
	return len(tasks)
}

// TaskPollSummary is the result recorded on an async_task_poll system task row,
// summarizing one polling pass.
type TaskPollSummary struct {
	UnfinishedTasks  int `json:"unfinished_tasks"`
	PlatformsScanned int `json:"platforms_scanned"`
	NullTasksFailed  int `json:"null_tasks_failed"`
}

// RunTaskPollingOnce performs one async-task (Suno/video) polling pass
// synchronously. It honors ctx cancellation (the system-task runner cancels it
// when the lease is lost) and, when report is non-nil, reports progress as
// (processedPlatforms, totalPlatforms). It returns immediately if the task
// adaptor factory has not been wired yet, to avoid a nil call during startup.
func RunTaskPollingOnce(ctx context.Context, report func(processed, total int)) TaskPollSummary {
	summary := TaskPollSummary{}
	if GetTaskAdaptorFunc == nil {
		return summary
	}
	if ctx == nil {
		ctx = context.Background()
	}

	common.SysLog("任务进度轮询开始")
	sweepTimedOutTasks(ctx)
	if failed := failInterruptedSynchronousImageTasks(ctx); failed > 0 {
		logger.LogWarn(ctx, fmt.Sprintf("failed and refunded %d interrupted synchronous image tasks", failed))
	}
	allTasks := model.GetAllUnFinishRemoteTasks(constant.TaskQueryLimit)
	localTaskLimit := cap(synchronousImageTaskSlots()) - len(synchronousImageTaskSlots())
	if constant.TaskQueryLimit > 0 && localTaskLimit > constant.TaskQueryLimit {
		localTaskLimit = constant.TaskQueryLimit
	}
	localTasks := model.GetQueuedLocalImageTasks(localTaskLimit)
	summary.UnfinishedTasks = len(allTasks)
	platformTask := make(map[constant.TaskPlatform][]*model.Task)
	localPlatformTask := make(map[constant.TaskPlatform][]*model.Task)
	platforms := make(map[constant.TaskPlatform]struct{})
	for _, t := range allTasks {
		platforms[t.Platform] = struct{}{}
		platformTask[t.Platform] = append(platformTask[t.Platform], t)
	}
	if len(localTasks) > 0 {
		summary.UnfinishedTasks += len(localTasks)
		platforms[constant.TaskPlatformAsyncImage] = struct{}{}
		localPlatformTask[constant.TaskPlatformAsyncImage] = localTasks
		localTasks = nil
	}

	totalPlatforms := len(platforms)
	processedPlatforms := 0
	for platform := range platforms {
		if ctx.Err() != nil {
			break
		}
		if report != nil {
			report(processedPlatforms, totalPlatforms)
		}
		processedPlatforms++
		tasks := platformTask[platform]
		localTasks := localPlatformTask[platform]
		if len(tasks) == 0 && len(localTasks) == 0 {
			continue
		}
		summary.PlatformsScanned++
		if len(localTasks) > 0 {
			if err := UpdateLocalTasks(ctx, platform, localTasks); err != nil {
				common.SysLog(fmt.Sprintf("UpdateLocalTasks fail: %s", err))
			}
			delete(localPlatformTask, platform)
			localTasks = nil
		}
		taskChannelM := make(map[int][]string)
		taskM := make(map[string]*model.Task)
		nullTaskIds := make([]int64, 0)
		for _, task := range tasks {
			upstreamID := task.GetUpstreamTaskID()
			if upstreamID == "" {
				// 统计失败的未完成任务
				nullTaskIds = append(nullTaskIds, task.ID)
				continue
			}
			taskM[upstreamID] = task
			taskChannelM[task.ChannelId] = append(taskChannelM[task.ChannelId], upstreamID)
		}
		if len(nullTaskIds) > 0 {
			summary.NullTasksFailed += len(nullTaskIds)
			err := model.TaskBulkUpdateByID(nullTaskIds, map[string]any{
				"status":   "FAILURE",
				"progress": "100%",
			})
			if err != nil {
				logger.LogError(ctx, fmt.Sprintf("Fix null task_id task error: %v", err))
			} else {
				logger.LogInfo(ctx, fmt.Sprintf("Fix null task_id task success: %v", nullTaskIds))
			}
		}
		if len(taskChannelM) == 0 {
			continue
		}

		DispatchPlatformUpdate(ctx, platform, taskChannelM, taskM)
	}
	if report != nil && ctx.Err() == nil {
		report(totalPlatforms, totalPlatforms)
	}
	common.SysLog("任务进度轮询完成")
	return summary
}

// UpdateLocalTasks dispatches persisted tasks for synchronous upstreams.
// Synchronous image workers outlive the polling pass and refill available
// slots without retaining the complete queued-task batch in memory.
func UpdateLocalTasks(ctx context.Context, platform constant.TaskPlatform, tasks []*model.Task) error {
	if len(tasks) == 0 {
		return nil
	}
	if GetTaskAdaptorFunc == nil {
		return errors.New("task adaptor factory not configured")
	}
	adaptor := GetTaskAdaptorFunc(platform)
	if adaptor == nil {
		return fmt.Errorf("task adaptor not found for platform %s", platform)
	}
	executor, ok := adaptor.(LocalTaskExecutor)
	if !ok {
		return fmt.Errorf("platform %s does not support local task execution", platform)
	}
	if platform != constant.TaskPlatformAsyncImage {
		for _, task := range tasks {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if task == nil || task.Status == model.TaskStatusInProgress {
				continue
			}
			if err := updateLocalSingleTask(ctx, adaptor, executor, task); err != nil {
				logger.LogError(ctx, fmt.Sprintf("Failed to execute local task %s: %s", task.TaskID, err.Error()))
			}
		}
		return nil
	}

	for _, task := range tasks {
		if task == nil || task.Status == model.TaskStatusInProgress {
			continue
		}
		if task.Platform != constant.TaskPlatformAsyncImage ||
			task.PrivateData.UpstreamMode != constant.TaskImageUpstreamModeSync {
			if err := updateLocalSingleTask(ctx, adaptor, executor, task); err != nil {
				logger.LogError(ctx, fmt.Sprintf("Failed to execute local task %s: %s", task.TaskID, err.Error()))
			}
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		tryStartSynchronousImageTask(task, adaptor, executor)
	}
	return nil
}

// TryDispatchLocalTask starts a newly persisted synchronous image task when a
// worker slot is available. Tasks beyond the process-wide limit remain queued
// for the persistent task runner, which uses the same slots.
func TryDispatchLocalTask(task *model.Task) bool {
	if task == nil || task.Platform != constant.TaskPlatformAsyncImage ||
		task.PrivateData.UpstreamMode != constant.TaskImageUpstreamModeSync ||
		(task.PrivateData.RequestPayloadFile == "" && len(task.PrivateData.RequestBody) == 0) || GetTaskAdaptorFunc == nil {
		return false
	}
	adaptor := GetTaskAdaptorFunc(task.Platform)
	if adaptor == nil {
		return false
	}
	executor, ok := adaptor.(LocalTaskExecutor)
	if !ok {
		return false
	}
	return tryStartSynchronousImageTask(task, adaptor, executor)
}

func tryStartSynchronousImageTask(task *model.Task, adaptor TaskPollingAdaptor, executor LocalTaskExecutor) bool {
	slots := synchronousImageTaskSlots()
	select {
	case slots <- struct{}{}:
	default:
		return false
	}

	gopool.Go(func() {
		defer func() {
			<-slots
			if _, _, err := EnqueueSystemTask(model.SystemTaskTypeAsyncTaskPoll, nil); err != nil {
				common.SysError("enqueue image task polling after worker completion: " + err.Error())
			}
		}()
		workerCtx, cancelWorker := context.WithCancel(context.Background())
		heartbeatDone := make(chan struct{})
		gopool.Go(func() {
			defer close(heartbeatDone)
			ticker := time.NewTicker(synchronousImageHeartbeat)
			defer ticker.Stop()
			for {
				select {
				case <-workerCtx.Done():
					return
				case <-ticker.C:
					if err := model.TouchInProgressLocalImageTask(task.ID); err != nil {
						logger.LogWarn(workerCtx, fmt.Sprintf("heartbeat synchronous image task %s: %v", task.TaskID, err))
					}
				}
			}
		})
		defer func() {
			cancelWorker()
			<-heartbeatDone
		}()
		if err := updateLocalSingleTask(workerCtx, adaptor, executor, task); err != nil {
			logger.LogError(workerCtx, fmt.Sprintf("Failed to execute local task %s: %s", task.TaskID, err.Error()))
		}
	})
	return true
}

func updateLocalSingleTask(ctx context.Context, adaptor TaskPollingAdaptor, executor LocalTaskExecutor, task *model.Task) error {
	ch, err := model.CacheGetChannel(task.ChannelId)
	if err != nil {
		return fmt.Errorf("get channel %d: %w", task.ChannelId, err)
	}

	isSynchronousImageTask := task.Platform == constant.TaskPlatformAsyncImage &&
		task.PrivateData.UpstreamMode == constant.TaskImageUpstreamModeSync
	previousStatus := task.Status
	now := time.Now().Unix()
	task.Status = model.TaskStatusInProgress
	task.Progress = taskcommon.ProgressInProgress
	if task.StartTime == 0 {
		task.StartTime = now
	}
	var won bool
	if isSynchronousImageTask {
		won, err = task.ClaimLocalTaskWithStatus(previousStatus)
	} else {
		won, err = task.UpdateWithStatus(previousStatus)
	}
	if err != nil {
		return fmt.Errorf("claim local task %s: %w", task.TaskID, err)
	}
	if !won {
		return nil
	}

	payloadFile := task.PrivateData.RequestPayloadFile
	if payloadFile != "" {
		defer func() {
			if removeErr := RemoveAsyncImagePayload(payloadFile); removeErr != nil {
				logger.LogWarn(ctx, fmt.Sprintf("remove image task payload %s: %v", task.TaskID, removeErr))
			}
		}()
	}

	taskCtx := ctx
	cancelTask := func() {}
	if isSynchronousImageTask {
		taskCtx, cancelTask = context.WithTimeout(ctx, synchronousImageTaskTimeout)
	}
	defer cancelTask()

	var taskResult *relaycommon.TaskInfo
	var responseBody []byte
	var executeErr error
	if isSynchronousImageTask {
		taskResult, responseBody, executeErr = executeSynchronousImageTask(
			taskCtx,
			executor,
			task,
			ch,
			synchronousImageRetryDelay,
		)
	} else {
		taskResult, responseBody, executeErr = executor.ExecuteLocalTask(taskCtx, task, ch)
	}
	if executeErr != nil {
		failureReason := executeErr.Error()
		if errors.Is(executeErr, context.DeadlineExceeded) {
			failureReason = fmt.Sprintf("synchronous image upstream timed out after %d seconds", int(synchronousImageTaskTimeout.Seconds()))
		}
		if err := failLocalTask(ctx, task, failureReason, responseBody); err != nil {
			return err
		}
		return executeErr
	}
	if taskResult == nil {
		err := fmt.Errorf("local task %s returned no result", task.TaskID)
		if persistErr := failLocalTask(ctx, task, err.Error(), responseBody); persistErr != nil {
			return persistErr
		}
		return err
	}

	task.Status = model.TaskStatus(taskResult.Status)
	if task.Status == "" {
		task.Status = model.TaskStatusSuccess
	}
	if task.Status != model.TaskStatusSuccess && task.Status != model.TaskStatusFailure {
		err := fmt.Errorf("local task %s returned non-terminal status %s", task.TaskID, task.Status)
		if persistErr := failLocalTask(ctx, task, err.Error(), responseBody); persistErr != nil {
			return persistErr
		}
		return err
	}
	task.Progress = taskResult.Progress
	if task.Progress == "" {
		task.Progress = taskcommon.ProgressComplete
	}
	task.FinishTime = time.Now().Unix()
	task.Data = responseBody
	task.PrivateData.RequestPayloadFile = ""
	task.PrivateData.RequestBody = nil
	task.PrivateData.RequestContentType = ""
	if taskResult.Url != "" {
		task.PrivateData.ResultURL = taskResult.Url
	}
	if task.Status == model.TaskStatusFailure {
		task.FailReason = taskResult.Reason
	}
	updated, err := task.UpdateWithStatus(model.TaskStatusInProgress)
	if err != nil {
		return fmt.Errorf("persist local task result %s: %w", task.TaskID, err)
	}
	if !updated {
		return nil
	}
	if task.Status == model.TaskStatusSuccess {
		settleTaskBillingOnComplete(ctx, adaptor, task, taskResult)
	} else if task.Status == model.TaskStatusFailure && task.Quota != 0 {
		RefundTaskQuota(ctx, task, task.FailReason)
	}
	return nil
}

func executeSynchronousImageTask(
	ctx context.Context,
	executor LocalTaskExecutor,
	task *model.Task,
	ch *model.Channel,
	retryDelay time.Duration,
) (*relaycommon.TaskInfo, []byte, error) {
	task.PrivateData.LocalTaskAttempts++
	taskResult, responseBody, err := executor.ExecuteLocalTask(ctx, task, ch)
	var retryableErr *RetryableLocalTaskError
	if err == nil || !errors.As(err, &retryableErr) {
		return taskResult, responseBody, err
	}

	logger.LogWarn(ctx, fmt.Sprintf(
		"retry synchronous image task %s once after transient upstream failure: %v",
		task.TaskID,
		err,
	))
	timer := time.NewTimer(retryDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, responseBody, ctx.Err()
	case <-timer.C:
	}

	task.PrivateData.LocalTaskAttempts++
	return executor.ExecuteLocalTask(ctx, task, ch)
}

func failLocalTask(ctx context.Context, task *model.Task, reason string, responseBody []byte) error {
	task.Status = model.TaskStatusFailure
	task.Progress = taskcommon.ProgressComplete
	task.FinishTime = time.Now().Unix()
	task.FailReason = reason
	task.Data = responseBody
	task.PrivateData.RequestPayloadFile = ""
	task.PrivateData.RequestBody = nil
	task.PrivateData.RequestContentType = ""
	updated, updateErr := task.UpdateWithStatus(model.TaskStatusInProgress)
	if updateErr != nil {
		return fmt.Errorf("persist local task failure %s: %w", task.TaskID, updateErr)
	}
	if !updated {
		return nil
	}
	if task.Quota != 0 {
		RefundTaskQuota(ctx, task, task.FailReason)
	}
	return nil
}

// DispatchPlatformUpdate 按平台分发轮询更新
func DispatchPlatformUpdate(ctx context.Context, platform constant.TaskPlatform, taskChannelM map[int][]string, taskM map[string]*model.Task) {
	if ctx == nil {
		ctx = context.Background()
	}
	switch platform {
	case constant.TaskPlatformMidjourney:
		// MJ 轮询由其自身处理，这里预留入口
	case constant.TaskPlatformSuno:
		_ = UpdateSunoTasks(ctx, taskChannelM, taskM)
	default:
		if err := UpdateVideoTasks(ctx, platform, taskChannelM, taskM); err != nil {
			common.SysLog(fmt.Sprintf("UpdateVideoTasks fail: %s", err))
		}
	}
}

// UpdateSunoTasks 按渠道更新所有 Suno 任务
func UpdateSunoTasks(ctx context.Context, taskChannelM map[int][]string, taskM map[string]*model.Task) error {
	for channelId, taskIds := range taskChannelM {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := updateSunoTasks(ctx, channelId, taskIds, taskM)
		if err != nil {
			logger.LogError(ctx, fmt.Sprintf("渠道 #%d 更新异步任务失败: %s", channelId, err.Error()))
		}
	}
	return nil
}

func updateSunoTasks(ctx context.Context, channelId int, taskIds []string, taskM map[string]*model.Task) error {
	logger.LogInfo(ctx, fmt.Sprintf("渠道 #%d 未完成的任务有: %d", channelId, len(taskIds)))
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if len(taskIds) == 0 {
		return nil
	}
	ch, err := model.CacheGetChannel(channelId)
	if err != nil {
		common.SysLog(fmt.Sprintf("CacheGetChannel: %v", err))
		// Collect DB primary key IDs for bulk update (taskIds are upstream IDs, not task_id column values)
		var failedIDs []int64
		for _, upstreamID := range taskIds {
			if t, ok := taskM[upstreamID]; ok {
				failedIDs = append(failedIDs, t.ID)
			}
		}
		err = model.TaskBulkUpdateByID(failedIDs, map[string]any{
			"fail_reason": fmt.Sprintf("获取渠道信息失败，请联系管理员，渠道ID：%d", channelId),
			"status":      "FAILURE",
			"progress":    "100%",
		})
		if err != nil {
			common.SysLog(fmt.Sprintf("UpdateSunoTask error: %v", err))
		}
		return err
	}
	adaptor := GetTaskAdaptorFunc(constant.TaskPlatformSuno)
	if adaptor == nil {
		return errors.New("adaptor not found")
	}
	proxy := ch.GetSetting().Proxy
	resp, err := adaptor.FetchTask(*ch.BaseURL, ch.Key, map[string]any{
		"ids": taskIds,
	}, proxy)
	if err != nil {
		common.SysLog(fmt.Sprintf("Get Task Do req error: %v", err))
		return err
	}
	if resp.StatusCode != http.StatusOK {
		logger.LogError(ctx, fmt.Sprintf("Get Task status code: %d", resp.StatusCode))
		return fmt.Errorf("Get Task status code: %d", resp.StatusCode)
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		common.SysLog(fmt.Sprintf("Get Suno Task parse body error: %v", err))
		return err
	}
	var responseItems dto.TaskResponse[[]dto.SunoDataResponse]
	err = common.Unmarshal(responseBody, &responseItems)
	if err != nil {
		logger.LogError(ctx, fmt.Sprintf("Get Suno Task parse body error2: %v, body: %s", err, string(responseBody)))
		return err
	}
	if !responseItems.IsSuccess() {
		common.SysLog(fmt.Sprintf("渠道 #%d 未完成的任务有: %d, 成功获取到任务数: %s", channelId, len(taskIds), string(responseBody)))
		return err
	}

	for _, responseItem := range responseItems.Data {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		task := taskM[responseItem.TaskID]
		if task == nil {
			logger.LogWarn(ctx, fmt.Sprintf("Suno task response ignored: unknown task_id=%s", responseItem.TaskID))
			continue
		}
		if !taskNeedsUpdate(task, responseItem) {
			continue
		}

		prevStatus := task.Status
		task.Status = lo.If(model.TaskStatus(responseItem.Status) != "", model.TaskStatus(responseItem.Status)).Else(task.Status)
		task.FailReason = lo.If(responseItem.FailReason != "", responseItem.FailReason).Else(task.FailReason)
		task.SubmitTime = lo.If(responseItem.SubmitTime != 0, responseItem.SubmitTime).Else(task.SubmitTime)
		task.StartTime = lo.If(responseItem.StartTime != 0, responseItem.StartTime).Else(task.StartTime)
		task.FinishTime = lo.If(responseItem.FinishTime != 0, responseItem.FinishTime).Else(task.FinishTime)
		isFailure := responseItem.FailReason != "" || task.Status == model.TaskStatusFailure
		if isFailure {
			logger.LogInfo(ctx, task.TaskID+" 构建失败，"+task.FailReason)
			task.Status = model.TaskStatusFailure
			task.Progress = "100%"
		}
		if responseItem.Status == model.TaskStatusSuccess {
			task.Progress = "100%"
		}
		task.Data = responseItem.Data

		// 持久化走 CAS，防止重叠轮询/sweep/多实例/持久化失败重试导致重复退款或覆盖终态。
		won, err := task.UpdateWithStatus(prevStatus)
		if err != nil {
			logger.LogError(ctx, fmt.Sprintf("UpdateSunoTask task %s error: %v", task.TaskID, err))
		} else if !won {
			logger.LogWarn(ctx, fmt.Sprintf("Task %s CAS lost or no-op update, skip billing", task.TaskID))
		} else if isFailure && prevStatus != model.TaskStatusFailure && task.Quota != 0 {
			RefundTaskQuota(ctx, task, task.FailReason)
		}
	}
	return nil
}

// taskNeedsUpdate 检查 Suno 任务是否需要更新
func taskNeedsUpdate(oldTask *model.Task, newTask dto.SunoDataResponse) bool {
	if oldTask.SubmitTime != newTask.SubmitTime {
		return true
	}
	if oldTask.StartTime != newTask.StartTime {
		return true
	}
	if oldTask.FinishTime != newTask.FinishTime {
		return true
	}
	if string(oldTask.Status) != newTask.Status {
		return true
	}
	if oldTask.FailReason != newTask.FailReason {
		return true
	}

	if (oldTask.Status == model.TaskStatusFailure || oldTask.Status == model.TaskStatusSuccess) && oldTask.Progress != "100%" {
		return true
	}

	oldData, _ := common.Marshal(oldTask.Data)
	newData, _ := common.Marshal(newTask.Data)

	sort.Slice(oldData, func(i, j int) bool {
		return oldData[i] < oldData[j]
	})
	sort.Slice(newData, func(i, j int) bool {
		return newData[i] < newData[j]
	})

	if string(oldData) != string(newData) {
		return true
	}
	return false
}

// UpdateVideoTasks 按渠道更新所有视频任务
func UpdateVideoTasks(ctx context.Context, platform constant.TaskPlatform, taskChannelM map[int][]string, taskM map[string]*model.Task) error {
	channelIDs := make([]int, 0, len(taskChannelM))
	for channelID := range taskChannelM {
		channelIDs = append(channelIDs, channelID)
	}
	sort.Ints(channelIDs)

	var wg sync.WaitGroup
	for _, channelId := range channelIDs {
		taskIds := taskChannelM[channelId]
		if len(taskIds) == 0 {
			continue
		}
		taskIds = append([]string(nil), taskIds...)

		wg.Add(1)
		gopool.Go(func() {
			defer wg.Done()
			if err := updateVideoTasks(ctx, platform, channelId, taskIds, taskM); err != nil {
				logger.LogError(ctx, fmt.Sprintf("Channel #%d failed to update video async tasks: %s", channelId, err.Error()))
			}
		})
	}
	wg.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func updateVideoTasks(ctx context.Context, platform constant.TaskPlatform, channelId int, taskIds []string, taskM map[string]*model.Task) error {
	logger.LogInfo(ctx, fmt.Sprintf("Channel #%d pending video tasks: %d", channelId, len(taskIds)))
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if len(taskIds) == 0 {
		return nil
	}
	cacheGetChannel, err := model.CacheGetChannel(channelId)
	if err != nil {
		// Collect DB primary key IDs for bulk update (taskIds are upstream IDs, not task_id column values)
		var failedIDs []int64
		for _, upstreamID := range taskIds {
			if t, ok := taskM[upstreamID]; ok {
				failedIDs = append(failedIDs, t.ID)
			}
		}
		errUpdate := model.TaskBulkUpdateByID(failedIDs, map[string]any{
			"fail_reason": fmt.Sprintf("Failed to get channel info, channel ID: %d", channelId),
			"status":      "FAILURE",
			"progress":    "100%",
		})
		if errUpdate != nil {
			common.SysLog(fmt.Sprintf("UpdateVideoTask error: %v", errUpdate))
		}
		return fmt.Errorf("CacheGetChannel failed: %w", err)
	}
	adaptor := GetTaskAdaptorFunc(platform)
	if adaptor == nil {
		return fmt.Errorf("video adaptor not found")
	}
	info := &relaycommon.RelayInfo{}
	info.ChannelMeta = &relaycommon.ChannelMeta{
		ChannelBaseUrl: cacheGetChannel.GetBaseURL(),
	}
	info.ApiKey = cacheGetChannel.Key
	adaptor.Init(info)
	disablePollingSleep := cacheGetChannel.GetOtherSettings().DisableTaskPollingSleep
	for i, taskId := range taskIds {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := updateVideoSingleTask(ctx, adaptor, cacheGetChannel, taskId, taskM); err != nil {
			logger.LogError(ctx, fmt.Sprintf("Failed to update video task %s: %s", taskId, err.Error()))
		}
		if disablePollingSleep || i == len(taskIds)-1 {
			continue
		}

		// sleep 1 second between tasks for this channel only.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}
	return nil
}

func updateVideoSingleTask(ctx context.Context, adaptor TaskPollingAdaptor, ch *model.Channel, taskId string, taskM map[string]*model.Task) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	baseURL := constant.ChannelBaseURLs[ch.Type]
	if ch.GetBaseURL() != "" {
		baseURL = ch.GetBaseURL()
	}
	proxy := ch.GetSetting().Proxy

	task := taskM[taskId]
	if task == nil {
		logger.LogError(ctx, fmt.Sprintf("Task %s not found in taskM", taskId))
		return fmt.Errorf("task %s not found", taskId)
	}
	key := ch.Key

	privateData := task.PrivateData
	if privateData.Key != "" {
		key = privateData.Key
	}
	resp, err := adaptor.FetchTask(baseURL, key, map[string]any{
		"task_id": task.GetUpstreamTaskID(),
		"action":  task.Action,
	}, proxy)
	if err != nil {
		return fmt.Errorf("fetchTask failed for task %s: %w", taskId, err)
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("readAll failed for task %s: %w", taskId, err)
	}
	isImageTask := task.Action == constant.TaskActionImageGenerate || task.Action == constant.TaskActionImageEdit
	if isImageTask {
		responseBody, err = RewriteImageResponseURLs(responseBody, ch.GetOtherSettings())
		if err != nil {
			return fmt.Errorf("rewrite image task response URLs for task %s: %w", taskId, err)
		}
	}

	logger.LogDebug(ctx, "updateVideoSingleTask response: %s", responseBody)

	snap := task.Snapshot()

	taskResult := &relaycommon.TaskInfo{}
	// try parse as New API response format
	var responseItems dto.TaskResponse[model.Task]
	if err = common.Unmarshal(responseBody, &responseItems); err == nil && responseItems.IsSuccess() {
		logger.LogDebug(ctx, "updateVideoSingleTask parsed as new api response format: %+v", responseItems)
		t := responseItems.Data
		taskResult.TaskID = t.TaskID
		taskResult.Status = string(t.Status)
		taskResult.Url = t.GetResultURL()
		taskResult.Progress = t.Progress
		taskResult.Reason = t.FailReason
		task.Data = t.Data
	} else if taskResult, err = adaptor.ParseTaskResult(responseBody); err != nil {
		return fmt.Errorf("parseTaskResult failed for task %s: %w", taskId, err)
	}

	task.Data = redactVideoResponseBody(responseBody)

	logger.LogDebug(ctx, "updateVideoSingleTask taskResult: %+v", taskResult)

	now := time.Now().Unix()
	if taskResult.Status == "" {
		//taskResult = relaycommon.FailTaskInfo("upstream returned empty status")
		errorResult := &dto.GeneralErrorResponse{}
		if err = common.Unmarshal(responseBody, &errorResult); err == nil {
			openaiError := errorResult.TryToOpenAIError()
			if openaiError != nil {
				// 返回规范的 OpenAI 错误格式，提取错误信息，判断错误是否为任务失败
				if openaiError.Code == "429" {
					// 429 错误通常表示请求过多或速率限制，暂时不认为是任务失败，保持原状态等待下一轮轮询
					return nil
				}

				// 其他错误认为是任务失败，记录错误信息并更新任务状态
				taskResult = relaycommon.FailTaskInfo("upstream returned error")
			} else {
				// unknown error format, log original response
				logger.LogError(ctx, fmt.Sprintf("Task %s returned empty status with unrecognized error format, response: %s", taskId, string(responseBody)))
				taskResult = relaycommon.FailTaskInfo("upstream returned unrecognized message")
			}
		}
	}

	shouldRefund := false
	shouldSettle := false
	quota := task.Quota

	task.Status = model.TaskStatus(taskResult.Status)
	switch taskResult.Status {
	case model.TaskStatusSubmitted:
		task.Progress = taskcommon.ProgressSubmitted
	case model.TaskStatusQueued:
		task.Progress = taskcommon.ProgressQueued
	case model.TaskStatusInProgress:
		task.Progress = taskcommon.ProgressInProgress
		if task.StartTime == 0 {
			task.StartTime = now
		}
	case model.TaskStatusSuccess:
		task.Progress = taskcommon.ProgressComplete
		if task.FinishTime == 0 {
			task.FinishTime = now
		}
		if strings.HasPrefix(taskResult.Url, "data:") && !isImageTask {
			// data: URI (e.g. Vertex base64 encoded video) — keep in Data, not in ResultURL
			task.PrivateData.ResultURL = taskcommon.BuildProxyURL(task.TaskID)
		} else if taskResult.Url != "" {
			// Direct upstream URL (e.g. Kling, Ali, Doubao, etc.)
			task.PrivateData.ResultURL = taskResult.Url
		} else if !isImageTask {
			// No URL from adaptor — construct proxy URL using public task ID
			task.PrivateData.ResultURL = taskcommon.BuildProxyURL(task.TaskID)
		}
		shouldSettle = true
	case model.TaskStatusFailure:
		logger.LogJson(ctx, fmt.Sprintf("Task %s failed", taskId), task)
		task.Status = model.TaskStatusFailure
		task.Progress = taskcommon.ProgressComplete
		if task.FinishTime == 0 {
			task.FinishTime = now
		}
		task.FailReason = taskResult.Reason
		logger.LogInfo(ctx, fmt.Sprintf("Task %s failed: %s", task.TaskID, task.FailReason))
		taskResult.Progress = taskcommon.ProgressComplete
		if quota != 0 {
			shouldRefund = true
		}
	default:
		return fmt.Errorf("unknown task status %s for task %s", taskResult.Status, task.TaskID)
	}
	if taskResult.Progress != "" {
		task.Progress = taskResult.Progress
	}

	isDone := task.Status == model.TaskStatusSuccess || task.Status == model.TaskStatusFailure
	if isDone && snap.Status != task.Status {
		won, err := task.UpdateWithStatus(snap.Status)
		if err != nil {
			logger.LogError(ctx, fmt.Sprintf("UpdateWithStatus failed for task %s: %s", task.TaskID, err.Error()))
			shouldRefund = false
			shouldSettle = false
		} else if !won {
			logger.LogWarn(ctx, fmt.Sprintf("Task %s CAS lost or no-op update, skip billing", task.TaskID))
			shouldRefund = false
			shouldSettle = false
		}
	} else if !snap.Equal(task.Snapshot()) {
		if _, err := task.UpdateWithStatus(snap.Status); err != nil {
			logger.LogError(ctx, fmt.Sprintf("Failed to update task %s: %s", task.TaskID, err.Error()))
		}
	} else {
		// No changes, skip update
		logger.LogDebug(ctx, "No update needed for task %s", task.TaskID)
	}

	if shouldSettle {
		settleTaskBillingOnComplete(ctx, adaptor, task, taskResult)
	}
	if shouldRefund {
		RefundTaskQuota(ctx, task, task.FailReason)
	}

	return nil
}

func redactVideoResponseBody(body []byte) []byte {
	var m map[string]any
	if err := common.Unmarshal(body, &m); err != nil {
		return body
	}
	resp, _ := m["response"].(map[string]any)
	if resp != nil {
		delete(resp, "bytesBase64Encoded")
		if v, ok := resp["video"].(string); ok {
			resp["video"] = truncateBase64(v)
		}
		if vs, ok := resp["videos"].([]any); ok {
			for i := range vs {
				if vm, ok := vs[i].(map[string]any); ok {
					delete(vm, "bytesBase64Encoded")
				}
			}
		}
	}
	b, err := common.Marshal(m)
	if err != nil {
		return body
	}
	return b
}

func truncateBase64(s string) string {
	const maxKeep = 256
	if len(s) <= maxKeep {
		return s
	}
	return s[:maxKeep] + "..."
}

// settleTaskBillingOnComplete 任务完成时的统一计费调整。
// 优先级：1. adaptor.AdjustBillingOnComplete 返回正数 → 使用 adaptor 计算的额度
//
//  2. taskResult.TotalTokens > 0 → 按 token 重算
//  3. 都不满足 → 保持预扣额度不变
func settleTaskBillingOnComplete(ctx context.Context, adaptor TaskPollingAdaptor, task *model.Task, taskResult *relaycommon.TaskInfo) {
	// 0. 按次计费的任务不做差额结算
	if bc := task.PrivateData.BillingContext; bc != nil && bc.PerCallBilling {
		logger.LogInfo(ctx, fmt.Sprintf("任务 %s 按次计费，跳过差额结算", task.TaskID))
		return
	}
	// 1. 优先让 adaptor 决定最终额度
	if actualQuota := adaptor.AdjustBillingOnComplete(task, taskResult); actualQuota > 0 {
		RecalculateTaskQuota(ctx, task, actualQuota, "adaptor计费调整")
		return
	}
	// 2. 回退到 token 重算
	if taskResult.TotalTokens > 0 {
		RecalculateTaskQuotaByTokens(ctx, task, taskResult.TotalTokens)
		return
	}
	// 3. 无调整，保持预扣额度
}
