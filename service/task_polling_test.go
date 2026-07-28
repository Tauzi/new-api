package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/bytedance/gopkg/util/gopool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type taskPollingFetchAdaptor struct {
	mu           sync.Mutex
	taskIDs      []string
	fetched      chan string
	blockTaskID  string
	blockStarted chan struct{}
	releaseBlock chan struct{}
	blockOnce    sync.Once
}

type sunoFailurePollingAdaptor struct {
	failReason string
}

type localTaskExecutorAdaptor struct {
	calls int
}

type timeoutThenSuccessLocalTaskExecutor struct {
	localTaskExecutorAdaptor
	timeoutFailures int
	deadlines       []time.Duration
}

type blockingLocalTaskExecutor struct {
	localTaskExecutorAdaptor
	mu        sync.Mutex
	started   chan string
	release   chan struct{}
	active    int
	maxActive int
}

type completedImagePollingAdaptor struct{}

func (a *completedImagePollingAdaptor) Init(_ *relaycommon.RelayInfo) {}

func (a *completedImagePollingAdaptor) FetchTask(_ string, _ string, _ map[string]any, _ string) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body: io.NopCloser(bytes.NewBufferString(`{
			"id":"upstream_image",
			"status":"completed",
			"progress":"100%",
			"data":[{"url":"https://ig.kcai.asia/generated/async.png"}]
		}`)),
	}, nil
}

func (a *completedImagePollingAdaptor) ParseTaskResult(body []byte) (*relaycommon.TaskInfo, error) {
	var response struct {
		ID       string          `json:"id"`
		Status   string          `json:"status"`
		Progress string          `json:"progress"`
		Data     []dto.ImageData `json:"data"`
	}
	if err := common.Unmarshal(body, &response); err != nil {
		return nil, err
	}
	return &relaycommon.TaskInfo{
		TaskID:   response.ID,
		Status:   string(model.TaskStatusSuccess),
		Progress: response.Progress,
		Url:      response.Data[0].Url,
	}, nil
}

func (a *completedImagePollingAdaptor) AdjustBillingOnComplete(_ *model.Task, _ *relaycommon.TaskInfo) int {
	return 0
}

func (a *localTaskExecutorAdaptor) Init(_ *relaycommon.RelayInfo) {}

func (a *localTaskExecutorAdaptor) FetchTask(_ string, _ string, _ map[string]any, _ string) (*http.Response, error) {
	return nil, nil
}

func (a *localTaskExecutorAdaptor) ParseTaskResult([]byte) (*relaycommon.TaskInfo, error) {
	return nil, nil
}

func (a *localTaskExecutorAdaptor) AdjustBillingOnComplete(_ *model.Task, _ *relaycommon.TaskInfo) int {
	return 0
}

func (a *localTaskExecutorAdaptor) ExecuteLocalTask(_ context.Context, task *model.Task, _ *model.Channel) (*relaycommon.TaskInfo, []byte, error) {
	a.calls++
	return &relaycommon.TaskInfo{
		TaskID:   task.TaskID,
		Status:   string(model.TaskStatusSuccess),
		Progress: "100%",
		Url:      "https://example.com/local.png",
	}, []byte(`{"data":[{"url":"https://example.com/local.png"}]}`), nil
}

func (a *timeoutThenSuccessLocalTaskExecutor) ExecuteLocalTask(ctx context.Context, task *model.Task, _ *model.Channel) (*relaycommon.TaskInfo, []byte, error) {
	a.calls++
	if deadline, ok := ctx.Deadline(); ok {
		a.deadlines = append(a.deadlines, time.Until(deadline))
	}
	if a.calls <= a.timeoutFailures {
		return nil, nil, context.DeadlineExceeded
	}
	return &relaycommon.TaskInfo{
		TaskID:   task.TaskID,
		Status:   string(model.TaskStatusSuccess),
		Progress: "100%",
		Url:      "https://example.com/retried.png",
	}, []byte(`{"data":[{"url":"https://example.com/retried.png"}]}`), nil
}

func (a *blockingLocalTaskExecutor) ExecuteLocalTask(_ context.Context, task *model.Task, _ *model.Channel) (*relaycommon.TaskInfo, []byte, error) {
	a.mu.Lock()
	a.active++
	if a.active > a.maxActive {
		a.maxActive = a.active
	}
	a.mu.Unlock()

	a.started <- task.TaskID
	<-a.release

	a.mu.Lock()
	a.active--
	a.mu.Unlock()
	return &relaycommon.TaskInfo{
		TaskID:   task.TaskID,
		Status:   string(model.TaskStatusSuccess),
		Progress: "100%",
		Url:      "https://example.com/concurrent.png",
	}, []byte(`{"data":[{"url":"https://example.com/concurrent.png"}]}`), nil
}

func (a *blockingLocalTaskExecutor) maxConcurrency() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.maxActive
}

func (a *sunoFailurePollingAdaptor) Init(_ *relaycommon.RelayInfo) {}

func (a *sunoFailurePollingAdaptor) FetchTask(_ string, _ string, body map[string]any, _ string) (*http.Response, error) {
	taskIDs, _ := body["ids"].([]string)
	items := make([]dto.SunoDataResponse, 0, len(taskIDs))
	for _, taskID := range taskIDs {
		items = append(items, dto.SunoDataResponse{
			TaskID:     taskID,
			Status:     string(model.TaskStatusFailure),
			FailReason: a.failReason,
			FinishTime: time.Now().Unix(),
		})
	}

	responseBody, err := common.Marshal(dto.TaskResponse[[]dto.SunoDataResponse]{
		Code: dto.TaskSuccessCode,
		Data: items,
	})
	if err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(responseBody)),
	}, nil
}

func (a *sunoFailurePollingAdaptor) ParseTaskResult([]byte) (*relaycommon.TaskInfo, error) {
	return nil, nil
}

func (a *sunoFailurePollingAdaptor) AdjustBillingOnComplete(_ *model.Task, _ *relaycommon.TaskInfo) int {
	return 0
}

func (a *taskPollingFetchAdaptor) Init(_ *relaycommon.RelayInfo) {}

func (a *taskPollingFetchAdaptor) FetchTask(_ string, _ string, body map[string]any, _ string) (*http.Response, error) {
	taskID, _ := body["task_id"].(string)
	if taskID == a.blockTaskID && a.releaseBlock != nil {
		a.blockOnce.Do(func() {
			if a.blockStarted != nil {
				close(a.blockStarted)
			}
		})
		<-a.releaseBlock
	}

	a.mu.Lock()
	a.taskIDs = append(a.taskIDs, taskID)
	a.mu.Unlock()
	if a.fetched != nil {
		select {
		case a.fetched <- taskID:
		default:
		}
	}

	response := dto.TaskResponse[model.Task]{
		Code: dto.TaskSuccessCode,
		Data: model.Task{
			TaskID:   taskID,
			Status:   model.TaskStatusInProgress,
			Progress: "30%",
		},
	}
	responseBody, err := common.Marshal(response)
	if err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(responseBody)),
	}, nil
}

func (a *taskPollingFetchAdaptor) ParseTaskResult([]byte) (*relaycommon.TaskInfo, error) {
	return &relaycommon.TaskInfo{Status: model.TaskStatusInProgress}, nil
}

func (a *taskPollingFetchAdaptor) AdjustBillingOnComplete(_ *model.Task, _ *relaycommon.TaskInfo) int {
	return 0
}

func (a *taskPollingFetchAdaptor) fetchCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.taskIDs)
}

func (a *taskPollingFetchAdaptor) fetchedTaskIDs() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.taskIDs...)
}

func seedTaskPollingChannel(t *testing.T, id int, disableSleep bool) {
	t.Helper()
	ch := &model.Channel{
		Id:     id,
		Type:   constant.ChannelTypeKling,
		Name:   "polling_channel",
		Key:    "sk-test",
		Status: common.ChannelStatusEnabled,
	}
	if disableSleep {
		ch.SetOtherSettings(dto.ChannelOtherSettings{DisableTaskPollingSleep: true})
	}
	require.NoError(t, model.DB.Create(ch).Error)
}

func seedPollingTask(t *testing.T, channelID int, publicID string, upstreamID string) *model.Task {
	t.Helper()
	task := &model.Task{
		TaskID:    publicID,
		Platform:  constant.TaskPlatform("kling"),
		UserId:    1,
		ChannelId: channelID,
		Action:    constant.TaskActionGenerate,
		Status:    model.TaskStatusInProgress,
		Progress:  "30%",
		CreatedAt: time.Now().Unix(),
		UpdatedAt: time.Now().Unix(),
		PrivateData: model.TaskPrivateData{
			UpstreamTaskID: upstreamID,
		},
	}
	require.NoError(t, model.DB.Create(task).Error)
	return task
}

func TestUpdateLocalTasksExecutesSynchronousImageTask(t *testing.T) {
	truncate(t)
	const channelID = 601
	seedTaskPollingChannel(t, channelID, true)
	task := &model.Task{
		TaskID:    "task_local_sync",
		Platform:  constant.TaskPlatformAsyncImage,
		UserId:    1,
		ChannelId: channelID,
		Action:    constant.TaskActionImageGenerate,
		Status:    model.TaskStatusQueued,
		Progress:  "10%",
		CreatedAt: time.Now().Unix(),
		UpdatedAt: time.Now().Unix(),
		PrivateData: model.TaskPrivateData{
			UpstreamMode:       constant.TaskImageUpstreamModeSync,
			RequestBody:        []byte(`{"model":"sync-image","prompt":"test"}`),
			RequestContentType: "application/json",
		},
	}
	require.NoError(t, model.DB.Create(task).Error)

	executor := &localTaskExecutorAdaptor{}
	previousFactory := GetTaskAdaptorFunc
	GetTaskAdaptorFunc = func(constant.TaskPlatform) TaskPollingAdaptor { return executor }
	t.Cleanup(func() { GetTaskAdaptorFunc = previousFactory })

	require.NoError(t, UpdateLocalTasks(context.Background(), constant.TaskPlatformAsyncImage, []*model.Task{task}))
	require.Eventually(t, func() bool {
		return task.Status == model.TaskStatusSuccess
	}, 2*time.Second, 10*time.Millisecond)
	assert.Equal(t, 1, executor.calls)
	assert.Equal(t, "100%", task.Progress)
	assert.Equal(t, "https://example.com/local.png", task.PrivateData.ResultURL)
	assert.Empty(t, task.PrivateData.RequestBody)
}

func TestLocalImagePollingLoadsOnlyQueuedTasksWithinAvailableSlots(t *testing.T) {
	truncate(t)
	now := time.Now().Unix()
	tasks := []*model.Task{
		{
			TaskID:    "task_local_queued",
			Platform:  constant.TaskPlatformAsyncImage,
			Status:    model.TaskStatusQueued,
			Progress:  "10%",
			CreatedAt: now,
			UpdatedAt: now,
			PrivateData: model.TaskPrivateData{
				UpstreamMode: constant.TaskImageUpstreamModeSync,
				RequestBody:  []byte(`{"prompt":"queued"}`),
			},
		},
		{
			TaskID:    "task_local_in_progress",
			Platform:  constant.TaskPlatformAsyncImage,
			Status:    model.TaskStatusInProgress,
			Progress:  "30%",
			CreatedAt: now,
			UpdatedAt: now,
			PrivateData: model.TaskPrivateData{
				UpstreamMode: constant.TaskImageUpstreamModeSync,
				RequestBody:  []byte(`{"prompt":"running"}`),
			},
		},
		{
			TaskID:    "task_remote_in_progress",
			Platform:  constant.TaskPlatformAsyncImage,
			Status:    model.TaskStatusInProgress,
			Progress:  "30%",
			CreatedAt: now,
			UpdatedAt: now,
			PrivateData: model.TaskPrivateData{
				UpstreamMode:   constant.TaskImageUpstreamModeAsync,
				UpstreamTaskID: "upstream_remote",
			},
		},
	}
	for _, task := range tasks {
		require.NoError(t, model.DB.Create(task).Error)
	}

	localTasks := model.GetQueuedLocalImageTasks(1)
	require.Len(t, localTasks, 1)
	assert.Equal(t, "task_local_queued", localTasks[0].TaskID)
	assert.Equal(t, []byte(`{"prompt":"queued"}`), localTasks[0].PrivateData.RequestBody)

	remoteTasks := model.GetAllUnFinishRemoteTasks(10)
	require.Len(t, remoteTasks, 1)
	assert.Equal(t, "task_remote_in_progress", remoteTasks[0].TaskID)
}

func TestSynchronousImageConcurrencyUsesGlobalConfigurableSlots(t *testing.T) {
	truncate(t)
	const firstChannelID = 605
	const secondChannelID = 606
	seedTaskPollingChannel(t, firstChannelID, true)
	seedTaskPollingChannel(t, secondChannelID, true)
	previousSlots := constant.AsyncImageWorkerSlots
	constant.AsyncImageWorkerSlots = 3
	synchronousImageTaskSlotsOnce = sync.Once{}
	synchronousImageTaskSlotPool = nil
	t.Cleanup(func() {
		constant.AsyncImageWorkerSlots = previousSlots
		synchronousImageTaskSlotsOnce = sync.Once{}
		synchronousImageTaskSlotPool = nil
	})

	newTask := func(id string, channelID int) *model.Task {
		task := &model.Task{
			TaskID:    id,
			Platform:  constant.TaskPlatformAsyncImage,
			UserId:    1,
			ChannelId: channelID,
			Action:    constant.TaskActionImageGenerate,
			Status:    model.TaskStatusQueued,
			Progress:  "10%",
			CreatedAt: time.Now().Unix(),
			UpdatedAt: time.Now().Unix(),
			PrivateData: model.TaskPrivateData{
				UpstreamMode:       constant.TaskImageUpstreamModeSync,
				RequestBody:        []byte(`{"model":"sync-image","prompt":"test"}`),
				RequestContentType: "application/json",
			},
		}
		require.NoError(t, model.DB.Create(task).Error)
		return task
	}

	channelIDs := []int{firstChannelID, secondChannelID, firstChannelID, secondChannelID}
	tasks := make([]*model.Task, 0, len(channelIDs))
	for i, channelID := range channelIDs {
		tasks = append(tasks, newTask(fmt.Sprintf("task_global_slot_%d", i), channelID))
	}
	executor := &blockingLocalTaskExecutor{
		started: make(chan string, len(tasks)),
		release: make(chan struct{}),
	}
	previousFactory := GetTaskAdaptorFunc
	GetTaskAdaptorFunc = func(constant.TaskPlatform) TaskPollingAdaptor { return executor }
	t.Cleanup(func() { GetTaskAdaptorFunc = previousFactory })

	for i, task := range tasks {
		started := TryDispatchLocalTask(task)
		assert.Equal(t, i < 3, started)
	}

	startedIDs := make(map[string]struct{}, 3)
	for i := 0; i < 3; i++ {
		select {
		case taskID := <-executor.started:
			startedIDs[taskID] = struct{}{}
		case <-time.After(2 * time.Second):
			t.Fatal("concurrent image tasks did not start")
		}
	}
	assert.NotContains(t, startedIDs, tasks[3].TaskID)
	assert.Equal(t, 3, executor.maxConcurrency())
	assert.Equal(t, model.TaskStatus(model.TaskStatusQueued), tasks[3].Status)

	close(executor.release)
	require.Eventually(t, func() bool {
		return len(synchronousImageTaskSlots()) == 0
	}, 2*time.Second, 10*time.Millisecond)
	require.True(t, TryDispatchLocalTask(tasks[3]))
	require.Eventually(t, func() bool {
		return tasks[3].Status == model.TaskStatusSuccess
	}, 2*time.Second, 10*time.Millisecond)
}

func TestUpdateLocalTasksTimesOutAndRetriesSynchronousImageOnce(t *testing.T) {
	truncate(t)
	const channelID = 603
	seedTaskPollingChannel(t, channelID, true)
	task := &model.Task{
		TaskID:    "task_local_sync_retry",
		Platform:  constant.TaskPlatformAsyncImage,
		UserId:    1,
		ChannelId: channelID,
		Action:    constant.TaskActionImageGenerate,
		Status:    model.TaskStatusQueued,
		Progress:  "10%",
		CreatedAt: time.Now().Unix(),
		UpdatedAt: time.Now().Unix(),
		PrivateData: model.TaskPrivateData{
			UpstreamMode:       constant.TaskImageUpstreamModeSync,
			RequestBody:        []byte(`{"model":"sync-image","prompt":"test"}`),
			RequestContentType: "application/json",
		},
	}
	require.NoError(t, model.DB.Create(task).Error)

	executor := &timeoutThenSuccessLocalTaskExecutor{timeoutFailures: 1}
	previousFactory := GetTaskAdaptorFunc
	GetTaskAdaptorFunc = func(constant.TaskPlatform) TaskPollingAdaptor { return executor }
	t.Cleanup(func() { GetTaskAdaptorFunc = previousFactory })

	require.NoError(t, UpdateLocalTasks(context.Background(), constant.TaskPlatformAsyncImage, []*model.Task{task}))
	require.Eventually(t, func() bool {
		return task.Status == model.TaskStatusSuccess
	}, 2*time.Second, 10*time.Millisecond)
	assert.Equal(t, 2, executor.calls)
	require.Len(t, executor.deadlines, 2)
	for _, remaining := range executor.deadlines {
		assert.Greater(t, remaining, 299*time.Second)
		assert.LessOrEqual(t, remaining, 300*time.Second)
	}
	assert.Equal(t, model.TaskStatus(model.TaskStatusSuccess), task.Status)
	assert.Equal(t, 2, task.PrivateData.LocalTaskAttempts)
	assert.Equal(t, "https://example.com/retried.png", task.PrivateData.ResultURL)
	assert.Empty(t, task.PrivateData.RequestBody)

	var persisted model.Task
	require.NoError(t, model.DB.First(&persisted, task.ID).Error)
	assert.Equal(t, 2, persisted.PrivateData.LocalTaskAttempts)
	assert.Equal(t, model.TaskStatus(model.TaskStatusSuccess), persisted.Status)
}

func TestUpdateLocalTasksStopsAfterOneSynchronousImageRetry(t *testing.T) {
	truncate(t)
	const channelID = 604
	seedTaskPollingChannel(t, channelID, true)
	task := &model.Task{
		TaskID:    "task_local_sync_retry_exhausted",
		Platform:  constant.TaskPlatformAsyncImage,
		UserId:    1,
		ChannelId: channelID,
		Action:    constant.TaskActionImageGenerate,
		Status:    model.TaskStatusQueued,
		Progress:  "10%",
		CreatedAt: time.Now().Unix(),
		UpdatedAt: time.Now().Unix(),
		PrivateData: model.TaskPrivateData{
			UpstreamMode:       constant.TaskImageUpstreamModeSync,
			RequestBody:        []byte(`{"model":"sync-image","prompt":"test"}`),
			RequestContentType: "application/json",
		},
	}
	require.NoError(t, model.DB.Create(task).Error)

	executor := &timeoutThenSuccessLocalTaskExecutor{timeoutFailures: 2}
	previousFactory := GetTaskAdaptorFunc
	GetTaskAdaptorFunc = func(constant.TaskPlatform) TaskPollingAdaptor { return executor }
	t.Cleanup(func() { GetTaskAdaptorFunc = previousFactory })

	require.NoError(t, UpdateLocalTasks(context.Background(), constant.TaskPlatformAsyncImage, []*model.Task{task}))
	require.Eventually(t, func() bool {
		return task.Status == model.TaskStatusFailure
	}, 2*time.Second, 10*time.Millisecond)
	assert.Equal(t, 2, executor.calls)
	assert.Equal(t, model.TaskStatus(model.TaskStatusFailure), task.Status)
	assert.Equal(t, 2, task.PrivateData.LocalTaskAttempts)
	assert.Equal(t, "synchronous image upstream timed out after 300 seconds", task.FailReason)
	assert.Empty(t, task.PrivateData.RequestBody)

	var persisted model.Task
	require.NoError(t, model.DB.First(&persisted, task.ID).Error)
	assert.Equal(t, 2, persisted.PrivateData.LocalTaskAttempts)
	assert.Equal(t, model.TaskStatus(model.TaskStatusFailure), persisted.Status)
}

func TestUpdateVideoTasksRewritesAsynchronousImageResultURL(t *testing.T) {
	truncate(t)
	const channelID = 602
	baseURL := "https://upstream.example.com"
	channel := &model.Channel{
		Id:      channelID,
		Type:    constant.ChannelTypeOpenAI,
		Name:    "async_image_channel",
		Key:     "sk-test",
		Status:  common.ChannelStatusEnabled,
		BaseURL: &baseURL,
	}
	channel.SetOtherSettings(dto.ChannelOtherSettings{
		DisableTaskPollingSleep: true,
		ImageURLSourcePrefix:    "https://ig.kcai.asia",
		ImageURLTargetPrefix:    "https://mianyunai.com",
	})
	require.NoError(t, model.DB.Create(channel).Error)

	task := &model.Task{
		TaskID:    "task_async_image",
		Platform:  constant.TaskPlatformAsyncImage,
		UserId:    1,
		ChannelId: channelID,
		Action:    constant.TaskActionImageGenerate,
		Status:    model.TaskStatusQueued,
		Progress:  "10%",
		CreatedAt: time.Now().Unix(),
		UpdatedAt: time.Now().Unix(),
		PrivateData: model.TaskPrivateData{
			UpstreamTaskID: "upstream_image",
			UpstreamMode:   constant.TaskImageUpstreamModeAsync,
		},
	}
	require.NoError(t, model.DB.Create(task).Error)

	adaptor := &completedImagePollingAdaptor{}
	previousFactory := GetTaskAdaptorFunc
	GetTaskAdaptorFunc = func(constant.TaskPlatform) TaskPollingAdaptor { return adaptor }
	t.Cleanup(func() { GetTaskAdaptorFunc = previousFactory })

	require.NoError(t, UpdateVideoTasks(context.Background(), constant.TaskPlatformAsyncImage, map[int][]string{
		channelID: {task.GetUpstreamTaskID()},
	}, map[string]*model.Task{
		task.GetUpstreamTaskID(): task,
	}))

	assert.Equal(t, model.TaskStatus(model.TaskStatusSuccess), task.Status)
	assert.Equal(t, "https://mianyunai.com/generated/async.png", task.PrivateData.ResultURL)
	assert.Contains(t, string(task.Data), "https://mianyunai.com/generated/async.png")
	assert.NotContains(t, string(task.Data), "ig.kcai.asia")
}

func TestUpdateVideoTasksDefaultSleepWaitsBetweenTasks(t *testing.T) {
	truncate(t)

	const channelID = 101
	seedTaskPollingChannel(t, channelID, false)
	first := seedPollingTask(t, channelID, "task_public_1", "upstream_1")
	second := seedPollingTask(t, channelID, "task_public_2", "upstream_2")

	adaptor := &taskPollingFetchAdaptor{}
	previousFactory := GetTaskAdaptorFunc
	GetTaskAdaptorFunc = func(constant.TaskPlatform) TaskPollingAdaptor { return adaptor }
	t.Cleanup(func() { GetTaskAdaptorFunc = previousFactory })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := UpdateVideoTasks(ctx, constant.TaskPlatform("kling"), map[int][]string{
		channelID: {
			first.GetUpstreamTaskID(),
			second.GetUpstreamTaskID(),
		},
	}, map[string]*model.Task{
		first.GetUpstreamTaskID():  first,
		second.GetUpstreamTaskID(): second,
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 1, adaptor.fetchCount())
}

func TestUpdateVideoTasksCanSkipPollingSleepPerChannel(t *testing.T) {
	truncate(t)

	const channelID = 102
	seedTaskPollingChannel(t, channelID, true)
	first := seedPollingTask(t, channelID, "task_public_3", "upstream_3")
	second := seedPollingTask(t, channelID, "task_public_4", "upstream_4")

	adaptor := &taskPollingFetchAdaptor{}
	previousFactory := GetTaskAdaptorFunc
	GetTaskAdaptorFunc = func(constant.TaskPlatform) TaskPollingAdaptor { return adaptor }
	t.Cleanup(func() { GetTaskAdaptorFunc = previousFactory })

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := UpdateVideoTasks(ctx, constant.TaskPlatform("kling"), map[int][]string{
		channelID: {
			first.GetUpstreamTaskID(),
			second.GetUpstreamTaskID(),
		},
	}, map[string]*model.Task{
		first.GetUpstreamTaskID():  first,
		second.GetUpstreamTaskID(): second,
	})

	require.NoError(t, err)
	assert.Equal(t, 2, adaptor.fetchCount())
}

func TestUpdateVideoTasksDefaultSleepDoesNotBlockOtherChannels(t *testing.T) {
	truncate(t)

	const firstChannelID = 201
	const secondChannelID = 202
	seedTaskPollingChannel(t, firstChannelID, false)
	seedTaskPollingChannel(t, secondChannelID, false)
	firstChannelFirst := seedPollingTask(t, firstChannelID, "task_public_5", "upstream_a_1")
	firstChannelSecond := seedPollingTask(t, firstChannelID, "task_public_6", "upstream_a_2")
	secondChannelFirst := seedPollingTask(t, secondChannelID, "task_public_7", "upstream_b_1")
	secondChannelSecond := seedPollingTask(t, secondChannelID, "task_public_8", "upstream_b_2")

	adaptor := &taskPollingFetchAdaptor{}
	previousFactory := GetTaskAdaptorFunc
	GetTaskAdaptorFunc = func(constant.TaskPlatform) TaskPollingAdaptor { return adaptor }
	t.Cleanup(func() { GetTaskAdaptorFunc = previousFactory })

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := UpdateVideoTasks(ctx, constant.TaskPlatform("kling"), map[int][]string{
		firstChannelID: {
			firstChannelFirst.GetUpstreamTaskID(),
			firstChannelSecond.GetUpstreamTaskID(),
		},
		secondChannelID: {
			secondChannelFirst.GetUpstreamTaskID(),
			secondChannelSecond.GetUpstreamTaskID(),
		},
	}, map[string]*model.Task{
		firstChannelFirst.GetUpstreamTaskID():   firstChannelFirst,
		firstChannelSecond.GetUpstreamTaskID():  firstChannelSecond,
		secondChannelFirst.GetUpstreamTaskID():  secondChannelFirst,
		secondChannelSecond.GetUpstreamTaskID(): secondChannelSecond,
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.ElementsMatch(t, []string{"upstream_a_1", "upstream_b_1"}, adaptor.fetchedTaskIDs())
}

func TestUpdateVideoTasksSlowChannelDoesNotBlockOtherChannels(t *testing.T) {
	truncate(t)

	const slowChannelID = 251
	const fastChannelID = 252
	seedTaskPollingChannel(t, slowChannelID, false)
	seedTaskPollingChannel(t, fastChannelID, true)
	slowTask := seedPollingTask(t, slowChannelID, "task_public_slow", "upstream_slow_1")
	fastFirst := seedPollingTask(t, fastChannelID, "task_public_fast_1", "upstream_fast_parallel_1")
	fastSecond := seedPollingTask(t, fastChannelID, "task_public_fast_2", "upstream_fast_parallel_2")

	adaptor := &taskPollingFetchAdaptor{
		fetched:      make(chan string, 4),
		blockTaskID:  slowTask.GetUpstreamTaskID(),
		blockStarted: make(chan struct{}),
		releaseBlock: make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseBlockedTask := func() {
		releaseOnce.Do(func() {
			close(adaptor.releaseBlock)
		})
	}
	t.Cleanup(releaseBlockedTask)
	previousFactory := GetTaskAdaptorFunc
	GetTaskAdaptorFunc = func(constant.TaskPlatform) TaskPollingAdaptor { return adaptor }
	t.Cleanup(func() { GetTaskAdaptorFunc = previousFactory })

	errCh := make(chan error, 1)
	gopool.Go(func() {
		errCh <- UpdateVideoTasks(context.Background(), constant.TaskPlatform("kling"), map[int][]string{
			slowChannelID: {
				slowTask.GetUpstreamTaskID(),
			},
			fastChannelID: {
				fastFirst.GetUpstreamTaskID(),
				fastSecond.GetUpstreamTaskID(),
			},
		}, map[string]*model.Task{
			slowTask.GetUpstreamTaskID():   slowTask,
			fastFirst.GetUpstreamTaskID():  fastFirst,
			fastSecond.GetUpstreamTaskID(): fastSecond,
		})
	})

	select {
	case <-adaptor.blockStarted:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("slow channel did not start blocking")
	}

	require.Eventually(t, func() bool {
		fetchedTaskIDs := adaptor.fetchedTaskIDs()
		return len(fetchedTaskIDs) == 2 &&
			fetchedTaskIDs[0] == fastFirst.GetUpstreamTaskID() &&
			fetchedTaskIDs[1] == fastSecond.GetUpstreamTaskID()
	}, 500*time.Millisecond, 10*time.Millisecond)

	releaseBlockedTask()
	require.NoError(t, <-errCh)
	assert.ElementsMatch(t, []string{
		slowTask.GetUpstreamTaskID(),
		fastFirst.GetUpstreamTaskID(),
		fastSecond.GetUpstreamTaskID(),
	}, adaptor.fetchedTaskIDs())
}

func TestUpdateVideoTasksMixedChannelSleepSettings(t *testing.T) {
	truncate(t)

	const sleepyChannelID = 301
	const fastChannelID = 302
	seedTaskPollingChannel(t, sleepyChannelID, false)
	seedTaskPollingChannel(t, fastChannelID, true)
	sleepyFirst := seedPollingTask(t, sleepyChannelID, "task_public_9", "upstream_sleepy_1")
	sleepySecond := seedPollingTask(t, sleepyChannelID, "task_public_10", "upstream_sleepy_2")
	fastFirst := seedPollingTask(t, fastChannelID, "task_public_11", "upstream_fast_1")
	fastSecond := seedPollingTask(t, fastChannelID, "task_public_12", "upstream_fast_2")

	adaptor := &taskPollingFetchAdaptor{}
	previousFactory := GetTaskAdaptorFunc
	GetTaskAdaptorFunc = func(constant.TaskPlatform) TaskPollingAdaptor { return adaptor }
	t.Cleanup(func() { GetTaskAdaptorFunc = previousFactory })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := UpdateVideoTasks(ctx, constant.TaskPlatform("kling"), map[int][]string{
		sleepyChannelID: {
			sleepyFirst.GetUpstreamTaskID(),
			sleepySecond.GetUpstreamTaskID(),
		},
		fastChannelID: {
			fastFirst.GetUpstreamTaskID(),
			fastSecond.GetUpstreamTaskID(),
		},
	}, map[string]*model.Task{
		sleepyFirst.GetUpstreamTaskID():  sleepyFirst,
		sleepySecond.GetUpstreamTaskID(): sleepySecond,
		fastFirst.GetUpstreamTaskID():    fastFirst,
		fastSecond.GetUpstreamTaskID():   fastSecond,
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.ElementsMatch(t, []string{"upstream_sleepy_1", "upstream_fast_1", "upstream_fast_2"}, adaptor.fetchedTaskIDs())
}

func TestUpdateSunoTasksStalePollsRefundExactlyOnce(t *testing.T) {
	truncate(t)

	const userID, tokenID, channelID = 401, 401, 401
	const initialUserQuota, initialTokenQuota, taskQuota = 10_000, 6_000, 2_500
	const publicTaskID, upstreamTaskID = "suno_public_refund_once", "suno_upstream_refund_once"

	seedUser(t, userID, initialUserQuota)
	seedToken(t, tokenID, userID, "sk-suno-refund-once", initialTokenQuota)
	baseURL := "https://suno.invalid"
	require.NoError(t, model.DB.Create(&model.Channel{
		Id:      channelID,
		Type:    constant.ChannelTypeSunoAPI,
		Name:    "suno_refund_once",
		Key:     "sk-suno-channel",
		Status:  common.ChannelStatusEnabled,
		BaseURL: &baseURL,
	}).Error)

	task := makeTask(userID, channelID, taskQuota, tokenID, BillingSourceWallet, 0)
	task.TaskID = publicTaskID
	task.Platform = constant.TaskPlatformSuno
	task.Status = model.TaskStatusInProgress
	task.Progress = "50%"
	task.SubmitTime = time.Now().Unix()
	task.PrivateData.UpstreamTaskID = upstreamTaskID
	require.NoError(t, model.DB.Create(task).Error)

	var firstPollTask model.Task
	var staleSecondPollTask model.Task
	require.NoError(t, model.DB.First(&firstPollTask, task.ID).Error)
	require.NoError(t, model.DB.First(&staleSecondPollTask, task.ID).Error)

	adaptor := &sunoFailurePollingAdaptor{failReason: "upstream failed"}
	previousFactory := GetTaskAdaptorFunc
	GetTaskAdaptorFunc = func(constant.TaskPlatform) TaskPollingAdaptor { return adaptor }
	t.Cleanup(func() { GetTaskAdaptorFunc = previousFactory })

	require.NoError(t, updateSunoTasks(context.Background(), channelID, []string{upstreamTaskID}, map[string]*model.Task{
		upstreamTaskID: &firstPollTask,
	}))
	require.NoError(t, updateSunoTasks(context.Background(), channelID, []string{upstreamTaskID}, map[string]*model.Task{
		upstreamTaskID: &staleSecondPollTask,
	}))

	var reloaded model.Task
	require.NoError(t, model.DB.First(&reloaded, task.ID).Error)
	assert.EqualValues(t, model.TaskStatusFailure, reloaded.Status)
	assert.Zero(t, reloaded.Quota)
	assert.Equal(t, initialUserQuota+taskQuota, getUserQuota(t, userID))
	assert.Equal(t, initialTokenQuota+taskQuota, getTokenRemainQuota(t, tokenID))
	assert.Equal(t, int64(1), countLogs(t))
}

func TestRunTaskPollingOnceDoesNotRefundHistoricalFailedTask(t *testing.T) {
	truncate(t)

	const userID, initialQuota, taskQuota = 402, 10_000, 1_200
	seedUser(t, userID, initialQuota)

	task := makeTask(userID, 0, taskQuota, 0, BillingSourceWallet, 0)
	task.TaskID = "historical_failed_already_refunded"
	task.Status = model.TaskStatusFailure
	task.Progress = "100%"
	task.SubmitTime = time.Now().Add(-90 * 24 * time.Hour).Unix()
	task.UpdatedAt = time.Now().Add(-time.Minute).Unix()
	require.NoError(t, model.DB.Create(task).Error)

	previousFactory := GetTaskAdaptorFunc
	GetTaskAdaptorFunc = func(constant.TaskPlatform) TaskPollingAdaptor {
		return &taskPollingFetchAdaptor{}
	}
	t.Cleanup(func() { GetTaskAdaptorFunc = previousFactory })

	summary := RunTaskPollingOnce(context.Background(), nil)

	assert.Zero(t, summary.UnfinishedTasks)
	assert.Equal(t, initialQuota, getUserQuota(t, userID))
	assert.Equal(t, taskQuota, getTaskQuota(t, task.ID))
	assert.Equal(t, int64(0), countLogs(t))
}

func TestInterruptedSynchronousImageTaskFailsAndRefundsWithoutRetry(t *testing.T) {
	truncate(t)

	const userID, initialQuota, taskQuota = 405, 10_000, 1_500
	seedUser(t, userID, initialQuota)
	task := makeTask(userID, 0, taskQuota, 0, BillingSourceWallet, 0)
	task.TaskID = "interrupted_synchronous_image"
	task.Platform = constant.TaskPlatformAsyncImage
	task.Status = model.TaskStatusInProgress
	task.Progress = "50%"
	task.SubmitTime = time.Now().Add(-2 * time.Minute).Unix()
	task.PrivateData.UpstreamMode = constant.TaskImageUpstreamModeSync
	task.PrivateData.RequestBody = []byte("persisted multipart request")
	task.PrivateData.RequestContentType = "multipart/form-data"
	require.NoError(t, model.DB.Create(task).Error)
	require.NoError(t, model.DB.Model(&model.Task{}).
		Where("id = ?", task.ID).
		Update("updated_at", time.Now().Add(-2*time.Minute).Unix()).Error)

	failed := failInterruptedSynchronousImageTasks(context.Background())
	assert.Equal(t, 1, failed)

	var reloaded model.Task
	require.NoError(t, model.DB.First(&reloaded, task.ID).Error)
	assert.Equal(t, model.TaskStatus(model.TaskStatusFailure), reloaded.Status)
	assert.Equal(t, "100%", reloaded.Progress)
	assert.Contains(t, reloaded.FailReason, "结果无法确认")
	assert.Empty(t, reloaded.PrivateData.RequestBody)
	assert.Empty(t, reloaded.PrivateData.RequestContentType)
	assert.Zero(t, reloaded.Quota)
	assert.Equal(t, initialQuota+taskQuota, getUserQuota(t, userID))
	assert.Equal(t, int64(1), countLogs(t))

	assert.Zero(t, failInterruptedSynchronousImageTasks(context.Background()))
	assert.Equal(t, initialQuota+taskQuota, getUserQuota(t, userID))
	assert.Equal(t, int64(1), countLogs(t))
}

func TestSweepTimedOutTasksHonorsRefundRolloutBoundary(t *testing.T) {
	truncate(t)

	const (
		userID          = 403
		initialQuota    = 10_000
		legacyTaskQuota = 1_800
		modernTaskQuota = 1_200
	)
	seedUser(t, userID, initialQuota)

	legacyTask := makeTask(userID, 0, legacyTaskQuota, 0, BillingSourceWallet, 0)
	legacyTask.TaskID = "legacy_timeout_without_refund"
	legacyTask.Progress = "50%"
	legacyTask.SubmitTime = 1771718399 // 2026-02-21 23:59:59 UTC
	legacyTask.PrivateData.RequestBody = []byte("legacy image request")
	legacyTask.PrivateData.RequestContentType = "multipart/form-data"
	require.NoError(t, model.DB.Create(legacyTask).Error)

	modernTask := makeTask(userID, 0, modernTaskQuota, 0, BillingSourceWallet, 0)
	modernTask.TaskID = "modern_timeout_with_refund"
	modernTask.Progress = "50%"
	modernTask.SubmitTime = 1771718400 // 2026-02-22 00:00:00 UTC
	modernTask.PrivateData.RequestBody = []byte("modern image request")
	modernTask.PrivateData.RequestContentType = "multipart/form-data"
	require.NoError(t, model.DB.Create(modernTask).Error)

	previousTimeout := constant.TaskTimeoutMinutes
	constant.TaskTimeoutMinutes = 1
	t.Cleanup(func() { constant.TaskTimeoutMinutes = previousTimeout })

	sweepTimedOutTasks(context.Background())

	var reloadedLegacy model.Task
	var reloadedModern model.Task
	require.NoError(t, model.DB.First(&reloadedLegacy, legacyTask.ID).Error)
	require.NoError(t, model.DB.First(&reloadedModern, modernTask.ID).Error)
	assert.EqualValues(t, model.TaskStatusFailure, reloadedLegacy.Status)
	assert.EqualValues(t, model.TaskStatusFailure, reloadedModern.Status)
	assert.Zero(t, reloadedLegacy.Quota)
	assert.Zero(t, reloadedModern.Quota)
	assert.Contains(t, reloadedLegacy.FailReason, "旧系统遗留任务")
	assert.Contains(t, reloadedModern.FailReason, "任务超时")
	assert.Empty(t, reloadedLegacy.PrivateData.RequestBody)
	assert.Empty(t, reloadedModern.PrivateData.RequestBody)
	assert.Empty(t, reloadedLegacy.PrivateData.RequestContentType)
	assert.Empty(t, reloadedModern.PrivateData.RequestContentType)
	assert.Equal(t, initialQuota+modernTaskQuota, getUserQuota(t, userID))
	assert.Equal(t, int64(1), countLogs(t))
}
