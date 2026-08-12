package model

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/QuantumNous/new-api/constant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClearExpiredTerminalTaskDataBatch(t *testing.T) {
	truncateTables(t)
	const cutoff = int64(10_000)

	tasks := []*Task{
		{TaskID: "expired-success", UserId: 1, Status: TaskStatusSuccess, SubmitTime: 8_000, FinishTime: 9_000, Data: json.RawMessage(`{"result":"large"}`), FailReason: "kept metadata"},
		{TaskID: "expired-failure", UserId: 1, Status: TaskStatusFailure, SubmitTime: 8_000, FinishTime: 9_500, Data: json.RawMessage(`{"error":"large"}`)},
		{TaskID: "legacy-terminal", UserId: 1, Status: TaskStatusSuccess, SubmitTime: 9_000, FinishTime: 0, Data: json.RawMessage(`{"legacy":true}`)},
		{TaskID: "recent-success", UserId: 1, Status: TaskStatusSuccess, SubmitTime: 9_500, FinishTime: 10_001, Data: json.RawMessage(`{"keep":true}`)},
		{TaskID: "queued", UserId: 1, Status: TaskStatusQueued, SubmitTime: 1_000, Data: json.RawMessage(`{"keep":true}`)},
		{TaskID: "in-progress", UserId: 1, Status: TaskStatusInProgress, SubmitTime: 1_000, Data: json.RawMessage(`{"keep":true}`)},
	}
	for _, task := range tasks {
		insertTask(t, task)
	}

	cleared, err := ClearExpiredTerminalTaskDataBatch(context.Background(), cutoff, 2)
	require.NoError(t, err)
	assert.Equal(t, int64(2), cleared)
	cleared, err = ClearExpiredTerminalTaskDataBatch(context.Background(), cutoff, 2)
	require.NoError(t, err)
	assert.Equal(t, int64(1), cleared)

	var stored []*Task
	require.NoError(t, DB.Order("id").Find(&stored).Error)
	require.Len(t, stored, len(tasks))
	assert.Empty(t, stored[0].Data)
	assert.Equal(t, "kept metadata", stored[0].FailReason)
	assert.Empty(t, stored[1].Data)
	assert.Empty(t, stored[2].Data)
	assert.NotEmpty(t, stored[3].Data)
	assert.NotEmpty(t, stored[4].Data)
	assert.NotEmpty(t, stored[5].Data)
}

func TestTaskLogListsOmitPayloadColumns(t *testing.T) {
	truncateTables(t)
	task := &Task{
		TaskID:     "task-log-list",
		UserId:     42,
		ChannelId:  7,
		Platform:   constant.TaskPlatformAsyncImage,
		Status:     TaskStatusSuccess,
		SubmitTime: 1,
		FinishTime: 2,
		FailReason: "metadata remains",
		Data:       json.RawMessage(`{"b64_json":"large"}`),
		PrivateData: TaskPrivateData{
			Key: "secret",
		},
	}
	insertTask(t, task)

	userTasks := TaskGetAllUserTask(task.UserId, 0, 10, SyncTaskQueryParams{})
	require.Len(t, userTasks, 1)
	assert.Empty(t, userTasks[0].Data)
	assert.Equal(t, TaskPrivateData{}, userTasks[0].PrivateData)
	assert.Equal(t, "metadata remains", userTasks[0].FailReason)

	adminTasks := TaskGetAllTasks(0, 10, SyncTaskQueryParams{})
	require.Len(t, adminTasks, 1)
	assert.Empty(t, adminTasks[0].Data)
	assert.Equal(t, TaskPrivateData{}, adminTasks[0].PrivateData)
	assert.Equal(t, 7, adminTasks[0].ChannelId)
}
