package model

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	commonRelay "github.com/QuantumNous/new-api/relay/common"
	"gorm.io/gorm"
)

type TaskStatus string

func (t TaskStatus) ToVideoStatus() string {
	var status string
	switch t {
	case TaskStatusQueued, TaskStatusSubmitted:
		status = dto.VideoStatusQueued
	case TaskStatusInProgress:
		status = dto.VideoStatusInProgress
	case TaskStatusSuccess:
		status = dto.VideoStatusCompleted
	case TaskStatusFailure:
		status = dto.VideoStatusFailed
	default:
		status = dto.VideoStatusUnknown // Default fallback
	}
	return status
}

const (
	TaskStatusNotStart   TaskStatus = "NOT_START"
	TaskStatusSubmitted             = "SUBMITTED"
	TaskStatusQueued                = "QUEUED"
	TaskStatusInProgress            = "IN_PROGRESS"
	TaskStatusFailure               = "FAILURE"
	TaskStatusSuccess               = "SUCCESS"
	TaskStatusUnknown               = "UNKNOWN"
)

// TaskRefundLegacyCutoff separates tasks created before timeout refunds were
// introduced. Those legacy tasks are failed without an automatic refund.
const TaskRefundLegacyCutoff int64 = 1771718400 // 2026-02-22 00:00:00 UTC

type Task struct {
	ID         int64                 `json:"id" gorm:"primary_key;AUTO_INCREMENT"`
	CreatedAt  int64                 `json:"created_at" gorm:"index"`
	UpdatedAt  int64                 `json:"updated_at"`
	TaskID     string                `json:"task_id" gorm:"type:varchar(191);index"` // 第三方id，不一定有/ song id\ Task id
	Platform   constant.TaskPlatform `json:"platform" gorm:"type:varchar(30);index"` // 平台
	UserId     int                   `json:"user_id" gorm:"index"`
	Group      string                `json:"group" gorm:"type:varchar(50)"` // 修正计费用
	ChannelId  int                   `json:"channel_id" gorm:"index"`
	Quota      int                   `json:"quota"`
	Action     string                `json:"action" gorm:"type:varchar(40);index"` // 任务类型, song, lyrics, description-mode
	Status     TaskStatus            `json:"status" gorm:"type:varchar(20);index"` // 任务状态
	FailReason string                `json:"fail_reason"`
	SubmitTime int64                 `json:"submit_time" gorm:"index"`
	StartTime  int64                 `json:"start_time" gorm:"index"`
	FinishTime int64                 `json:"finish_time" gorm:"index"`
	Progress   string                `json:"progress" gorm:"type:varchar(20);index"`
	Properties Properties            `json:"properties" gorm:"type:json"`
	Username   string                `json:"username,omitempty" gorm:"-"`
	// 禁止返回给用户，内部可能包含key等隐私信息
	PrivateData TaskPrivateData `json:"-" gorm:"column:private_data;type:json"`
	Data        json.RawMessage `json:"data" gorm:"type:json"`
}

func (t *Task) SetData(data any) {
	b, _ := common.Marshal(data)
	t.Data = json.RawMessage(b)
}

func (t *Task) GetData(v any) error {
	return common.Unmarshal(t.Data, &v)
}

type Properties struct {
	Input             string `json:"input"`
	UpstreamModelName string `json:"upstream_model_name,omitempty"`
	OriginModelName   string `json:"origin_model_name,omitempty"`
}

func (m *Properties) Scan(val interface{}) error {
	bytesValue, _ := val.([]byte)
	if len(bytesValue) == 0 {
		*m = Properties{}
		return nil
	}
	return common.Unmarshal(bytesValue, m)
}

func (m Properties) Value() (driver.Value, error) {
	if m == (Properties{}) {
		return nil, nil
	}
	return common.Marshal(m)
}

type TaskPrivateData struct {
	Key                string `json:"key,omitempty"`
	UpstreamTaskID     string `json:"upstream_task_id,omitempty"`     // 上游真实 task ID
	UpstreamMode       string `json:"upstream_mode,omitempty"`        // "async" uses provider task polling; "sync" runs a local worker
	RequestPayloadFile string `json:"request_payload_file,omitempty"` // local-sync request file under the async image queue directory
	RequestBody        []byte `json:"request_body,omitempty"`         // legacy local-sync request body; new tasks use RequestPayloadFile
	RequestContentType string `json:"request_content_type,omitempty"`
	LocalTaskAttempts  int    `json:"local_task_attempts,omitempty"`
	ResultURL          string `json:"result_url,omitempty"` // 任务成功后的结果 URL（视频地址等）
	// 计费上下文：用于异步退款/差额结算（轮询阶段读取）
	BillingSource  string              `json:"billing_source,omitempty"`  // "wallet" 或 "subscription"
	SubscriptionId int                 `json:"subscription_id,omitempty"` // 订阅 ID，用于订阅退款
	TokenId        int                 `json:"token_id,omitempty"`        // 令牌 ID，用于令牌额度退款
	NodeName       string              `json:"node_name,omitempty"`       // 发起任务的节点名，轮询结算阶段据此归属日志而非最后查询节点
	BillingContext *TaskBillingContext `json:"billing_context,omitempty"` // 计费参数快照（用于轮询阶段重新计算）
}

// TaskBillingContext 记录任务提交时的计费参数，以便轮询阶段可以重新计算额度。
type TaskBillingContext struct {
	ModelPrice      float64            `json:"model_price,omitempty"`       // 模型单价
	GroupRatio      float64            `json:"group_ratio,omitempty"`       // 分组倍率
	ModelRatio      float64            `json:"model_ratio,omitempty"`       // 模型倍率
	OtherRatios     map[string]float64 `json:"other_ratios,omitempty"`      // 附加倍率（时长、分辨率等）
	OriginModelName string             `json:"origin_model_name,omitempty"` // 模型名称，必须为OriginModelName
	PerCallBilling  bool               `json:"per_call_billing,omitempty"`  // 按次计费：跳过轮询阶段的差额结算
}

// GetUpstreamTaskID 获取上游真实 task ID（用于与 provider 通信）
// 旧数据没有 UpstreamTaskID 时，TaskID 本身就是上游 ID
func (t *Task) GetUpstreamTaskID() string {
	if t.PrivateData.UpstreamMode == constant.TaskImageUpstreamModeSync {
		return ""
	}
	if t.PrivateData.UpstreamTaskID != "" {
		return t.PrivateData.UpstreamTaskID
	}
	return t.TaskID
}

// GetResultURL 获取任务结果 URL（视频地址等）
// 新数据存在 PrivateData.ResultURL 中；旧数据回退到 FailReason（历史兼容）
func (t *Task) GetResultURL() string {
	if t.PrivateData.ResultURL != "" {
		return t.PrivateData.ResultURL
	}
	return t.FailReason
}

// GenerateTaskID 生成对外暴露的 task_xxxx 格式 ID
func GenerateTaskID() string {
	key, _ := common.GenerateRandomCharsKey(32)
	return "task_" + key
}

func (p *TaskPrivateData) Scan(val interface{}) error {
	bytesValue, _ := val.([]byte)
	if len(bytesValue) == 0 {
		return nil
	}
	return common.Unmarshal(bytesValue, p)
}

func (p TaskPrivateData) Value() (driver.Value, error) {
	if p.Key == "" && p.UpstreamTaskID == "" && p.UpstreamMode == "" &&
		p.RequestPayloadFile == "" && len(p.RequestBody) == 0 && p.RequestContentType == "" && p.LocalTaskAttempts == 0 &&
		p.ResultURL == "" && p.BillingSource == "" && p.SubscriptionId == 0 &&
		p.TokenId == 0 && p.NodeName == "" && p.BillingContext == nil {
		return nil, nil
	}
	return common.Marshal(p)
}

// SyncTaskQueryParams 用于包含所有搜索条件的结构体，可以根据需求添加更多字段
type SyncTaskQueryParams struct {
	Platform       constant.TaskPlatform
	ChannelID      string
	TaskID         string
	UserID         string
	Action         string
	Status         string
	StartTimestamp int64
	EndTimestamp   int64
	UserIDs        []int
}

func InitTask(platform constant.TaskPlatform, relayInfo *commonRelay.RelayInfo) *Task {
	properties := Properties{}
	privateData := TaskPrivateData{}
	if relayInfo != nil && relayInfo.ChannelMeta != nil {
		if relayInfo.ChannelMeta.ChannelType == constant.ChannelTypeGemini ||
			relayInfo.ChannelMeta.ChannelType == constant.ChannelTypeVertexAi {
			privateData.Key = relayInfo.ChannelMeta.ApiKey
		}
		if relayInfo.UpstreamModelName != "" {
			properties.UpstreamModelName = relayInfo.UpstreamModelName
		}
		if relayInfo.OriginModelName != "" {
			properties.OriginModelName = relayInfo.OriginModelName
		}
	}

	// 使用预生成的公开 ID（如果有），否则新生成
	taskID := ""
	if relayInfo.TaskRelayInfo != nil && relayInfo.TaskRelayInfo.PublicTaskID != "" {
		taskID = relayInfo.TaskRelayInfo.PublicTaskID
	} else {
		taskID = GenerateTaskID()
	}

	t := &Task{
		TaskID:      taskID,
		UserId:      relayInfo.UserId,
		Group:       relayInfo.UsingGroup,
		SubmitTime:  time.Now().Unix(),
		Status:      TaskStatusNotStart,
		Progress:    "0%",
		ChannelId:   relayInfo.ChannelId,
		Platform:    platform,
		Properties:  properties,
		PrivateData: privateData,
	}
	return t
}

func TaskGetAllUserTask(userId int, startIdx int, num int, queryParams SyncTaskQueryParams) []*Task {
	var tasks []*Task
	var err error

	// 初始化查询构建器
	query := DB.Where("user_id = ?", userId)

	if queryParams.TaskID != "" {
		query = query.Where("task_id = ?", queryParams.TaskID)
	}
	if queryParams.Action != "" {
		query = query.Where("action = ?", queryParams.Action)
	}
	if queryParams.Status != "" {
		query = query.Where("status = ?", queryParams.Status)
	}
	if queryParams.Platform != "" {
		query = query.Where("platform = ?", queryParams.Platform)
	}
	if queryParams.StartTimestamp != 0 {
		// 假设您已将前端传来的时间戳转换为数据库所需的时间格式，并处理了时间戳的验证和解析
		query = query.Where("submit_time >= ?", queryParams.StartTimestamp)
	}
	if queryParams.EndTimestamp != 0 {
		query = query.Where("submit_time <= ?", queryParams.EndTimestamp)
	}

	// 获取数据
	err = query.Omit("channel_id", "data", "private_data").Order("id desc").Limit(num).Offset(startIdx).Find(&tasks).Error
	if err != nil {
		return nil
	}

	return tasks
}

func TaskGetAllTasks(startIdx int, num int, queryParams SyncTaskQueryParams) []*Task {
	var tasks []*Task
	var err error

	// 初始化查询构建器
	query := DB

	// 添加过滤条件
	if queryParams.ChannelID != "" {
		query = query.Where("channel_id = ?", queryParams.ChannelID)
	}
	if queryParams.Platform != "" {
		query = query.Where("platform = ?", queryParams.Platform)
	}
	if queryParams.UserID != "" {
		query = query.Where("user_id = ?", queryParams.UserID)
	}
	if len(queryParams.UserIDs) != 0 {
		query = query.Where("user_id in (?)", queryParams.UserIDs)
	}
	if queryParams.TaskID != "" {
		query = query.Where("task_id = ?", queryParams.TaskID)
	}
	if queryParams.Action != "" {
		query = query.Where("action = ?", queryParams.Action)
	}
	if queryParams.Status != "" {
		query = query.Where("status = ?", queryParams.Status)
	}
	if queryParams.StartTimestamp != 0 {
		query = query.Where("submit_time >= ?", queryParams.StartTimestamp)
	}
	if queryParams.EndTimestamp != 0 {
		query = query.Where("submit_time <= ?", queryParams.EndTimestamp)
	}

	// 获取数据
	err = query.Omit("data", "private_data").Order("id desc").Limit(num).Offset(startIdx).Find(&tasks).Error
	if err != nil {
		return nil
	}

	return tasks
}

// ClearExpiredTerminalTaskDataBatch removes only expired result payloads. It
// first selects IDs so PostgreSQL does not load the data TOAST values and so a
// large initial cleanup can be split into short transactions.
func ClearExpiredTerminalTaskDataBatch(ctx context.Context, cutoffUnix int64, limit int) (int64, error) {
	if limit <= 0 {
		return 0, nil
	}

	var ids []int64
	err := DB.WithContext(ctx).
		Model(&Task{}).
		Select("id").
		Where("status IN ?", []TaskStatus{TaskStatusSuccess, TaskStatusFailure}).
		Where("data IS NOT NULL").
		Where("(finish_time > 0 AND finish_time <= ?) OR (COALESCE(finish_time, 0) = 0 AND submit_time <= ?)", cutoffUnix, cutoffUnix).
		Order("id").
		Limit(limit).
		Pluck("id", &ids).Error
	if err != nil || len(ids) == 0 {
		return 0, err
	}

	result := DB.WithContext(ctx).
		Model(&Task{}).
		Where("id IN ?", ids).
		UpdateColumn("data", nil)
	return result.RowsAffected, result.Error
}

func GetTimedOutUnfinishedTasks(cutoffUnix int64, limit int) []*Task {
	var tasks []*Task
	err := DB.Where("progress != ?", "100%").
		Where("status NOT IN ?", []string{TaskStatusFailure, TaskStatusSuccess}).
		Where("submit_time < ?", cutoffUnix).
		Order("submit_time").
		Limit(limit).
		Find(&tasks).Error
	if err != nil {
		return nil
	}
	return tasks
}

func taskPrivateDataUpstreamModeExpression() string {
	switch {
	case common.UsingMainDatabase(common.DatabaseTypePostgreSQL):
		return "private_data->>'upstream_mode'"
	case common.UsingMainDatabase(common.DatabaseTypeMySQL):
		return "JSON_UNQUOTE(JSON_EXTRACT(private_data, '$.upstream_mode'))"
	default:
		return "json_extract(private_data, '$.upstream_mode')"
	}
}

func taskPrivateDataPayloadFileExpression() string {
	switch {
	case common.UsingMainDatabase(common.DatabaseTypePostgreSQL):
		return "private_data->>'request_payload_file'"
	case common.UsingMainDatabase(common.DatabaseTypeMySQL):
		return "JSON_UNQUOTE(JSON_EXTRACT(private_data, '$.request_payload_file'))"
	default:
		return "json_extract(private_data, '$.request_payload_file')"
	}
}

// GetAllUnFinishRemoteTasks excludes local synchronous image jobs. Their
// payloads are held in the disk queue and loaded only when a worker slot is
// available.
func GetAllUnFinishRemoteTasks(limit int) []*Task {
	var tasks []*Task
	modeExpression := taskPrivateDataUpstreamModeExpression()
	err := DB.Where("progress != ?", "100%").
		Where("status NOT IN ?", []TaskStatus{TaskStatusFailure, TaskStatusSuccess}).
		Where("platform <> ? OR COALESCE("+modeExpression+", '') <> ?", constant.TaskPlatformAsyncImage, constant.TaskImageUpstreamModeSync).
		Limit(limit).
		Order("id").
		Find(&tasks).Error
	if err != nil {
		return nil
	}
	return tasks
}

// GetQueuedLocalImageTasks loads only as many queued jobs as there are worker
// slots. In-progress local jobs are owned by workers and are not loaded again.
func GetQueuedLocalImageTasks(limit int) []*Task {
	if limit <= 0 {
		return nil
	}
	var tasks []*Task
	modeExpression := taskPrivateDataUpstreamModeExpression()
	err := DB.Where("platform = ?", constant.TaskPlatformAsyncImage).
		Where("progress != ?", "100%").
		Where("status NOT IN ?", []TaskStatus{TaskStatusInProgress, TaskStatusFailure, TaskStatusSuccess}).
		Where("COALESCE("+modeExpression+", '') = ?", constant.TaskImageUpstreamModeSync).
		Limit(limit).
		Order("id").
		Find(&tasks).Error
	if err != nil {
		return nil
	}
	return tasks
}

type FailedLocalImageTask struct {
	Task        *Task
	PayloadFile string
}

// FailStaleInProgressLocalImageTasks atomically terminalizes workers that lost
// their heartbeat and removes payload references before loading them for refunds.
func FailStaleInProgressLocalImageTasks(cutoff int64, limit int, reason string) ([]FailedLocalImageTask, error) {
	if limit <= 0 {
		return nil, nil
	}
	modeExpression := taskPrivateDataUpstreamModeExpression()
	type payloadCandidate struct {
		ID          int64
		PayloadFile string `gorm:"column:request_payload_file"`
	}
	var candidates []payloadCandidate
	err := DB.Model(&Task{}).
		Select("id, "+taskPrivateDataPayloadFileExpression()+" AS request_payload_file").
		Where("platform = ?", constant.TaskPlatformAsyncImage).
		Where("status = ?", TaskStatusInProgress).
		Where("updated_at < ?", cutoff).
		Where("COALESCE("+modeExpression+", '') = ?", constant.TaskImageUpstreamModeSync).
		Order("id").
		Limit(limit).
		Scan(&candidates).Error
	if err != nil || len(candidates) == 0 {
		return nil, err
	}
	privateDataWithoutRequest := "json_remove(private_data, '$.request_payload_file', '$.request_body', '$.request_content_type')"
	if common.UsingMainDatabase(common.DatabaseTypePostgreSQL) {
		privateDataWithoutRequest = "(private_data::jsonb - 'request_payload_file' - 'request_body' - 'request_content_type')::json"
	} else if common.UsingMainDatabase(common.DatabaseTypeMySQL) {
		privateDataWithoutRequest = "JSON_REMOVE(private_data, '$.request_payload_file', '$.request_body', '$.request_content_type')"
	}
	now := time.Now().Unix()
	failed := make([]FailedLocalImageTask, 0, len(candidates))
	for _, candidate := range candidates {
		var task Task
		won := false
		err = DB.Transaction(func(tx *gorm.DB) error {
			result := tx.Model(&Task{}).
				Where("id = ? AND status = ? AND updated_at < ?", candidate.ID, TaskStatusInProgress, cutoff).
				Updates(map[string]any{
					"status":       TaskStatusFailure,
					"progress":     "100%",
					"finish_time":  now,
					"fail_reason":  reason,
					"private_data": gorm.Expr(privateDataWithoutRequest),
					"updated_at":   now,
				})
			if result.Error != nil || result.RowsAffected == 0 {
				return result.Error
			}
			won = true
			return tx.First(&task, candidate.ID).Error
		})
		if err != nil {
			return failed, err
		}
		if won {
			failed = append(failed, FailedLocalImageTask{Task: &task, PayloadFile: candidate.PayloadFile})
		}
	}
	return failed, nil
}

func TouchInProgressLocalImageTask(id int64) error {
	return DB.Model(&Task{}).
		Where("id = ? AND status = ?", id, TaskStatusInProgress).
		Update("updated_at", time.Now().Unix()).Error
}

// HasUnfinishedSyncTasks reports whether at least one async (Suno/video) task is
// still in progress. It is a cheap existence check (LIMIT 1) used to decide
// whether the async_task_poll system task needs to run; when no task is pending
// the scheduler skips creating a row entirely.
func HasUnfinishedSyncTasks() bool {
	var id int64
	err := DB.Model(&Task{}).
		Where("progress != ?", "100%").
		Where("status != ?", TaskStatusFailure).
		Where("status != ?", TaskStatusSuccess).
		Limit(1).
		Pluck("id", &id).Error
	return err == nil && id != 0
}

func GetByTaskId(userId int, taskId string) (*Task, bool, error) {
	if taskId == "" {
		return nil, false, nil
	}
	var task *Task
	var err error
	err = DB.Where("user_id = ? and task_id = ?", userId, taskId).
		First(&task).Error
	exist, err := RecordExist(err)
	if err != nil {
		return nil, false, err
	}
	return task, exist, err
}

func GetImageTaskByTaskId(taskId string) (*Task, bool, error) {
	if taskId == "" {
		return nil, false, nil
	}
	var task *Task
	err := DB.Where("task_id = ? AND platform = ?", taskId, constant.TaskPlatformAsyncImage).
		First(&task).Error
	exist, err := RecordExist(err)
	if err != nil {
		return nil, false, err
	}
	return task, exist, nil
}

func GetByTaskIds(userId int, taskIds []any) ([]*Task, error) {
	if len(taskIds) == 0 {
		return nil, nil
	}
	var task []*Task
	var err error
	err = DB.Where("user_id = ? and task_id in (?)", userId, taskIds).
		Find(&task).Error
	if err != nil {
		return nil, err
	}
	return task, nil
}

func (Task *Task) Insert() error {
	var err error
	err = DB.Create(Task).Error
	return err
}

type taskSnapshot struct {
	Status     TaskStatus
	Progress   string
	StartTime  int64
	FinishTime int64
	FailReason string
	ResultURL  string
	Data       json.RawMessage
}

func (s taskSnapshot) Equal(other taskSnapshot) bool {
	return s.Status == other.Status &&
		s.Progress == other.Progress &&
		s.StartTime == other.StartTime &&
		s.FinishTime == other.FinishTime &&
		s.FailReason == other.FailReason &&
		s.ResultURL == other.ResultURL &&
		bytes.Equal(s.Data, other.Data)
}

func (t *Task) Snapshot() taskSnapshot {
	return taskSnapshot{
		Status:     t.Status,
		Progress:   t.Progress,
		StartTime:  t.StartTime,
		FinishTime: t.FinishTime,
		FailReason: t.FailReason,
		ResultURL:  t.PrivateData.ResultURL,
		Data:       t.Data,
	}
}

func (Task *Task) Update() error {
	var err error
	err = DB.Save(Task).Error
	return err
}

func (t *Task) UpdateQuota() error {
	return DB.Model(t).Update("quota", t.Quota).Error
}

// UpdateWithStatus performs a conditional UPDATE guarded by fromStatus (CAS).
// Returns (true, nil) if this caller won the update, (false, nil) if
// another process already moved the task out of fromStatus. MySQL commonly
// reports changed rows rather than matched rows, so a same-value no-op update
// can also return false even when the status predicate still matched.
//
// Uses Model().Select("*").Updates() instead of Save() because GORM's Save
// falls back to INSERT ON CONFLICT when the WHERE-guarded UPDATE matches
// zero rows, which silently bypasses the CAS guard.
func (t *Task) UpdateWithStatus(fromStatus TaskStatus) (bool, error) {
	result := DB.Model(t).Where("status = ?", fromStatus).Select("*").Updates(t)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// ClaimLocalTaskWithStatus updates only worker-claim fields. The persisted
// request body can be tens of megabytes, so rewriting private_data merely to
// record an attempt would create another large MVCC/TOAST row version.
func (t *Task) ClaimLocalTaskWithStatus(fromStatus TaskStatus) (bool, error) {
	t.UpdatedAt = time.Now().Unix()
	result := DB.Model(&Task{}).
		Where("id = ? AND status = ?", t.ID, fromStatus).
		Updates(map[string]any{
			"status":     t.Status,
			"progress":   t.Progress,
			"start_time": t.StartTime,
			"updated_at": t.UpdatedAt,
		})
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// TaskBulkUpdateByID performs an unconditional bulk UPDATE by primary key IDs.
// WARNING: This function has NO CAS (Compare-And-Swap) guard — it will overwrite
// any concurrent status changes. DO NOT use in billing/quota lifecycle flows
// (e.g., timeout, success, failure transitions that trigger refunds or settlements).
// For status transitions that involve billing, use Task.UpdateWithStatus() instead.
func TaskBulkUpdateByID(ids []int64, params map[string]any) error {
	if len(ids) == 0 {
		return nil
	}
	return DB.Model(&Task{}).
		Where("id in (?)", ids).
		Updates(params).Error
}

type TaskQuotaUsage struct {
	Mode  string  `json:"mode"`
	Count float64 `json:"count"`
}

// TaskCountAllTasks returns total tasks that match the given query params (admin usage)
func TaskCountAllTasks(queryParams SyncTaskQueryParams) int64 {
	var total int64
	query := DB.Model(&Task{})
	if queryParams.ChannelID != "" {
		query = query.Where("channel_id = ?", queryParams.ChannelID)
	}
	if queryParams.Platform != "" {
		query = query.Where("platform = ?", queryParams.Platform)
	}
	if queryParams.UserID != "" {
		query = query.Where("user_id = ?", queryParams.UserID)
	}
	if len(queryParams.UserIDs) != 0 {
		query = query.Where("user_id in (?)", queryParams.UserIDs)
	}
	if queryParams.TaskID != "" {
		query = query.Where("task_id = ?", queryParams.TaskID)
	}
	if queryParams.Action != "" {
		query = query.Where("action = ?", queryParams.Action)
	}
	if queryParams.Status != "" {
		query = query.Where("status = ?", queryParams.Status)
	}
	if queryParams.StartTimestamp != 0 {
		query = query.Where("submit_time >= ?", queryParams.StartTimestamp)
	}
	if queryParams.EndTimestamp != 0 {
		query = query.Where("submit_time <= ?", queryParams.EndTimestamp)
	}
	_ = query.Count(&total).Error
	return total
}

// TaskCountAllUserTask returns total tasks for given user
func TaskCountAllUserTask(userId int, queryParams SyncTaskQueryParams) int64 {
	var total int64
	query := DB.Model(&Task{}).Where("user_id = ?", userId)
	if queryParams.TaskID != "" {
		query = query.Where("task_id = ?", queryParams.TaskID)
	}
	if queryParams.Action != "" {
		query = query.Where("action = ?", queryParams.Action)
	}
	if queryParams.Status != "" {
		query = query.Where("status = ?", queryParams.Status)
	}
	if queryParams.Platform != "" {
		query = query.Where("platform = ?", queryParams.Platform)
	}
	if queryParams.StartTimestamp != 0 {
		query = query.Where("submit_time >= ?", queryParams.StartTimestamp)
	}
	if queryParams.EndTimestamp != 0 {
		query = query.Where("submit_time <= ?", queryParams.EndTimestamp)
	}
	_ = query.Count(&total).Error
	return total
}
func (t *Task) ToOpenAIVideo() *dto.OpenAIVideo {
	openAIVideo := dto.NewOpenAIVideo()
	openAIVideo.ID = t.TaskID
	openAIVideo.Status = t.Status.ToVideoStatus()
	openAIVideo.Model = t.Properties.OriginModelName
	openAIVideo.SetProgressStr(t.Progress)
	openAIVideo.CreatedAt = t.CreatedAt
	openAIVideo.CompletedAt = t.UpdatedAt
	openAIVideo.SetMetadata("url", t.GetResultURL())
	return openAIVideo
}
