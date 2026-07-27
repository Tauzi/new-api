package router

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/model"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestImageTaskFetchIsPublicAndImageOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&model.Task{}))

	previousDB := model.DB
	model.DB = db
	t.Cleanup(func() { model.DB = previousDB })

	data, err := common.Marshal(map[string]any{
		"model": "gpt-image-test",
		"data":  []map[string]string{{"url": "https://example.test/generated.png"}},
	})
	require.NoError(t, err)
	require.NoError(t, db.Create(&model.Task{
		TaskID:   "task_public_image",
		UserId:   42,
		Platform: constant.TaskPlatformAsyncImage,
		Action:   constant.TaskActionImageGenerate,
		Status:   model.TaskStatusSuccess,
		Progress: "100%",
		Data:     data,
	}).Error)
	require.NoError(t, db.Create(&model.Task{
		TaskID:   "task_non_image",
		UserId:   42,
		Platform: constant.TaskPlatformSuno,
		Action:   constant.SunoActionMusic,
		Status:   model.TaskStatusSuccess,
		Progress: "100%",
	}).Error)

	engine := gin.New()
	SetRelayRouter(engine)

	t.Run("fetches image task without authorization", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/v1/images/generations/task_public_image", nil)
		engine.ServeHTTP(recorder, request)

		require.Equal(t, http.StatusOK, recorder.Code)
		var response dto.OpenAIImageTask
		require.NoError(t, common.Unmarshal(recorder.Body.Bytes(), &response))
		assert.Equal(t, "task_public_image", response.ID)
		assert.Equal(t, dto.ImageTaskStatusCompleted, response.Status)
		require.Len(t, response.Data, 1)
		assert.Equal(t, "https://example.test/generated.png", response.Data[0].Url)
	})

	t.Run("does not expose non-image tasks", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/v1/images/generations/task_non_image", nil)
		engine.ServeHTTP(recorder, request)

		assert.Equal(t, http.StatusNotFound, recorder.Code)
	})
}
