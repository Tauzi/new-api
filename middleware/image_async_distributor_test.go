package middleware

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/QuantumNous/new-api/common"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetModelFromRequestReadsAsyncForImageJSON(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", bytes.NewBufferString(`{
		"model":"gpt-image-2-async",
		"async":true
	}`))
	c.Request.Header.Set("Content-Type", "application/json")
	t.Cleanup(func() { common.CleanupBodyStorage(c) })

	request, err := getModelFromRequest(c)
	require.NoError(t, err)
	require.Equal(t, "gpt-image-2-async", request.Model)
	require.True(t, request.Async)
}

func TestGetModelFromRequestReadsAsyncForImageMultipart(t *testing.T) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	require.NoError(t, writer.WriteField("model", "gpt-image-2-async"))
	require.NoError(t, writer.WriteField("async", "true"))
	require.NoError(t, writer.Close())

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/edits", bytes.NewReader(body.Bytes()))
	c.Request.Header.Set("Content-Type", writer.FormDataContentType())
	t.Cleanup(func() { common.CleanupBodyStorage(c) })

	request, err := getModelFromRequest(c)
	require.NoError(t, err)
	require.Equal(t, "gpt-image-2-async", request.Model)
	require.True(t, request.Async)
}

func TestGetModelFromRequestSpoolsImageMultipartToDiskAndCleansIt(t *testing.T) {
	source, err := os.CreateTemp(t.TempDir(), "multipart-source-*.tmp")
	require.NoError(t, err)
	writer := multipart.NewWriter(source)
	require.NoError(t, writer.WriteField("model", "gpt-image-2-async"))
	require.NoError(t, writer.WriteField("async", "true"))
	part, err := writer.CreateFormFile("image", "input.png")
	require.NoError(t, err)
	_, err = io.WriteString(part, "non-empty-image-data")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, source.Close())

	requestBody, err := os.Open(source.Name())
	require.NoError(t, err)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/edits", requestBody)
	c.Request.Header.Set("Content-Type", writer.FormDataContentType())
	t.Cleanup(func() { common.CleanupBodyStorage(c) })

	request, err := getModelFromRequest(c)
	require.NoError(t, err)
	require.Equal(t, "gpt-image-2-async", request.Model)
	require.True(t, request.Async)

	stored, exists := c.Get(common.KeyBodyStorage)
	require.True(t, exists)
	storage, ok := stored.(common.BodyStorage)
	require.True(t, ok)
	assert.True(t, storage.IsDisk())

	uploaded, err := c.Request.MultipartForm.File["image"][0].Open()
	require.NoError(t, err)
	diskFile, ok := uploaded.(*os.File)
	require.True(t, ok)
	multipartPath := diskFile.Name()
	require.NoError(t, uploaded.Close())
	assert.FileExists(t, multipartPath)

	common.CleanupBodyStorage(c)
	assert.Nil(t, c.Request.MultipartForm)
	assert.NoFileExists(t, multipartPath)
}

func TestGetModelFromRequestIgnoresUnrelatedAsyncField(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(`{
		"model":"gpt-4o",
		"async":"provider-specific-value"
	}`))
	c.Request.Header.Set("Content-Type", "application/json")
	t.Cleanup(func() { common.CleanupBodyStorage(c) })

	request, err := getModelFromRequest(c)
	require.NoError(t, err)
	require.Equal(t, "gpt-4o", request.Model)
	require.False(t, request.Async)
}
