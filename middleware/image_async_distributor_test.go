package middleware

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/QuantumNous/new-api/common"

	"github.com/gin-gonic/gin"
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
