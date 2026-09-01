package image

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	stdimage "image"
	"image/color"
	"image/png"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	relayconstant "github.com/QuantumNous/new-api/relay/constant"
	"github.com/QuantumNous/new-api/service"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testModel = "gpt-image-2-4k-async"

func useTestImageQueue(t *testing.T) {
	t.Helper()
	previous := constant.AsyncImageQueueDir
	constant.AsyncImageQueueDir = t.TempDir()
	t.Cleanup(func() { constant.AsyncImageQueueDir = previous })
}

func newJSONContext(t *testing.T, body string) *gin.Context {
	t.Helper()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", bytes.NewBufferString(body))
	c.Request.Header.Set("Content-Type", "application/json")
	t.Cleanup(func() { common.CleanupBodyStorage(c) })
	return c
}

func TestBuildRequestBodyNormalizesAsyncImageRequest(t *testing.T) {
	c := newJSONContext(t, `{
		"async": true,
		"image_size": "4k",
		"images": ["https://example.com/reference.png"],
		"model": "gpt-image-2-4k-async",
		"n": 1,
		"prompt": "city at night",
		"quality": "auto",
		"size": "1024x1024"
	}`)
	info := &relaycommon.RelayInfo{
		RelayMode:       relayconstant.RelayModeImagesGenerations,
		OriginModelName: testModel,
		ChannelMeta: &relaycommon.ChannelMeta{
			UpstreamModelName: testModel,
		},
	}
	adaptor := &TaskAdaptor{}
	require.Nil(t, adaptor.ValidateRequestAndSetAction(c, info))
	assert.Equal(t, constant.TaskActionImageGenerate, info.Action)

	reader, err := adaptor.BuildRequestBody(c, info)
	require.NoError(t, err)
	body, err := io.ReadAll(reader)
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, common.Unmarshal(body, &got))
	assert.Equal(t, testModel, got["model"])
	assert.Equal(t, true, got["async"])
	assert.EqualValues(t, 1, got["n"])
	assert.Equal(t, "medium", got["quality"])
	assert.Equal(t, "4k", got["image_size"])
	assert.Equal(t, "1024x1024", got["size"])
	assert.Equal(t, []any{"https://example.com/reference.png"}, got["images"])
}

func TestBuildRequestBodyNormalizesInvalidSizeTypeToAuto(t *testing.T) {
	c := newJSONContext(t, `{
		"model": "gpt-image-2-4k-async",
		"prompt": "city at night",
		"size": true
	}`)
	info := &relaycommon.RelayInfo{
		RelayMode:       relayconstant.RelayModeImagesGenerations,
		OriginModelName: testModel,
		ChannelMeta: &relaycommon.ChannelMeta{
			UpstreamModelName: testModel,
		},
	}
	adaptor := &TaskAdaptor{}
	require.Nil(t, adaptor.ValidateRequestAndSetAction(c, info))

	reader, err := adaptor.BuildRequestBody(c, info)
	require.NoError(t, err)
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, common.Unmarshal(body, &got))
	assert.Equal(t, "auto", got["size"])
}

func TestBuildRequestBodyPreservesStringSize(t *testing.T) {
	c := newJSONContext(t, `{
		"model": "gpt-image-2-4k-async",
		"prompt": "city at night",
		"size": "1024×1024"
	}`)
	info := &relaycommon.RelayInfo{
		RelayMode:       relayconstant.RelayModeImagesGenerations,
		OriginModelName: testModel,
		ChannelMeta: &relaycommon.ChannelMeta{
			UpstreamModelName: testModel,
		},
	}
	reader, err := (&TaskAdaptor{}).BuildRequestBody(c, info)
	require.NoError(t, err)
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, common.Unmarshal(body, &got))
	assert.Equal(t, "1024×1024", got["size"])
}

func TestPrepareLocalTaskUsesSynchronousUpstreamMode(t *testing.T) {
	useTestImageQueue(t)
	c := newJSONContext(t, `{
		"async": true,
		"model": "vendor-image-sync",
		"prompt": "local worker",
		"size": "2048x2048"
	}`)
	info := &relaycommon.RelayInfo{
		RelayMode:       relayconstant.RelayModeImagesGenerations,
		OriginModelName: "vendor-image-sync",
		ChannelMeta: &relaycommon.ChannelMeta{
			UpstreamModelName: "vendor-image-sync",
			ChannelOtherSettings: dto.ChannelOtherSettings{
				ImageTaskMode: constant.TaskImageUpstreamModeSync,
			},
		},
		TaskRelayInfo: &relaycommon.TaskRelayInfo{PublicTaskID: "task_local"},
	}

	localData, isLocal, err := (&TaskAdaptor{}).PrepareLocalTask(c, info)
	require.NoError(t, err)
	require.True(t, isLocal)
	require.NotNil(t, localData)
	assert.NotEmpty(t, localData.PayloadFile)
	assert.False(t, c.Writer.Written())

	payload, err := service.OpenAsyncImagePayload(localData.PayloadFile)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = payload.Close()
		_ = service.RemoveAsyncImagePayload(localData.PayloadFile)
	})
	requestBody, err := io.ReadAll(payload)
	require.NoError(t, err)
	var upstreamBody map[string]any
	require.NoError(t, common.Unmarshal(requestBody, &upstreamBody))
	assert.Equal(t, "vendor-image-sync", upstreamBody["model"])
	assert.Equal(t, "2048x2048", upstreamBody["size"])
	assert.NotContains(t, upstreamBody, "async")
}

func TestExecuteLocalTaskParsesSynchronousImageResponse(t *testing.T) {
	useTestImageQueue(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/v1/images/generations", r.URL.Path)
		assert.Equal(t, "Bearer secret", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"vendor-image-sync","data":[{"url":"https://example.com/result.png"}]}`))
	}))
	defer server.Close()

	payloadFile, err := service.WriteAsyncImagePayload("task_local", func(dst io.Writer) error {
		_, writeErr := io.WriteString(dst, `{"model":"vendor-image-sync","prompt":"test"}`)
		return writeErr
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.RemoveAsyncImagePayload(payloadFile) })
	task := &model.Task{
		TaskID: "task_local",
		Action: constant.TaskActionImageGenerate,
		PrivateData: model.TaskPrivateData{
			RequestPayloadFile: payloadFile,
			RequestContentType: "application/json",
		},
	}
	channelBaseURL := server.URL
	ch := &model.Channel{BaseURL: &channelBaseURL, Key: "secret"}
	ch.SetOtherSettings(dto.ChannelOtherSettings{
		ImageURLSourcePrefix: "https://example.com",
		ImageURLTargetPrefix: "https://images.example.net",
	})

	result, responseBody, err := (&TaskAdaptor{}).ExecuteLocalTask(context.Background(), task, ch)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, string(model.TaskStatusSuccess), result.Status)
	assert.Equal(t, "https://images.example.net/result.png", result.Url)
	assert.Contains(t, string(responseBody), "https://images.example.net/result.png")
	assert.Contains(t, string(responseBody), "result.png")
}

func TestExecuteLocalTaskReturnsHTTPStatusErrors(t *testing.T) {
	tests := []struct {
		name          string
		status        int
		wantRetryable bool
	}{
		{name: "bad request", status: http.StatusBadRequest},
		{name: "rate limit", status: http.StatusTooManyRequests},
		{name: "server error", status: http.StatusInternalServerError},
		{name: "gateway timeout", status: http.StatusGatewayTimeout, wantRetryable: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(`{"error":{"message":"upstream failed"}}`))
			}))
			t.Cleanup(server.Close)

			task := &model.Task{
				TaskID: "task_retryable_status",
				Action: constant.TaskActionImageGenerate,
				PrivateData: model.TaskPrivateData{
					RequestBody:        []byte(`{"model":"sync-image","prompt":"test"}`),
					RequestContentType: "application/json",
				},
			}
			baseURL := server.URL
			ch := &model.Channel{BaseURL: &baseURL, Key: "secret"}

			_, _, err := (&TaskAdaptor{}).ExecuteLocalTask(context.Background(), task, ch)
			require.Error(t, err)
			assert.ErrorContains(t, err, fmt.Sprintf("status %d", tt.status))
			var retryableErr *service.RetryableLocalTaskError
			assert.Equal(t, tt.wantRetryable, errors.As(err, &retryableErr))
		})
	}
}

func TestRetryableSynchronousImageTransportErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "eof", err: fmt.Errorf("request failed: %w", io.EOF), want: true},
		{name: "unexpected eof", err: fmt.Errorf("read failed: %w", io.ErrUnexpectedEOF), want: true},
		{name: "closed connection", err: errors.New("read: connection reset by peer"), want: true},
		{name: "deadline", err: context.DeadlineExceeded},
		{name: "validation", err: errors.New("invalid request")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isRetryableSynchronousImageTransportError(tt.err))
		})
	}
}

func TestValidateRequestAllowsModelSpecificSize(t *testing.T) {
	c := newJSONContext(t, `{
		"async": true,
		"model": "vendor-image-8k-async",
		"prompt": "large canvas",
		"size": "8192x8192",
		"image_size": "8k"
	}`)
	info := &relaycommon.RelayInfo{
		RelayMode:       relayconstant.RelayModeImagesGenerations,
		OriginModelName: "vendor-image-8k-async",
		ChannelMeta: &relaycommon.ChannelMeta{
			UpstreamModelName: "vendor-image-8k-async",
		},
	}
	assert.Nil(t, (&TaskAdaptor{}).ValidateRequestAndSetAction(c, info))
}

func TestValidateJSONReferencesDeduplicatesAliases(t *testing.T) {
	fields := map[string]json.RawMessage{
		"image":     json.RawMessage(`"https://example.com/a.png"`),
		"images":    json.RawMessage(`["https://example.com/a.png","https://example.com/b.png"]`),
		"imageUrls": json.RawMessage(`["https://example.com/b.png"]`),
	}
	require.NoError(t, validateJSONReferences(fields))

	tooMany := make([]string, 10)
	for i := range tooMany {
		tooMany[i] = "https://example.com/" + string(rune('a'+i)) + ".png"
	}
	encoded, err := common.Marshal(tooMany)
	require.NoError(t, err)
	err = validateJSONReferences(map[string]json.RawMessage{"images": encoded})
	assert.ErrorContains(t, err, "at most 9")
}

func TestParseTaskResult(t *testing.T) {
	adaptor := &TaskAdaptor{}
	result, err := adaptor.ParseTaskResult([]byte(`{
		"id":"upstream-task",
		"status":"completed",
		"progress":"100%",
		"data":[{"url":"https://example.com/result.png"}]
	}`))
	require.NoError(t, err)
	assert.Equal(t, "upstream-task", result.TaskID)
	assert.Equal(t, string(model.TaskStatusSuccess), result.Status)
	assert.Equal(t, "100%", result.Progress)
	assert.Equal(t, "https://example.com/result.png", result.Url)
}

func TestBuildEndpointAcceptsBaseURLWithOrWithoutV1(t *testing.T) {
	assert.Equal(t, "https://mianyunai.com/v1/images/generations", buildEndpoint("https://mianyunai.com", "/v1/images/generations"))
	assert.Equal(t, "https://mianyunai.com/v1/images/generations", buildEndpoint("https://mianyunai.com/v1", "/v1/images/generations"))
}

func TestFetchTaskUsesEditEndpointAndAuthorization(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/images/edits/upstream-task", r.URL.Path)
		assert.Equal(t, "Bearer secret", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"upstream-task","status":"queued","progress":"10%"}`))
	}))
	t.Cleanup(server.Close)

	adaptor := &TaskAdaptor{}
	resp, err := adaptor.FetchTask(server.URL, "secret", map[string]any{
		"task_id": "upstream-task",
		"action":  constant.TaskActionImageEdit,
	}, "")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestDoResponseHidesUpstreamTaskID(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	info := &relaycommon.RelayInfo{
		OriginModelName: testModel,
		TaskRelayInfo: &relaycommon.TaskRelayInfo{
			PublicTaskID: "task_public",
		},
	}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body: io.NopCloser(bytes.NewBufferString(`{
			"id":"task_upstream",
			"model":"gpt-image-2-4k-async",
			"object":"image.generation",
			"progress":"10%",
			"status":"queued"
		}`)),
	}

	upstreamID, _, taskErr := (&TaskAdaptor{}).DoResponse(c, resp, info)
	require.Nil(t, taskErr)
	assert.Equal(t, "task_upstream", upstreamID)
	assert.NotContains(t, recorder.Body.String(), "task_upstream")
	assert.Contains(t, recorder.Body.String(), "task_public")
}

func TestBuildMultipartBodyPreservesRepeatedImages(t *testing.T) {
	var original bytes.Buffer
	writer := multipart.NewWriter(&original)
	require.NoError(t, writer.WriteField("model", testModel))
	require.NoError(t, writer.WriteField("prompt", "edit these images"))
	require.NoError(t, writer.WriteField("async", "true"))
	require.NoError(t, writer.WriteField("quality", "auto"))
	for _, name := range []string{"one.png", "two.png"} {
		part, err := writer.CreateFormFile("image", name)
		require.NoError(t, err)
		_, err = part.Write([]byte("image-data-" + name))
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/edits", bytes.NewReader(original.Bytes()))
	c.Request.Header.Set("Content-Type", writer.FormDataContentType())
	t.Cleanup(func() { common.CleanupBodyStorage(c) })
	info := &relaycommon.RelayInfo{
		RelayMode:       relayconstant.RelayModeImagesEdits,
		OriginModelName: testModel,
		ChannelMeta: &relaycommon.ChannelMeta{
			UpstreamModelName: testModel,
		},
	}
	adaptor := &TaskAdaptor{}
	require.Nil(t, adaptor.ValidateRequestAndSetAction(c, info))
	assert.Equal(t, constant.TaskActionImageEdit, info.Action)

	reader, err := adaptor.BuildRequestBody(c, info)
	require.NoError(t, err)
	rebuilt, err := io.ReadAll(reader)
	require.NoError(t, err)
	_, params, err := mime.ParseMediaType(c.GetString("image_task_content_type"))
	require.NoError(t, err)
	form, err := multipart.NewReader(bytes.NewReader(rebuilt), params["boundary"]).ReadForm(maxImageBytes)
	require.NoError(t, err)
	t.Cleanup(func() { _ = form.RemoveAll() })

	assert.Equal(t, testModel, form.Value["model"][0])
	assert.Equal(t, "true", form.Value["async"][0])
	assert.Equal(t, "1", form.Value["n"][0])
	assert.Equal(t, "medium", form.Value["quality"][0])
	assert.Len(t, form.File["image"], 2)
}

func TestPrepareLocalMultipartTaskSpoolsRepeatedImages(t *testing.T) {
	useTestImageQueue(t)
	var original bytes.Buffer
	writer := multipart.NewWriter(&original)
	require.NoError(t, writer.WriteField("model", testModel))
	require.NoError(t, writer.WriteField("prompt", "edit these images"))
	require.NoError(t, writer.WriteField("async", "true"))
	for _, name := range []string{"one.png", "two.png"} {
		part, err := writer.CreateFormFile("image", name)
		require.NoError(t, err)
		_, err = part.Write([]byte("image-data-" + name))
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/edits", bytes.NewReader(original.Bytes()))
	c.Request.Header.Set("Content-Type", writer.FormDataContentType())
	t.Cleanup(func() { common.CleanupBodyStorage(c) })
	info := &relaycommon.RelayInfo{
		RelayMode:       relayconstant.RelayModeImagesEdits,
		OriginModelName: testModel,
		ChannelMeta: &relaycommon.ChannelMeta{
			UpstreamModelName: testModel,
			ChannelOtherSettings: dto.ChannelOtherSettings{
				ImageTaskMode: constant.TaskImageUpstreamModeSync,
			},
		},
		TaskRelayInfo: &relaycommon.TaskRelayInfo{PublicTaskID: "task_local_multipart"},
	}
	adaptor := &TaskAdaptor{}
	require.Nil(t, adaptor.ValidateRequestAndSetAction(c, info))

	localData, isLocal, err := adaptor.PrepareLocalTask(c, info)
	require.NoError(t, err)
	require.True(t, isLocal)
	payload, err := service.OpenAsyncImagePayload(localData.PayloadFile)
	require.NoError(t, err)
	defer payload.Close()
	t.Cleanup(func() { _ = service.RemoveAsyncImagePayload(localData.PayloadFile) })
	_, params, err := mime.ParseMediaType(localData.ContentType)
	require.NoError(t, err)
	form, err := multipart.NewReader(payload, params["boundary"]).ReadForm(maxImageBytes)
	require.NoError(t, err)
	t.Cleanup(func() { _ = form.RemoveAll() })

	assert.Empty(t, form.Value["async"])
	assert.Equal(t, "1", form.Value["n"][0])
	assert.Len(t, form.File["image"], 2)
}

func TestBuildMultipartBodySpoolsOutboundRequestToDisk(t *testing.T) {
	var original bytes.Buffer
	writer := multipart.NewWriter(&original)
	require.NoError(t, writer.WriteField("model", testModel))
	require.NoError(t, writer.WriteField("prompt", "stream this image"))
	require.NoError(t, writer.WriteField("async", "true"))
	part, err := writer.CreateFormFile("image", "input.png")
	require.NoError(t, err)
	_, err = part.Write([]byte("image-data"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/edits", bytes.NewReader(original.Bytes()))
	c.Request.Header.Set("Content-Type", writer.FormDataContentType())
	t.Cleanup(func() { common.CleanupBodyStorage(c) })
	info := &relaycommon.RelayInfo{
		RelayMode:       relayconstant.RelayModeImagesEdits,
		OriginModelName: testModel,
		ChannelMeta: &relaycommon.ChannelMeta{
			UpstreamModelName: testModel,
		},
	}

	reader, err := (&TaskAdaptor{}).BuildRequestBody(c, info)
	require.NoError(t, err)
	storage, ok := reader.(common.BodyStorage)
	require.True(t, ok)
	defer storage.Close()
	assert.True(t, storage.IsDisk())
	assert.Equal(t, storage.Size(), info.UpstreamRequestBodySize)
	rebuilt, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Contains(t, string(rebuilt), "image-data")
}

func TestValidateMultipartMask(t *testing.T) {
	buildContext := func(t *testing.T, input, mask []byte) *gin.Context {
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		require.NoError(t, writer.WriteField("model", testModel))
		require.NoError(t, writer.WriteField("prompt", "edit the transparent area"))
		require.NoError(t, writer.WriteField("async", "true"))
		for field, data := range map[string][]byte{"image": input, "mask": mask} {
			part, err := writer.CreateFormFile(field, field+".png")
			require.NoError(t, err)
			_, err = part.Write(data)
			require.NoError(t, err)
		}
		require.NoError(t, writer.Close())

		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/edits", bytes.NewReader(body.Bytes()))
		c.Request.Header.Set("Content-Type", writer.FormDataContentType())
		t.Cleanup(func() { common.CleanupBodyStorage(c) })
		return c
	}
	info := func() *relaycommon.RelayInfo {
		return &relaycommon.RelayInfo{
			RelayMode: relayconstant.RelayModeImagesEdits,
			ChannelMeta: &relaycommon.ChannelMeta{
				UpstreamModelName: testModel,
			},
		}
	}

	t.Run("matching alpha mask is accepted", func(t *testing.T) {
		request := buildContext(t, makeAlphaPNG(t, 32, 32), makeAlphaPNG(t, 32, 32))
		require.Nil(t, (&TaskAdaptor{}).ValidateRequestAndSetAction(request, info()))
	})
	t.Run("different dimensions are rejected", func(t *testing.T) {
		request := buildContext(t, makeAlphaPNG(t, 32, 32), makeAlphaPNG(t, 16, 32))
		taskErr := (&TaskAdaptor{}).ValidateRequestAndSetAction(request, info())
		require.NotNil(t, taskErr)
		assert.Contains(t, taskErr.Message, "same dimensions")
	})
}

func makeAlphaPNG(t *testing.T, width, height int) []byte {
	t.Helper()
	img := stdimage.NewNRGBA(stdimage.Rect(0, 0, width, height))
	img.Set(0, 0, color.NRGBA{R: 255, A: 0})
	var body bytes.Buffer
	require.NoError(t, png.Encode(&body, img))
	return body.Bytes()
}
