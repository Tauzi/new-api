package image

import (
	"bytes"
	"encoding/json"
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
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	relayconstant "github.com/QuantumNous/new-api/relay/constant"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		"image_size": "1k",
		"images": ["https://example.com/reference.png"],
		"model": "gpt-image-2-async",
		"n": 1,
		"prompt": "city at night",
		"quality": "auto",
		"size": "1024x1024"
	}`)
	info := &relaycommon.RelayInfo{
		RelayMode:       relayconstant.RelayModeImagesGenerations,
		OriginModelName: modelName,
		ChannelMeta: &relaycommon.ChannelMeta{
			UpstreamModelName: modelName,
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
	assert.Equal(t, modelName, got["model"])
	assert.Equal(t, true, got["async"])
	assert.EqualValues(t, 1, got["n"])
	assert.Equal(t, "medium", got["quality"])
	assert.Equal(t, "1K", got["image_size"])
	assert.Equal(t, "1024x1024", got["size"])
	assert.Equal(t, []any{"https://example.com/reference.png"}, got["images"])
}

func TestValidateSize(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "square", value: "1024x1024"},
		{name: "exact dimensions", value: "1024x768"},
		{name: "ratio", value: "16:9"},
		{name: "not aligned", value: "1000x1000", wantErr: true},
		{name: "too few pixels", value: "512x512", wantErr: true},
		{name: "too many pixels", value: "2048x1024", wantErr: true},
		{name: "ratio too wide", value: "2048x512", wantErr: true},
		{name: "ratio input too wide", value: "4:1", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSize(tt.value)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
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
		OriginModelName: modelName,
		TaskRelayInfo: &relaycommon.TaskRelayInfo{
			PublicTaskID: "task_public",
		},
	}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body: io.NopCloser(bytes.NewBufferString(`{
			"id":"task_upstream",
			"model":"gpt-image-2-async",
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
	require.NoError(t, writer.WriteField("model", modelName))
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
		OriginModelName: modelName,
		ChannelMeta: &relaycommon.ChannelMeta{
			UpstreamModelName: modelName,
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

	assert.Equal(t, modelName, form.Value["model"][0])
	assert.Equal(t, "true", form.Value["async"][0])
	assert.Equal(t, "1", form.Value["n"][0])
	assert.Equal(t, "medium", form.Value["quality"][0])
	assert.Len(t, form.File["image"], 2)
}

func TestValidateMultipartMask(t *testing.T) {
	buildContext := func(t *testing.T, input, mask []byte) *gin.Context {
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		require.NoError(t, writer.WriteField("model", modelName))
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
				UpstreamModelName: modelName,
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
