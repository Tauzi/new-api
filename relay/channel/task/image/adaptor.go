package image

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	stdimage "image"
	"image/png"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/relay/channel"
	taskcommon "github.com/QuantumNous/new-api/relay/channel/task/taskcommon"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	relayconstant "github.com/QuantumNous/new-api/relay/constant"
	"github.com/QuantumNous/new-api/service"

	"github.com/gin-gonic/gin"
)

const (
	maxImageBytes = 10 << 20
)

type TaskAdaptor struct {
	taskcommon.BaseBilling
	baseURL string
	apiKey  string
}

type imageTaskResponse struct {
	ID       string          `json:"id"`
	TaskID   string          `json:"task_id"`
	Model    string          `json:"model"`
	Object   string          `json:"object"`
	Progress json.RawMessage `json:"progress"`
	Status   string          `json:"status"`
	Data     []dto.ImageData `json:"data"`
	Error    *imageTaskError `json:"error,omitempty"`
}

type imageTaskError struct {
	Message string `json:"message"`
	Code    string `json:"code"`
}

func (a *TaskAdaptor) Init(info *relaycommon.RelayInfo) {
	if info == nil || info.ChannelMeta == nil {
		return
	}
	a.baseURL = strings.TrimRight(info.ChannelBaseUrl, "/")
	a.apiKey = info.ApiKey
}

func (a *TaskAdaptor) ValidateRequestAndSetAction(c *gin.Context, info *relaycommon.RelayInfo) *dto.TaskError {
	if info == nil {
		return taskError(fmt.Errorf("relay info is required"), "invalid_request")
	}
	if info.TaskRelayInfo == nil {
		info.TaskRelayInfo = &relaycommon.TaskRelayInfo{}
	}
	if info.RelayMode == relayconstant.RelayModeImagesEdits {
		info.Action = constant.TaskActionImageEdit
	} else {
		info.Action = constant.TaskActionImageGenerate
	}

	if err := validateRequest(c, info); err != nil {
		return taskError(err, "invalid_request")
	}
	return nil
}

func (a *TaskAdaptor) BuildRequestURL(info *relaycommon.RelayInfo) (string, error) {
	if a.baseURL == "" {
		return "", fmt.Errorf("image task channel base url is empty")
	}
	path := "/v1/images/generations"
	if info.Action == constant.TaskActionImageEdit {
		path = "/v1/images/edits"
	}
	return buildEndpoint(a.baseURL, path), nil
}

func (a *TaskAdaptor) BuildRequestHeader(c *gin.Context, req *http.Request, _ *relaycommon.RelayInfo) error {
	if a.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+a.apiKey)
	}
	contentType := c.GetString("image_task_content_type")
	if contentType == "" {
		contentType = "application/json"
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Accept", "application/json")
	return nil
}

func (a *TaskAdaptor) BuildRequestBody(c *gin.Context, info *relaycommon.RelayInfo) (io.Reader, error) {
	return a.buildRequestBody(c, info, true)
}

func (a *TaskAdaptor) buildRequestBody(c *gin.Context, info *relaycommon.RelayInfo, upstreamAsync bool) (io.Reader, error) {
	contentType := c.GetHeader("Content-Type")
	if strings.HasPrefix(contentType, "multipart/form-data") {
		return a.buildMultipartBody(c, info, upstreamAsync)
	}

	storage, err := common.GetBodyStorage(c)
	if err != nil {
		return nil, fmt.Errorf("get request body: %w", err)
	}
	body, err := storage.Bytes()
	if err != nil {
		return nil, fmt.Errorf("read request body: %w", err)
	}

	var fields map[string]json.RawMessage
	if err := common.Unmarshal(body, &fields); err != nil {
		return nil, fmt.Errorf("decode image request: %w", err)
	}
	if value, ok := fields["size"]; ok {
		if common.GetJsonType(value) != "string" {
			autoSize, marshalErr := common.Marshal("auto")
			if marshalErr != nil {
				return nil, marshalErr
			}
			fields["size"] = autoSize
		}
	}
	model, err := resolveModel(info, fields)
	if err != nil {
		return nil, err
	}
	fields["model"] = json.RawMessage(strconv.Quote(model))
	if upstreamAsync {
		fields["async"] = json.RawMessage("true")
	} else {
		delete(fields, "async")
	}
	fields["n"] = json.RawMessage("1")
	quality := "medium"
	if value, ok := fields["quality"]; ok {
		var requested string
		if err := common.Unmarshal(value, &requested); err == nil && requested != "" && !strings.EqualFold(requested, "auto") {
			quality = requested
		}
	}
	fields["quality"] = json.RawMessage(strconv.Quote(quality))
	out, err := common.Marshal(fields)
	if err != nil {
		return nil, err
	}
	c.Set("image_task_content_type", "application/json")
	return bytes.NewReader(out), nil
}

func (a *TaskAdaptor) buildMultipartBody(c *gin.Context, info *relaycommon.RelayInfo, upstreamAsync bool) (io.Reader, error) {
	form, err := imageMultipartForm(c)
	if err != nil {
		return nil, err
	}
	requestStorage, err := common.GetBodyStorage(c)
	if err != nil {
		return nil, err
	}
	pipeReader, pipeWriter := io.Pipe()
	writer := multipart.NewWriter(pipeWriter)
	c.Set("image_task_content_type", writer.FormDataContentType())
	go func() {
		writeErr := a.writeMultipartForm(info, upstreamAsync, form, writer)
		if writeErr == nil {
			writeErr = writer.Close()
		}
		_ = pipeWriter.CloseWithError(writeErr)
	}()
	// Keep Content-Length compatibility for upstreams that reject chunked
	// multipart requests while still avoiding a full in-memory buffer.
	outboundStorage, err := common.CreateDiskBodyStorageFromReader(pipeReader, requestStorage.Size()+(1<<20))
	_ = pipeReader.Close()
	if err != nil {
		return nil, err
	}
	info.UpstreamRequestBodySize = outboundStorage.Size()
	return outboundStorage, nil
}

func (a *TaskAdaptor) writeMultipartBody(c *gin.Context, info *relaycommon.RelayInfo, upstreamAsync bool, dst io.Writer) (string, error) {
	form, err := imageMultipartForm(c)
	if err != nil {
		return "", fmt.Errorf("parse multipart image request: %w", err)
	}

	writer := multipart.NewWriter(dst)
	if err := a.writeMultipartForm(info, upstreamAsync, form, writer); err != nil {
		return "", err
	}
	if err := writer.Close(); err != nil {
		return "", err
	}
	return writer.FormDataContentType(), nil
}

func (a *TaskAdaptor) writeMultipartForm(info *relaycommon.RelayInfo, upstreamAsync bool, form *multipart.Form, writer *multipart.Writer) error {
	model, err := resolveMultipartModel(info, form.Value)
	if err != nil {
		return err
	}
	if err := writer.WriteField("model", model); err != nil {
		return err
	}
	if upstreamAsync {
		if err := writer.WriteField("async", "true"); err != nil {
			return err
		}
	}
	if err := writer.WriteField("n", "1"); err != nil {
		return err
	}
	formValues := url.Values(form.Value)
	quality := strings.TrimSpace(formValues.Get("quality"))
	if quality == "" || strings.EqualFold(quality, "auto") {
		quality = "medium"
	}
	if err := writer.WriteField("quality", quality); err != nil {
		return err
	}
	for _, key := range []string{"image_size", "output_resolution"} {
		if value := formValues.Get(key); value != "" {
			if err := writer.WriteField(key, value); err != nil {
				return err
			}
		}
	}

	for key, values := range form.Value {
		if key == "model" || key == "async" || key == "n" || key == "quality" || key == "image_size" || key == "output_resolution" {
			continue
		}
		for _, value := range values {
			if err := writer.WriteField(key, value); err != nil {
				return err
			}
		}
	}
	for field, headers := range form.File {
		for _, header := range headers {
			file, err := header.Open()
			if err != nil {
				return err
			}
			if header.Size > maxImageBytes {
				_ = file.Close()
				return fmt.Errorf("multipart file %s exceeds %d MB", header.Filename, maxImageBytes/(1<<20))
			}
			prefix, readErr := io.ReadAll(io.LimitReader(file, 512))
			if readErr != nil {
				_ = file.Close()
				return readErr
			}
			if _, err := file.Seek(0, io.SeekStart); err != nil {
				_ = file.Close()
				return err
			}
			mimeType := header.Header.Get("Content-Type")
			if mimeType == "" || mimeType == "application/octet-stream" {
				mimeType = http.DetectContentType(prefix)
			}
			partHeader := make(textproto.MIMEHeader)
			partHeader.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s"; filename="%s"`, field, header.Filename))
			partHeader.Set("Content-Type", mimeType)
			part, err := writer.CreatePart(partHeader)
			if err != nil {
				_ = file.Close()
				return err
			}
			written, copyErr := io.Copy(part, io.LimitReader(file, maxImageBytes+1))
			closeErr := file.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
			if written > maxImageBytes {
				return fmt.Errorf("multipart file %s exceeds %d MB", header.Filename, maxImageBytes/(1<<20))
			}
		}
	}
	return nil
}

func (a *TaskAdaptor) PrepareLocalTask(c *gin.Context, info *relaycommon.RelayInfo) (*channel.LocalTaskData, bool, error) {
	if !a.isSyncMode(info) {
		return nil, false, nil
	}

	contentType := "application/json"
	payloadFile, err := service.WriteAsyncImagePayload(info.PublicTaskID, func(dst io.Writer) error {
		if strings.HasPrefix(c.GetHeader("Content-Type"), "multipart/form-data") {
			var writeErr error
			contentType, writeErr = a.writeMultipartBody(c, info, false, dst)
			return writeErr
		}
		bodyReader, buildErr := a.buildRequestBody(c, info, false)
		if buildErr != nil {
			return buildErr
		}
		_, copyErr := io.Copy(dst, bodyReader)
		return copyErr
	})
	if err != nil {
		return nil, true, fmt.Errorf("persist local task payload: %w", err)
	}
	return &channel.LocalTaskData{PayloadFile: payloadFile, ContentType: contentType}, true, nil
}

func (a *TaskAdaptor) isSyncMode(info *relaycommon.RelayInfo) bool {
	return info != nil && info.ChannelOtherSettings.GetImageTaskMode() == constant.TaskImageUpstreamModeSync
}

func (a *TaskAdaptor) ExecuteLocalTask(ctx context.Context, task *model.Task, ch *model.Channel) (*relaycommon.TaskInfo, []byte, error) {
	if task == nil || ch == nil {
		return nil, nil, fmt.Errorf("local image task or channel is nil")
	}
	if task.PrivateData.RequestPayloadFile == "" && len(task.PrivateData.RequestBody) == 0 {
		return nil, nil, fmt.Errorf("local image task request body is empty")
	}
	var requestBody io.Reader
	var contentLength int64
	if task.PrivateData.RequestPayloadFile != "" {
		file, err := service.OpenAsyncImagePayload(task.PrivateData.RequestPayloadFile)
		if err != nil {
			return nil, nil, err
		}
		defer file.Close()
		stat, err := file.Stat()
		if err != nil {
			return nil, nil, fmt.Errorf("stat local image task request body: %w", err)
		}
		requestBody = file
		contentLength = stat.Size()
	} else {
		requestBody = bytes.NewReader(task.PrivateData.RequestBody)
		contentLength = int64(len(task.PrivateData.RequestBody))
	}

	baseURL := ch.GetBaseURL()
	if baseURL == "" {
		baseURL = constant.ChannelBaseURLs[ch.Type]
	}
	path := "/v1/images/generations"
	if task.Action == constant.TaskActionImageEdit {
		path = "/v1/images/edits"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, buildEndpoint(baseURL, path), requestBody)
	if err != nil {
		return nil, nil, err
	}
	req.ContentLength = contentLength
	key := ch.Key
	if task.PrivateData.Key != "" {
		key = task.PrivateData.Key
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	contentType := task.PrivateData.RequestContentType
	if contentType == "" {
		contentType = "application/json"
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Accept", "application/json")
	client, err := service.GetHttpClientWithProxy(ch.GetSetting().Proxy)
	if err != nil {
		return nil, nil, fmt.Errorf("new proxy http client failed: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		if isRetryableSynchronousImageTransportError(err) {
			return nil, nil, &service.RetryableLocalTaskError{Err: err}
		}
		return nil, nil, err
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		if isRetryableSynchronousImageTransportError(err) {
			return nil, nil, &service.RetryableLocalTaskError{Err: err}
		}
		return nil, nil, err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		statusErr := fmt.Errorf("upstream image request failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
		if resp.StatusCode == http.StatusGatewayTimeout {
			return nil, responseBody, &service.RetryableLocalTaskError{Err: statusErr}
		}
		return nil, responseBody, statusErr
	}
	responseBody, err = service.RewriteImageResponseURLs(responseBody, ch.GetOtherSettings())
	if err != nil {
		return nil, responseBody, fmt.Errorf("rewrite synchronous image response URLs: %w", err)
	}

	var response imageTaskResponse
	if err := common.Unmarshal(responseBody, &response); err != nil {
		return nil, responseBody, fmt.Errorf("decode synchronous image response: %w", err)
	}
	if response.Error != nil {
		return nil, responseBody, fmt.Errorf("upstream image request failed: %s", response.Error.Message)
	}
	if len(response.Data) == 0 {
		return nil, responseBody, fmt.Errorf("upstream synchronous image response has no data")
	}
	return &relaycommon.TaskInfo{
		TaskID:   task.TaskID,
		Status:   string(model.TaskStatusSuccess),
		Progress: taskcommon.ProgressComplete,
		Url:      response.Data[0].Url,
	}, responseBody, nil
}

func isRetryableSynchronousImageTransportError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return true
	}

	message := strings.ToLower(err.Error())
	for _, fragment := range []string{
		"broken pipe",
		"connection aborted",
		"connection closed",
		"connection reset",
		"server closed idle connection",
		"use of closed connection",
		"use of closed network connection",
	} {
		if strings.Contains(message, fragment) {
			return true
		}
	}
	return false
}

func (a *TaskAdaptor) DoRequest(c *gin.Context, info *relaycommon.RelayInfo, requestBody io.Reader) (*http.Response, error) {
	resp, err := channel.DoTaskApiRequest(a, c, info, requestBody)
	if err != nil {
		if closer, ok := requestBody.(io.Closer); ok {
			_ = closer.Close()
		}
	}
	return resp, err
}

func (a *TaskAdaptor) DoResponse(c *gin.Context, resp *http.Response, info *relaycommon.RelayInfo) (string, []byte, *dto.TaskError) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", nil, taskError(err, "read_response_body_failed")
	}
	_ = resp.Body.Close()

	var upstream imageTaskResponse
	if err := common.Unmarshal(body, &upstream); err != nil {
		return "", nil, taskError(err, "invalid_upstream_response")
	}
	taskID := strings.TrimSpace(upstream.ID)
	if taskID == "" {
		taskID = strings.TrimSpace(upstream.TaskID)
	}
	if taskID == "" {
		return "", nil, taskError(fmt.Errorf("upstream response has no task id"), "invalid_upstream_response")
	}

	publicTask := dto.NewOpenAIImageTask(info.PublicTaskID, info.OriginModelName)
	if upstream.Status != "" {
		publicTask.Status = normalizePublicStatus(upstream.Status)
	}
	if progress := parseProgress(upstream.Progress); progress != "" {
		publicTask.Progress = progress
	}
	if upstream.Error != nil {
		publicTask.Error = &dto.ImageTaskError{Message: upstream.Error.Message, Code: upstream.Error.Code}
	}
	c.JSON(http.StatusOK, publicTask)
	return taskID, body, nil
}

func (a *TaskAdaptor) FetchTask(baseURL, key string, body map[string]any, proxy string) (*http.Response, error) {
	taskID, ok := body["task_id"].(string)
	if !ok || strings.TrimSpace(taskID) == "" {
		return nil, fmt.Errorf("invalid task_id")
	}
	action, _ := body["action"].(string)
	escapedTaskID := url.PathEscape(taskID)
	path := "/v1/images/generations/" + escapedTaskID
	if action == constant.TaskActionImageEdit {
		path = "/v1/images/edits/" + escapedTaskID
	}
	req, err := http.NewRequest(http.MethodGet, buildEndpoint(baseURL, path), nil)
	if err != nil {
		return nil, err
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	req.Header.Set("Accept", "application/json")
	client, err := service.GetHttpClientWithProxy(proxy)
	if err != nil {
		return nil, fmt.Errorf("new proxy http client failed: %w", err)
	}
	return client.Do(req)
}

func (a *TaskAdaptor) ParseTaskResult(body []byte) (*relaycommon.TaskInfo, error) {
	var response imageTaskResponse
	if err := common.Unmarshal(body, &response); err != nil {
		return nil, err
	}
	result := &relaycommon.TaskInfo{
		TaskID:   response.ID,
		Status:   normalizeInternalStatus(response.Status),
		Progress: parseProgress(response.Progress),
	}
	if result.TaskID == "" {
		result.TaskID = response.TaskID
	}
	if response.Error != nil {
		result.Reason = response.Error.Message
		result.Status = string(model.TaskStatusFailure)
	}
	if len(response.Data) > 0 {
		result.Url = response.Data[0].Url
	}
	if result.Status == "" {
		result.Status = string(model.TaskStatusInProgress)
	}
	if result.Progress == "" {
		result.Progress = defaultProgress(result.Status)
	}
	return result, nil
}

func (a *TaskAdaptor) GetModelList() []string {
	// Async image model names are configured per channel and are intentionally
	// not restricted to one hard-coded model.
	return nil
}

func (a *TaskAdaptor) GetChannelName() string {
	return "async-image"
}

func validateRequest(c *gin.Context, info *relaycommon.RelayInfo) error {
	contentType := c.GetHeader("Content-Type")
	if strings.HasPrefix(contentType, "multipart/form-data") {
		return validateMultipartRequest(c, info)
	}
	storage, err := common.GetBodyStorage(c)
	if err != nil {
		return err
	}
	body, err := storage.Bytes()
	if err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := common.Unmarshal(body, &fields); err != nil {
		return err
	}
	if _, err := resolveModel(info, fields); err != nil {
		return err
	}
	prompt, _ := rawString(fields["prompt"])
	if strings.TrimSpace(prompt) == "" {
		return fmt.Errorf("prompt is required")
	}
	if n, ok := rawInt(fields["n"]); ok && n != 1 {
		return fmt.Errorf("n must be 1")
	}
	quality, _ := rawString(fields["quality"])
	if quality != "" && !strings.EqualFold(quality, "auto") && quality != "low" && quality != "medium" && quality != "high" {
		return fmt.Errorf("quality must be low, medium, high, or auto")
	}
	if err := validateJSONReferences(fields); err != nil {
		return err
	}
	return nil
}

func resolveModel(info *relaycommon.RelayInfo, fields map[string]json.RawMessage) (string, error) {
	if model := configuredModel(info); model != "" {
		return model, nil
	}
	if model, ok := rawString(fields["model"]); ok && strings.TrimSpace(model) != "" {
		return strings.TrimSpace(model), nil
	}
	return "", fmt.Errorf("model is required")
}

func resolveMultipartModel(info *relaycommon.RelayInfo, fields map[string][]string) (string, error) {
	if model := configuredModel(info); model != "" {
		return model, nil
	}
	if model := strings.TrimSpace(url.Values(fields).Get("model")); model != "" {
		return model, nil
	}
	return "", fmt.Errorf("model is required")
}

func configuredModel(info *relaycommon.RelayInfo) string {
	if info == nil || info.ChannelMeta == nil {
		return ""
	}
	return strings.TrimSpace(info.UpstreamModelName)
}

func validateMultipartRequest(c *gin.Context, info *relaycommon.RelayInfo) error {
	form, err := imageMultipartForm(c)
	if err != nil {
		return err
	}
	formValues := url.Values(form.Value)
	if _, err := resolveMultipartModel(info, form.Value); err != nil {
		return err
	}
	if strings.TrimSpace(formValues.Get("prompt")) == "" {
		return fmt.Errorf("prompt is required")
	}
	if n := strings.TrimSpace(formValues.Get("n")); n != "" && n != "1" {
		return fmt.Errorf("n must be 1")
	}
	quality := strings.ToLower(strings.TrimSpace(formValues.Get("quality")))
	if quality != "" && quality != "auto" && quality != "low" && quality != "medium" && quality != "high" {
		return fmt.Errorf("quality must be low, medium, high, or auto")
	}
	for _, key := range []string{"image", "images", "mask"} {
		for _, header := range form.File[key] {
			if header.Size > maxImageBytes {
				return fmt.Errorf("multipart file %s exceeds %d MB", header.Filename, maxImageBytes/(1<<20))
			}
		}
	}
	if len(form.File["image"])+len(form.File["images"]) > 9 {
		return fmt.Errorf("at most 9 multipart input images are supported")
	}
	return validateMultipartMask(form)
}

func imageMultipartForm(c *gin.Context) (*multipart.Form, error) {
	if c.Request.MultipartForm != nil {
		return c.Request.MultipartForm, nil
	}
	if _, err := common.GetBodyStorageOnDisk(c); err != nil {
		return nil, err
	}
	form, err := common.ParseMultipartFormReusable(c)
	if err != nil {
		return nil, err
	}
	// net/http removes MultipartForm temporary files when the request ends.
	c.Request.MultipartForm = form
	return form, nil
}

func validateJSONReferences(fields map[string]json.RawMessage) error {
	aliases := []string{"image", "images", "imageUrls", "image_urls", "reference_images", "referenceImages", "image_refs"}
	unique := make(map[string]struct{})
	for _, alias := range aliases {
		value := fields[alias]
		if len(value) == 0 || string(value) == "null" {
			continue
		}
		var single string
		if err := common.Unmarshal(value, &single); err == nil {
			if err := addReference(unique, single); err != nil {
				return err
			}
			continue
		}
		var multiple []string
		if err := common.Unmarshal(value, &multiple); err != nil {
			return fmt.Errorf("%s must be a URL/data URI or a list of them", alias)
		}
		for _, reference := range multiple {
			if err := addReference(unique, reference); err != nil {
				return err
			}
		}
	}
	if len(unique) > 9 {
		return fmt.Errorf("at most 9 unique reference images are supported")
	}
	return nil
}

func addReference(unique map[string]struct{}, reference string) error {
	reference = strings.TrimSpace(reference)
	if reference == "" {
		return nil
	}
	if strings.HasPrefix(reference, "data:") {
		parts := strings.SplitN(reference, ",", 2)
		if len(parts) != 2 || !strings.Contains(parts[0], ";base64") {
			return fmt.Errorf("invalid image data URI")
		}
		mimeType := strings.TrimPrefix(strings.SplitN(parts[0], ";", 2)[0], "data:")
		if mimeType != "image/jpeg" && mimeType != "image/png" && mimeType != "image/webp" {
			return fmt.Errorf("reference image data URI must be JPEG, PNG, or WebP")
		}
		decodedLen := base64.StdEncoding.DecodedLen(len(parts[1]))
		if strings.HasSuffix(parts[1], "==") {
			decodedLen -= 2
		} else if strings.HasSuffix(parts[1], "=") {
			decodedLen--
		}
		if decodedLen > maxImageBytes {
			return fmt.Errorf("reference image exceeds %d MB", maxImageBytes/(1<<20))
		}
	} else {
		referenceURL, err := url.Parse(reference)
		if err != nil || (referenceURL.Scheme != "http" && referenceURL.Scheme != "https") || referenceURL.Host == "" {
			return fmt.Errorf("reference image must be an HTTP(S) URL or data URI")
		}
	}
	unique[reference] = struct{}{}
	return nil
}

func validateMultipartMask(form *multipart.Form) error {
	maskFiles := form.File["mask"]
	maskValues := form.Value["mask"]
	if len(maskFiles)+len(maskValues) > 1 {
		return fmt.Errorf("only one mask is supported")
	}
	if len(maskValues) == 1 && strings.TrimSpace(maskValues[0]) != "" {
		maskURL, err := url.Parse(maskValues[0])
		if err != nil || maskURL.Scheme != "https" || maskURL.Host == "" {
			return fmt.Errorf("mask URL must use HTTPS")
		}
	}
	if len(maskFiles) == 0 {
		return nil
	}

	maskConfig, maskHasAlpha, err := readMultipartPNGMetadata(maskFiles[0])
	if err != nil {
		return fmt.Errorf("decode mask PNG: %w", err)
	}
	if !maskHasAlpha {
		return fmt.Errorf("mask PNG must contain an alpha channel")
	}

	inputFiles := form.File["image"]
	if len(inputFiles) == 0 {
		inputFiles = form.File["images"]
	}
	if len(inputFiles) == 0 {
		return nil
	}
	inputConfig, _, err := readMultipartPNGMetadata(inputFiles[0])
	if err != nil {
		return fmt.Errorf("decode first input PNG: %w", err)
	}
	if maskConfig.Width != inputConfig.Width || maskConfig.Height != inputConfig.Height {
		return fmt.Errorf("mask and first input image must have the same dimensions")
	}
	return nil
}

func readMultipartPNGMetadata(header *multipart.FileHeader) (config stdimage.Config, hasAlpha bool, err error) {
	file, err := header.Open()
	if err != nil {
		return config, false, err
	}
	defer file.Close()

	prefix := make([]byte, 512)
	n, readErr := io.ReadFull(file, prefix)
	if readErr != nil && readErr != io.ErrUnexpectedEOF {
		return config, false, readErr
	}
	if http.DetectContentType(prefix[:n]) != "image/png" {
		return config, false, fmt.Errorf("file must use PNG format")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return config, false, err
	}
	config, err = png.DecodeConfig(file)
	if err != nil {
		return config, false, err
	}
	if _, err := file.Seek(8, io.SeekStart); err != nil {
		return config, false, err
	}

	var chunkHeader [8]byte
	for {
		if _, err := io.ReadFull(file, chunkHeader[:]); err != nil {
			return config, false, err
		}
		chunkLength := int64(binary.BigEndian.Uint32(chunkHeader[:4]))
		chunkType := string(chunkHeader[4:])
		if chunkType == "IHDR" {
			var ihdr [13]byte
			if chunkLength != int64(len(ihdr)) {
				return config, false, fmt.Errorf("invalid IHDR chunk")
			}
			if _, err := io.ReadFull(file, ihdr[:]); err != nil {
				return config, false, err
			}
			if ihdr[9] == 4 || ihdr[9] == 6 {
				return config, true, nil
			}
			if _, err := file.Seek(4, io.SeekCurrent); err != nil {
				return config, false, err
			}
			continue
		}
		if chunkType == "tRNS" {
			return config, true, nil
		}
		if chunkType == "IDAT" || chunkType == "IEND" {
			return config, false, nil
		}
		if _, err := file.Seek(chunkLength+4, io.SeekCurrent); err != nil {
			return config, false, err
		}
	}
}

func rawString(value json.RawMessage) (string, bool) {
	if len(value) == 0 {
		return "", false
	}
	var result string
	if err := common.Unmarshal(value, &result); err != nil {
		return "", false
	}
	return result, true
}

func rawInt(value json.RawMessage) (int, bool) {
	if len(value) == 0 {
		return 0, false
	}
	var result int
	if err := common.Unmarshal(value, &result); err != nil {
		return 0, false
	}
	return result, true
}

func taskError(err error, code string) *dto.TaskError {
	return service.TaskErrorWrapperLocal(err, code, http.StatusBadRequest)
}

func parseProgress(value json.RawMessage) string {
	if len(value) == 0 {
		return ""
	}
	var text string
	if err := common.Unmarshal(value, &text); err == nil && text != "" {
		if strings.HasSuffix(text, "%") {
			return text
		}
		return text + "%"
	}
	var number int
	if err := common.Unmarshal(value, &number); err == nil {
		return strconv.Itoa(number) + "%"
	}
	return ""
}

func normalizeInternalStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "queued", "pending", "submitted":
		return string(model.TaskStatusQueued)
	case "in_progress", "processing", "running":
		return string(model.TaskStatusInProgress)
	case "completed", "success", "succeeded", "done":
		return string(model.TaskStatusSuccess)
	case "failed", "failure", "error", "cancelled", "canceled":
		return string(model.TaskStatusFailure)
	default:
		return ""
	}
}

func normalizePublicStatus(status string) string {
	switch normalizeInternalStatus(status) {
	case string(model.TaskStatusQueued):
		return dto.ImageTaskStatusQueued
	case string(model.TaskStatusInProgress):
		return dto.ImageTaskStatusInProgress
	case string(model.TaskStatusSuccess):
		return dto.ImageTaskStatusCompleted
	case string(model.TaskStatusFailure):
		return dto.ImageTaskStatusFailed
	default:
		return dto.ImageTaskStatusInProgress
	}
}

func defaultProgress(status string) string {
	switch status {
	case string(model.TaskStatusQueued):
		return "10%"
	case string(model.TaskStatusSuccess), string(model.TaskStatusFailure):
		return "100%"
	default:
		return "50%"
	}
}

func buildEndpoint(baseURL, path string) string {
	baseURL = strings.TrimRight(baseURL, "/")
	if strings.HasSuffix(baseURL, "/v1") && strings.HasPrefix(path, "/v1/") {
		path = strings.TrimPrefix(path, "/v1")
	}
	return baseURL + path
}
