package image

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"image/png"
	"io"
	"mime/multipart"
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
	modelName      = "gpt-image-2-async"
	maxImageBytes  = 10 << 20
	minImagePixels = 655360
	maxImagePixels = 1048576
	maxImageSide   = 3840
	maxImageRatio  = 3.0
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

	if info.UpstreamModelName != "" && info.UpstreamModelName != modelName {
		return taskError(fmt.Errorf("async image model must be %s", modelName), "invalid_model")
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
	storage, err := common.GetBodyStorage(c)
	if err != nil {
		return nil, fmt.Errorf("get request body: %w", err)
	}
	body, err := storage.Bytes()
	if err != nil {
		return nil, fmt.Errorf("read request body: %w", err)
	}

	contentType := c.GetHeader("Content-Type")
	if strings.HasPrefix(contentType, "multipart/form-data") {
		return a.buildMultipartBody(c, info)
	}

	var fields map[string]json.RawMessage
	if err := common.Unmarshal(body, &fields); err != nil {
		return nil, fmt.Errorf("decode image request: %w", err)
	}
	fields["model"] = json.RawMessage(strconv.Quote(modelName))
	fields["async"] = json.RawMessage("true")
	fields["n"] = json.RawMessage("1")
	quality := "medium"
	if value, ok := fields["quality"]; ok {
		var requested string
		if err := common.Unmarshal(value, &requested); err == nil && requested != "" && !strings.EqualFold(requested, "auto") {
			quality = requested
		}
	}
	fields["quality"] = json.RawMessage(strconv.Quote(quality))
	for _, key := range []string{"image_size", "output_resolution"} {
		if value, ok := fields[key]; ok && len(value) > 0 && string(value) != "null" {
			fields[key] = json.RawMessage(strconv.Quote("1K"))
		}
	}
	out, err := common.Marshal(fields)
	if err != nil {
		return nil, err
	}
	c.Set("image_task_content_type", "application/json")
	return bytes.NewReader(out), nil
}

func (a *TaskAdaptor) buildMultipartBody(c *gin.Context, info *relaycommon.RelayInfo) (io.Reader, error) {
	form, err := common.ParseMultipartFormReusable(c)
	if err != nil {
		return nil, fmt.Errorf("parse multipart image request: %w", err)
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("model", modelName); err != nil {
		return nil, err
	}
	if err := writer.WriteField("async", "true"); err != nil {
		return nil, err
	}
	if err := writer.WriteField("n", "1"); err != nil {
		return nil, err
	}
	formValues := url.Values(form.Value)
	quality := strings.TrimSpace(formValues.Get("quality"))
	if quality == "" || strings.EqualFold(quality, "auto") {
		quality = "medium"
	}
	if err := writer.WriteField("quality", quality); err != nil {
		return nil, err
	}
	for _, key := range []string{"image_size", "output_resolution"} {
		if formValues.Get(key) != "" {
			if err := writer.WriteField(key, "1K"); err != nil {
				return nil, err
			}
		}
	}

	for key, values := range form.Value {
		if key == "model" || key == "async" || key == "n" || key == "quality" || key == "image_size" || key == "output_resolution" {
			continue
		}
		for _, value := range values {
			if err := writer.WriteField(key, value); err != nil {
				return nil, err
			}
		}
	}
	for field, headers := range form.File {
		for _, header := range headers {
			file, err := header.Open()
			if err != nil {
				return nil, err
			}
			fileBytes, readErr := io.ReadAll(io.LimitReader(file, maxImageBytes+1))
			_ = file.Close()
			if readErr != nil {
				return nil, readErr
			}
			if int64(len(fileBytes)) > maxImageBytes {
				return nil, fmt.Errorf("multipart file %s exceeds %d MB", header.Filename, maxImageBytes/(1<<20))
			}
			mimeType := header.Header.Get("Content-Type")
			if mimeType == "" || mimeType == "application/octet-stream" {
				mimeType = http.DetectContentType(fileBytes)
			}
			partHeader := make(textproto.MIMEHeader)
			partHeader.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s"; filename="%s"`, field, header.Filename))
			partHeader.Set("Content-Type", mimeType)
			part, err := writer.CreatePart(partHeader)
			if err != nil {
				return nil, err
			}
			if _, err = part.Write(fileBytes); err != nil {
				return nil, err
			}
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	c.Set("image_task_content_type", writer.FormDataContentType())
	_ = info
	return &body, nil
}

func (a *TaskAdaptor) DoRequest(c *gin.Context, info *relaycommon.RelayInfo, requestBody io.Reader) (*http.Response, error) {
	return channel.DoTaskApiRequest(a, c, info, requestBody)
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
	return []string{modelName}
}

func (a *TaskAdaptor) GetChannelName() string {
	return "async-image"
}

func validateRequest(c *gin.Context, info *relaycommon.RelayInfo) error {
	storage, err := common.GetBodyStorage(c)
	if err != nil {
		return err
	}
	body, err := storage.Bytes()
	if err != nil {
		return err
	}
	contentType := c.GetHeader("Content-Type")
	if strings.HasPrefix(contentType, "multipart/form-data") {
		return validateMultipartRequest(c)
	}
	var fields map[string]json.RawMessage
	if err := common.Unmarshal(body, &fields); err != nil {
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
	if size, ok := rawString(fields["size"]); ok && size != "" {
		if err := validateSize(size); err != nil {
			return err
		}
	}
	for _, key := range []string{"image_size", "output_resolution"} {
		if value, ok := rawString(fields[key]); ok && value != "" && !strings.EqualFold(value, "1k") {
			return fmt.Errorf("%s must be 1K", key)
		}
	}
	if err := validateJSONReferences(fields); err != nil {
		return err
	}
	if info.UpstreamModelName != "" && info.UpstreamModelName != modelName {
		return fmt.Errorf("async image model must be %s", modelName)
	}
	return nil
}

func validateMultipartRequest(c *gin.Context) error {
	form, err := common.ParseMultipartFormReusable(c)
	if err != nil {
		return err
	}
	formValues := url.Values(form.Value)
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
	if size := strings.TrimSpace(formValues.Get("size")); size != "" {
		if err := validateSize(size); err != nil {
			return err
		}
	}
	for _, key := range []string{"image_size", "output_resolution"} {
		if value := strings.TrimSpace(formValues.Get(key)); value != "" && !strings.EqualFold(value, "1k") {
			return fmt.Errorf("%s must be 1K", key)
		}
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

	maskBytes, err := readMultipartFile(maskFiles[0])
	if err != nil {
		return err
	}
	if http.DetectContentType(maskBytes) != "image/png" {
		return fmt.Errorf("mask must be a PNG file")
	}
	if !pngHasAlpha(maskBytes) {
		return fmt.Errorf("mask PNG must contain an alpha channel")
	}

	inputFiles := form.File["image"]
	if len(inputFiles) == 0 {
		inputFiles = form.File["images"]
	}
	if len(inputFiles) == 0 {
		return nil
	}
	inputBytes, err := readMultipartFile(inputFiles[0])
	if err != nil {
		return err
	}
	if http.DetectContentType(inputBytes) != "image/png" {
		return fmt.Errorf("mask and first input image must use the same PNG format")
	}
	maskConfig, err := png.DecodeConfig(bytes.NewReader(maskBytes))
	if err != nil {
		return fmt.Errorf("decode mask PNG: %w", err)
	}
	inputConfig, err := png.DecodeConfig(bytes.NewReader(inputBytes))
	if err != nil {
		return fmt.Errorf("decode first input PNG: %w", err)
	}
	if maskConfig.Width != inputConfig.Width || maskConfig.Height != inputConfig.Height {
		return fmt.Errorf("mask and first input image must have the same dimensions")
	}
	return nil
}

func readMultipartFile(header *multipart.FileHeader) ([]byte, error) {
	file, err := header.Open()
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxImageBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxImageBytes {
		return nil, fmt.Errorf("multipart file %s exceeds %d MB", header.Filename, maxImageBytes/(1<<20))
	}
	return data, nil
}

func pngHasAlpha(data []byte) bool {
	// PNG IHDR color types 4 and 6 include alpha. Indexed PNGs may carry
	// transparency in a tRNS chunk.
	if len(data) > 25 && (data[25] == 4 || data[25] == 6) {
		return true
	}
	return bytes.Contains(data, []byte("tRNS"))
}

func validateSize(value string) error {
	if strings.Contains(value, ":") {
		parts := strings.Split(value, ":")
		if len(parts) != 2 {
			return fmt.Errorf("invalid size ratio")
		}
		w, err1 := strconv.ParseFloat(parts[0], 64)
		h, err2 := strconv.ParseFloat(parts[1], 64)
		if err1 != nil || err2 != nil || w <= 0 || h <= 0 || w/h > maxImageRatio || h/w > maxImageRatio {
			return fmt.Errorf("size ratio must not exceed 3:1")
		}
		return nil
	}
	parts := strings.Split(value, "x")
	if len(parts) != 2 {
		return fmt.Errorf("size must be WIDTHxHEIGHT or W:H")
	}
	w, err1 := strconv.Atoi(parts[0])
	h, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || w <= 0 || h <= 0 {
		return fmt.Errorf("invalid size")
	}
	if w%16 != 0 || h%16 != 0 || w > maxImageSide || h > maxImageSide {
		return fmt.Errorf("size dimensions must be 16-aligned and no larger than 3840")
	}
	pixels := w * h
	if pixels < minImagePixels || pixels > maxImagePixels {
		return fmt.Errorf("size pixel count must be between %d and %d", minImagePixels, maxImagePixels)
	}
	if float64(w)/float64(h) > maxImageRatio || float64(h)/float64(w) > maxImageRatio {
		return fmt.Errorf("size ratio must not exceed 3:1")
	}
	return nil
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
