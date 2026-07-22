package dto

import "time"

const (
	ImageTaskStatusQueued     = "queued"
	ImageTaskStatusInProgress = "in_progress"
	ImageTaskStatusCompleted  = "completed"
	ImageTaskStatusFailed     = "failed"
)

// OpenAIImageTask is the asynchronous image-generation response exposed by
// NewAPI. It intentionally mirrors the upstream image task contract while
// keeping the public ID separate from the provider task ID.
type OpenAIImageTask struct {
	ID        string          `json:"id"`
	Model     string          `json:"model"`
	Object    string          `json:"object"`
	Progress  string          `json:"progress"`
	Status    string          `json:"status"`
	CreatedAt int64           `json:"created_at,omitempty"`
	Data      []ImageData     `json:"data,omitempty"`
	Error     *ImageTaskError `json:"error,omitempty"`
}

type ImageTaskError struct {
	Message string `json:"message"`
	Code    string `json:"code,omitempty"`
}

func NewOpenAIImageTask(id, model string) *OpenAIImageTask {
	return &OpenAIImageTask{
		ID:        id,
		Model:     model,
		Object:    "image.generation",
		Progress:  "10%",
		Status:    ImageTaskStatusQueued,
		CreatedAt: time.Now().Unix(),
	}
}
