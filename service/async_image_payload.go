package service

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/QuantumNous/new-api/constant"
)

func asyncImagePayloadPath(filename string) (string, error) {
	if filename == "" || filename == "." || filename == ".." || filepath.Base(filename) != filename {
		return "", fmt.Errorf("invalid async image payload filename")
	}
	queueDir := strings.TrimSpace(constant.AsyncImageQueueDir)
	if queueDir == "" {
		queueDir = "async-image-queue"
	}
	return filepath.Join(queueDir, filename), nil
}

// WriteAsyncImagePayload commits a worker payload atomically and returns only
// its safe basename for persistence in task private_data.
func WriteAsyncImagePayload(taskID string, write func(io.Writer) error) (string, error) {
	if taskID == "" || filepath.Base(taskID) != taskID {
		return "", fmt.Errorf("invalid async image task id")
	}
	queueDir := strings.TrimSpace(constant.AsyncImageQueueDir)
	if queueDir == "" {
		queueDir = "async-image-queue"
	}
	if err := os.MkdirAll(queueDir, 0o700); err != nil {
		return "", fmt.Errorf("create async image queue directory: %w", err)
	}
	if err := os.Chmod(queueDir, 0o700); err != nil {
		return "", fmt.Errorf("protect async image queue directory: %w", err)
	}

	tmp, err := os.CreateTemp(queueDir, taskID+"-*.tmp")
	if err != nil {
		return "", fmt.Errorf("create async image payload: %w", err)
	}
	tmpName := tmp.Name()
	closed := false
	defer func() {
		if !closed {
			_ = tmp.Close()
		}
		_ = os.Remove(tmpName)
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return "", fmt.Errorf("protect async image payload: %w", err)
	}
	if err := write(tmp); err != nil {
		return "", err
	}
	if err := tmp.Sync(); err != nil {
		return "", fmt.Errorf("sync async image payload: %w", err)
	}
	if err := tmp.Close(); err != nil {
		closed = true
		return "", fmt.Errorf("close async image payload: %w", err)
	}
	closed = true

	filename := strings.TrimSuffix(filepath.Base(tmpName), ".tmp") + ".payload"
	finalPath, err := asyncImagePayloadPath(filename)
	if err != nil {
		return "", err
	}
	if err := os.Rename(tmpName, finalPath); err != nil {
		return "", fmt.Errorf("commit async image payload: %w", err)
	}
	return filename, nil
}

func OpenAsyncImagePayload(filename string) (*os.File, error) {
	path, err := asyncImagePayloadPath(filename)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open async image payload: %w", err)
	}
	return file, nil
}

func RemoveAsyncImagePayload(filename string) error {
	if filename == "" {
		return nil
	}
	path, err := asyncImagePayloadPath(filename)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove async image payload: %w", err)
	}
	return nil
}
