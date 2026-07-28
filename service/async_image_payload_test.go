package service

import (
	"errors"
	"io"
	"os"
	"testing"

	"github.com/QuantumNous/new-api/constant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAsyncImagePayloadLifecycle(t *testing.T) {
	previous := constant.AsyncImageQueueDir
	constant.AsyncImageQueueDir = t.TempDir()
	t.Cleanup(func() { constant.AsyncImageQueueDir = previous })

	filename, err := WriteAsyncImagePayload("task_payload", func(dst io.Writer) error {
		_, writeErr := io.WriteString(dst, "multipart payload")
		return writeErr
	})
	require.NoError(t, err)
	assert.NotEmpty(t, filename)

	file, err := OpenAsyncImagePayload(filename)
	require.NoError(t, err)
	body, err := io.ReadAll(file)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	assert.Equal(t, "multipart payload", string(body))

	require.NoError(t, RemoveAsyncImagePayload(filename))
	require.NoError(t, RemoveAsyncImagePayload(filename))
	_, err = OpenAsyncImagePayload(filename)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestAsyncImagePayloadWriteFailureLeavesNoCommittedFile(t *testing.T) {
	previous := constant.AsyncImageQueueDir
	constant.AsyncImageQueueDir = t.TempDir()
	t.Cleanup(func() { constant.AsyncImageQueueDir = previous })

	expected := errors.New("write failed")
	filename, err := WriteAsyncImagePayload("task_failed_payload", func(io.Writer) error {
		return expected
	})
	assert.ErrorIs(t, err, expected)
	assert.Empty(t, filename)

	entries, readErr := os.ReadDir(constant.AsyncImageQueueDir)
	require.NoError(t, readErr)
	assert.Empty(t, entries)
}
