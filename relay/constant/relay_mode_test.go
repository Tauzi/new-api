package constant

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPath2RelayModeAsyncImageFetch(t *testing.T) {
	assert.Equal(t, RelayModeImagesGenerations, Path2RelayMode("/v1/images/generations"))
	assert.Equal(t, RelayModeImagesEdits, Path2RelayMode("/v1/images/edits"))
	assert.Equal(t, RelayModeImageGenerationsFetchByID, Path2RelayMode("/v1/images/generations/task_img_123"))
	assert.Equal(t, RelayModeImageEditsFetchByID, Path2RelayMode("/v1/images/edits/task_img_123"))
}
