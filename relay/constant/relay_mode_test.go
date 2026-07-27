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

func TestPath2RelayModeAlphaSearch(t *testing.T) {
	tests := []struct {
		path string
		want int
	}{
		{path: "/v1/alpha/search", want: RelayModeAlphaSearch},
		{path: "/v1/alpha/search?foo=1", want: RelayModeAlphaSearch},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			assert.Equal(t, tt.want, Path2RelayMode(tt.path))
		})
	}
}
