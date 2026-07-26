package service

import (
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/dto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRewriteImageResponseURLsPreservesPayloadAndRewritesMatches(t *testing.T) {
	body := []byte(`{
		"id":"task_upstream",
		"status":"completed",
		"data":[
			{"url":"https://ig.kcai.asia/generated/first.png?download=1","revised_prompt":"first","provider_metadata":{"seed":42}},
			{"url":"https://other.example.com/generated/second.png"}
		]
	}`)
	settings := dto.ChannelOtherSettings{
		ImageURLSourcePrefix: "https://ig.kcai.asia/",
		ImageURLTargetPrefix: "https://mianyunai.com/",
	}

	rewritten, err := RewriteImageResponseURLs(body, settings)
	require.NoError(t, err)

	var payload struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		Data   []struct {
			dto.ImageData
			ProviderMetadata struct {
				Seed int `json:"seed"`
			} `json:"provider_metadata"`
		} `json:"data"`
	}
	require.NoError(t, common.Unmarshal(rewritten, &payload))
	assert.Equal(t, "task_upstream", payload.ID)
	assert.Equal(t, "completed", payload.Status)
	require.Len(t, payload.Data, 2)
	assert.Equal(t, "https://mianyunai.com/generated/first.png?download=1", payload.Data[0].Url)
	assert.Equal(t, "first", payload.Data[0].RevisedPrompt)
	assert.Equal(t, 42, payload.Data[0].ProviderMetadata.Seed)
	assert.Equal(t, "https://other.example.com/generated/second.png", payload.Data[1].Url)
}

func TestRewriteImageURLRequiresPrefixBoundary(t *testing.T) {
	settings := dto.ChannelOtherSettings{
		ImageURLSourcePrefix: "https://ig.kcai.asia",
		ImageURLTargetPrefix: "https://mianyunai.com",
	}

	assert.Equal(t, "https://mianyunai.com", settings.RewriteImageURL("https://ig.kcai.asia"))
	assert.Equal(t, "https://ig.kcai.asia.example/generated/a.png", settings.RewriteImageURL("https://ig.kcai.asia.example/generated/a.png"))
}
