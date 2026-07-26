package service

import (
	"encoding/json"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/dto"
)

// RewriteImageResponseURLs applies a channel's prefix rule to every URL in an
// OpenAI-compatible image response while preserving all unknown response fields.
func RewriteImageResponseURLs(body []byte, settings dto.ChannelOtherSettings) ([]byte, error) {
	if settings.ImageURLSourcePrefix == "" || settings.ImageURLTargetPrefix == "" {
		return body, nil
	}

	var payload map[string]json.RawMessage
	if err := common.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	rawData, ok := payload["data"]
	if !ok {
		return body, nil
	}

	var data []map[string]json.RawMessage
	if err := common.Unmarshal(rawData, &data); err != nil {
		return nil, err
	}
	changed := false
	for i := range data {
		rawURL, ok := data[i]["url"]
		if !ok {
			continue
		}
		var imageURL string
		if err := common.Unmarshal(rawURL, &imageURL); err != nil {
			return nil, err
		}
		rewrittenURL := settings.RewriteImageURL(imageURL)
		if rewrittenURL == imageURL {
			continue
		}
		rewrittenRawURL, err := common.Marshal(rewrittenURL)
		if err != nil {
			return nil, err
		}
		data[i]["url"] = rewrittenRawURL
		changed = true
	}
	if !changed {
		return body, nil
	}

	rewrittenData, err := common.Marshal(data)
	if err != nil {
		return nil, err
	}
	payload["data"] = rewrittenData
	return common.Marshal(payload)
}
