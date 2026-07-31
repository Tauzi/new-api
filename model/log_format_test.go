package model

import (
	"testing"

	"github.com/QuantumNous/new-api/common"

	"github.com/stretchr/testify/require"
)

func TestFormatUserLogsStripsAdminOnlyFields(t *testing.T) {
	other := common.MapToJsonStr(map[string]interface{}{
		"model_price":         0.004,
		"is_model_mapped":     true,
		"upstream_model_name": "provider-secret-model",
		"admin_info": map[string]interface{}{
			"quota_saturation": map[string]interface{}{
				"op":      "QuotaFromDecimal",
				"kind":    "overflow",
				"clamped": common.MaxQuota,
			},
		},
	})
	logs := []*Log{{Other: other}}

	formatUserLogs(logs, 0)

	parsed, err := common.StrToMap(logs[0].Other)
	require.NoError(t, err)
	_, hasAdminInfo := parsed["admin_info"]
	require.False(t, hasAdminInfo, "admin_info (and nested quota_saturation) must be stripped for non-admin views")
	require.NotContains(t, parsed, "is_model_mapped")
	require.NotContains(t, parsed, "upstream_model_name")
	// Non-admin billing fields remain visible.
	require.Contains(t, parsed, "model_price")
}
