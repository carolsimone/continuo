package output

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmitSuccess_WritesOneJSONObjectWithNewline(t *testing.T) {
	var buf bytes.Buffer
	payload := map[string]string{"schedule_id": "sched_123", "schedule_name": "daily_ingest"}

	require.NoError(t, EmitSuccess(&buf, payload))

	assert.Equal(t, byte('\n'), buf.Bytes()[buf.Len()-1])
	var got map[string]string
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &got))
	assert.Equal(t, payload, got)
}

func TestEmitError_WrapsUnderErrorKey(t *testing.T) {
	var buf bytes.Buffer
	cliErr := CLIError{Code: CodeConflict, Message: "already running", Retryable: true}

	require.NoError(t, EmitError(&buf, cliErr))

	var envelope struct {
		Error CLIError `json:"error"`
	}
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &envelope))
	assert.Equal(t, cliErr, envelope.Error)
}
