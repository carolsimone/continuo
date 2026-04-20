package output

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHumanSuccess_WritesOneLineToStderr(t *testing.T) {
	var stderr bytes.Buffer
	require.NoError(t, HumanSuccess(&stderr, "Triggered run sched_123 for schedule 'daily_ingest'"))
	assert.Contains(t, stderr.String(), "Triggered run sched_123")
	assert.Equal(t, byte('\n'), stderr.Bytes()[stderr.Len()-1])
}

func TestHumanError_PrefixesErrorLabel(t *testing.T) {
	var stderr bytes.Buffer
	cliErr := CLIError{Code: CodeConflict, Message: "already running", Retryable: true}
	require.NoError(t, HumanError(&stderr, cliErr))
	got := stderr.String()
	assert.Contains(t, got, "Error [conflict]: already running")
}
