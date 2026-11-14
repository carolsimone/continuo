package gate_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/agent-remediation/service/steps/gate"
)

func TestRunID_ReadableAndBounded(t *testing.T) {
	assert.Equal(t, "verify-rel-1-service-2-a1", gate.RunID("rel-1", "service-2", 1))
	long := gate.RunID(strings.Repeat("r", 60), "svc", 3)
	assert.LessOrEqual(t, len(long), 52)
	assert.True(t, strings.HasPrefix(long, "verify-"))
	assert.True(t, strings.HasSuffix(long, "-a3"))
	assert.NotEqual(t, gate.RunID(strings.Repeat("r", 60), "svc", 3), gate.RunID(strings.Repeat("r", 59)+"x", "svc", 3))
}

// TestRunID_Prefix pins the exact id minted for a short (release, service,
// attempt) triple.
func TestRunID_Prefix(t *testing.T) {
	assert.Equal(t, "verify-rel-1-core-a2", gate.RunID("rel-1", "core", 2))
}

// TestRunID_SeparatesServicesAndAttempts pins what the id has to keep apart: two
// services edited by one attempt get their own verification run, and so does
// each attempt of one service, since each mints its own candidate schema from
// the id.
func TestRunID_SeparatesServicesAndAttempts(t *testing.T) {
	assert.NotEqual(t, gate.RunID("r1", "svc", 1), gate.RunID("r1", "other", 1))
	assert.NotEqual(t, gate.RunID("r1", "svc", 1), gate.RunID("r1", "svc", 2))
	assert.Equal(t, gate.RunID("r1", "svc", 1), gate.RunID("r1", "svc", 1),
		"a redelivery of one attempt must mint the id release-controller already accepted")
}

// TestRunID_ReplacesCharactersAnIDCannotCarry proves the run id and the S3 keys
// derived from it stay legal when the release or the service name carries a
// character neither can hold.
func TestRunID_ReplacesCharactersAnIDCannotCarry(t *testing.T) {
	assert.Equal(t, "verify-rel-1-svc-a-b-a1", gate.RunID("rel/1", "svc a+b", 1))
}
