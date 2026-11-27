package gate_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/agent-remediation/service/steps/gate"
)

func TestCheckID_ReadableAndBounded(t *testing.T) {
	assert.Equal(t, "verify-rel-1-service-2-r1-t1", gate.CheckID("rel-1", "service-2", 1, 1))
	long := gate.CheckID(strings.Repeat("r", 60), "svc", 2, 3)
	assert.LessOrEqual(t, len(long), 52)
	assert.True(t, strings.HasPrefix(long, "verify-"))
	assert.True(t, strings.HasSuffix(long, "-r2-t3"))
	assert.NotEqual(t, gate.CheckID(strings.Repeat("r", 60), "svc", 2, 3), gate.CheckID(strings.Repeat("r", 59)+"x", "svc", 2, 3))
}

// TestCheckID_Prefix pins the exact id minted for a short (release, project,
// round, try).
func TestCheckID_Prefix(t *testing.T) {
	assert.Equal(t, "verify-rel-1-core-r3-t2", gate.CheckID("rel-1", "core", 3, 2))
}

// TestCheckID_SeparatesProjectsAndTries pins what the id has to keep apart: two
// projects edited by one try get their own release check, and so does
// each try of one project and the same try number of each round, since each
// mints its own candidate schema from the id.
func TestCheckID_SeparatesProjectsAndTries(t *testing.T) {
	assert.NotEqual(t, gate.CheckID("r1", "svc", 1, 1), gate.CheckID("r1", "other", 1, 1))
	assert.NotEqual(t, gate.CheckID("r1", "svc", 1, 1), gate.CheckID("r1", "svc", 1, 2))
	assert.NotEqual(t, gate.CheckID("r1", "svc", 1, 1), gate.CheckID("r1", "svc", 2, 1))
	assert.Equal(t, gate.CheckID("r1", "svc", 1, 1), gate.CheckID("r1", "svc", 1, 1),
		"a redelivery of one try must mint the id release-controller already accepted")
}

// TestCheckID_ReplacesCharactersAnIDCannotCarry proves the check id and the S3 keys
// derived from it stay legal when the release or the project name carries a
// character neither can hold.
func TestCheckID_ReplacesCharactersAnIDCannotCarry(t *testing.T) {
	assert.Equal(t, "verify-rel-1-svc-a-b-r1-t1", gate.CheckID("rel/1", "svc a+b", 1, 1))
}
