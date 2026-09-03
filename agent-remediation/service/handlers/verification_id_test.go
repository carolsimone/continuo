package handlers

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVerificationRunID_ReadableAndBounded(t *testing.T) {
	assert.Equal(t, "verify-rel-1-service-2-a1", VerificationRunID("rel-1", "service-2", 1))
	long := VerificationRunID(strings.Repeat("r", 60), "svc", 3)
	assert.LessOrEqual(t, len(long), 52)
	assert.True(t, strings.HasPrefix(long, "verify-"))
	assert.True(t, strings.HasSuffix(long, "-a3"))
	assert.NotEqual(t, VerificationRunID(strings.Repeat("r", 60), "svc", 3), VerificationRunID(strings.Repeat("r", 59)+"x", "svc", 3))
}

// TestVerificationRunID_Prefix pins the exact id minted for a short
// (release, service, attempt) triple, so the prefix and digest scheme stay
// legible on the shortest, most common inputs.
func TestVerificationRunID_Prefix(t *testing.T) {
	assert.Equal(t, "verify-rel-1-core-a2", VerificationRunID("rel-1", "core", 2))
}

// TestVerificationRunID_SeparatesServicesAndAttempts pins what the id has to
// keep apart: two services edited by one attempt get their own verification
// run, and so does each attempt of one service, since each mints its own
// candidate schema from the id.
func TestVerificationRunID_SeparatesServicesAndAttempts(t *testing.T) {
	assert.NotEqual(t, VerificationRunID("r1", "svc", 1), VerificationRunID("r1", "other", 1))
	assert.NotEqual(t, VerificationRunID("r1", "svc", 1), VerificationRunID("r1", "svc", 2))
	assert.Equal(t, VerificationRunID("r1", "svc", 1), VerificationRunID("r1", "svc", 1),
		"a redelivery of one attempt must mint the id release-controller already accepted")
}

// TestVerificationRunID_ReplacesCharactersAnIDCannotCarry proves the run id
// and the S3 keys derived from it stay legal when the release or the service
// name carries a character neither can hold.
func TestVerificationRunID_ReplacesCharactersAnIDCannotCarry(t *testing.T) {
	assert.Equal(t, "verify-rel-1-svc-a-b-a1", VerificationRunID("rel/1", "svc a+b", 1))
}
