package handlers

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShadowReleaseID_ReadableAndBounded(t *testing.T) {
	assert.Equal(t, "shadow-rel-1-service-2-a1", ShadowReleaseID("rel-1", "service-2", 1))
	long := ShadowReleaseID(strings.Repeat("r", 60), "svc", 3)
	assert.LessOrEqual(t, len(long), 52)
	assert.True(t, strings.HasPrefix(long, "shadow-"))
	assert.True(t, strings.HasSuffix(long, "-a3"))
	assert.NotEqual(t, ShadowReleaseID(strings.Repeat("r", 60), "svc", 3), ShadowReleaseID(strings.Repeat("r", 59)+"x", "svc", 3))
}

// TestShadowReleaseID_SeparatesServicesAndAttempts pins what the id has to keep
// apart: two services edited by one attempt get their own shadow release, and
// so does each attempt of one service, since each mints its own candidate
// schema from the id.
func TestShadowReleaseID_SeparatesServicesAndAttempts(t *testing.T) {
	assert.NotEqual(t, ShadowReleaseID("r1", "svc", 1), ShadowReleaseID("r1", "other", 1))
	assert.NotEqual(t, ShadowReleaseID("r1", "svc", 1), ShadowReleaseID("r1", "svc", 2))
	assert.Equal(t, ShadowReleaseID("r1", "svc", 1), ShadowReleaseID("r1", "svc", 1),
		"a redelivery of one attempt must mint the id release-controller already accepted")
}

// TestShadowReleaseID_ReplacesCharactersAnIDCannotCarry proves the release id
// and the S3 keys derived from it stay legal when the release or the service
// name carries a character neither can hold.
func TestShadowReleaseID_ReplacesCharactersAnIDCannotCarry(t *testing.T) {
	assert.Equal(t, "shadow-rel-1-svc-a-b-a1", ShadowReleaseID("rel/1", "svc a+b", 1))
}
