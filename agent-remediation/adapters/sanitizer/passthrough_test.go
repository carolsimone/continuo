package sanitizer_test

import (
	"testing"

	"github.com/carolsimone/continuo/agent-remediation/adapters/sanitizer"
	"github.com/stretchr/testify/assert"
)

// The legacy lanes send their input as it is, credentials in it included, so a
// lane that needs a clean text must clean it itself.
func TestPassthrough_ReturnsItsInputUnchanged(t *testing.T) {
	log := "connect postgres://app:s3cr3t@db/x password=hunter2 mail ops@acme.com\n"

	assert.Equal(t, log, sanitizer.Passthrough{}.Sanitize(log))
	assert.Equal(t, "", sanitizer.Passthrough{}.Sanitize(""))
}
