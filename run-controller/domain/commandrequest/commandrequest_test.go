package commandrequest_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/run-controller/domain/commandrequest"
	"github.com/stretchr/testify/assert"
)

// The fingerprint is the hex SHA-256 of the command and then each part, each
// followed by a NUL byte.
func TestFingerprint_IsTheSHA256OfTheCommandAndItsPartsEachEndedByNUL(t *testing.T) {
	sum := sha256.Sum256([]byte("trigger_single_node_run\x00service-1.e2e_schema.table_a\x00latest\x00run\x00"))
	want := hex.EncodeToString(sum[:])

	got := commandrequest.Fingerprint("trigger_single_node_run", "service-1.e2e_schema.table_a", "latest", "run")

	assert.Equal(t, want, got)
	assert.Len(t, got, 64)
}

// The same command with the same parts has the same fingerprint, and any
// change of command or part changes it.
func TestFingerprint_ChangesWithTheCommandAndEveryPart(t *testing.T) {
	base := commandrequest.Fingerprint("start_run", "e2e-schedule", "run")

	assert.Equal(t, base, commandrequest.Fingerprint("start_run", "e2e-schedule", "run"))
	assert.NotEqual(t, base, commandrequest.Fingerprint("cancel_active_run", "e2e-schedule", "run"))
	assert.NotEqual(t, base, commandrequest.Fingerprint("start_run", "other-schedule", "run"))
	assert.NotEqual(t, base, commandrequest.Fingerprint("start_run", "e2e-schedule", "test"))
	assert.NotEqual(t, base, commandrequest.Fingerprint("start_run", "e2e-schedule"))
}

// The terminator keeps the boundaries between parts: the same characters split
// differently are different commands.
func TestFingerprint_KeepsTheBoundariesBetweenParts(t *testing.T) {
	assert.NotEqual(t,
		commandrequest.Fingerprint("start_run", "ab", "c"),
		commandrequest.Fingerprint("start_run", "a", "bc"))
	assert.NotEqual(t,
		commandrequest.Fingerprint("start_run", "a", ""),
		commandrequest.Fingerprint("start_run", "a"))
}

func TestErrReused_NamesTheReuse(t *testing.T) {
	assert.True(t, errors.Is(commandrequest.ErrReused, commandrequest.ErrReused))
	assert.Contains(t, commandrequest.ErrReused.Error(), "request_id reused with different parameters")
}
