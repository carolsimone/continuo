// Package commandrequest is the record of a command that creates or cancels a
// run, kept so the same request id returns the same run when a caller repeats
// the command.
package commandrequest

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/google/uuid"
)

// ErrReused reports a request id used again for a command with other
// parameters than the first call's.
var ErrReused = errors.New("request_id reused with different parameters")

// Record is the first call of a command under a request id: the tenant that
// made it, the id, the command's name, the fingerprint of its parameters, and
// the run it created or cancelled.
type Record struct {
	TenantID    string
	RequestID   uuid.UUID
	Command     string
	Fingerprint string
	RunID       uuid.UUID
	CreatedAt   time.Time
}

// Fingerprint returns the hex SHA-256 of command and then each part, each
// followed by a NUL byte, so the same characters split into other parts hash
// differently. The parts are the parsed command's parameters with their
// defaults applied, never the caller's identity.
func Fingerprint(command string, parts ...string) string {
	h := sha256.New()
	h.Write([]byte(command))
	h.Write([]byte{0})
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}
