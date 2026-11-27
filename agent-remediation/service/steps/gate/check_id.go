package gate

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// maxCheckIDLen bounds the minted id so every name derived from it downstream
// stays legal. The tightest consumer is the candidate schema release-controller
// creates for the check, built as "_candidate_" followed by the check id with every
// character outside [A-Za-z0-9_] replaced one-for-one, so the schema is exactly
// 11 bytes longer than the id. PostgreSQL truncates an identifier past 63 bytes
// rather than rejecting it, and what it cuts is the tail: the try suffix,
// then the project name. Two tries at one project would then share a schema
// and each would validate against the other's leftovers, so the id is bounded
// here instead.
const maxCheckIDLen = 63 - len("_candidate_")

// checkIDPrefix marks an id as a fix's release check at a glance, in log lines and
// the release check page.
const checkIDPrefix = "verify-"

// CheckID mints the id of the release check that verifies one try's edits
// to one project. It embeds the failing release, the edited project, and the
// try's round and number (-r<round>-t<try>), so it is unique per (project,
// round, try) and legible in log lines and the
// release check page, and so a redelivery of the same try reuses the same id
// (release-controller's submission is idempotent on it).
//
// Past maxCheckIDLen the middle, the failing release and project, is shortened
// and given a digest of what it held. The prefix and the round and try are
// kept whole because they are what a reader identifies the check by, and the
// digest is what keeps two projects whose names diverge only past the cut on
// separate candidate schemas.
func CheckID(releaseID, project string, round, try int) string {
	middle := sanitizeIDSegment(releaseID) + "-" + sanitizeIDSegment(project)
	suffix := fmt.Sprintf("-r%d-t%d", round, try)
	if len(checkIDPrefix)+len(middle)+len(suffix) <= maxCheckIDLen {
		return checkIDPrefix + middle + suffix
	}
	digest := sha256.Sum256([]byte(middle))
	tail := hex.EncodeToString(digest[:4]) // 8 hex chars
	// Reserve the digest and its separator before cutting, so shortening eats
	// the readable head rather than the part that restores uniqueness.
	budget := max(maxCheckIDLen-len(checkIDPrefix)-len(suffix)-len(tail)-1, 0)
	return checkIDPrefix + strings.TrimRight(middle[:budget], "-") + "-" + tail + suffix
}

// sanitizeIDSegment reduces a value to the characters a check id and the S3 key
// derived from it can safely carry, replacing every other character with a
// dash. Dots, dashes, and underscores survive, so a project name reads
// unchanged.
func sanitizeIDSegment(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == '.', r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, s)
}
