package handlers

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// maxVerificationRunIDLen bounds the minted id so every name derived from it
// downstream stays legal. The tightest consumer is the candidate schema
// release-controller creates for the run, built as "_candidate_" followed
// by the run id with every character outside [A-Za-z0-9_] replaced
// one-for-one — so the schema is exactly 11 bytes longer than the id.
// PostgreSQL truncates an identifier past 63 bytes rather than rejecting it,
// and what it cuts is the tail: the attempt suffix, then the service name. Two
// attempts at one service would then share a schema and each would validate
// against the other's leftovers, so the id is bounded here instead.
const maxVerificationRunIDLen = 63 - len("_candidate_")

// verificationIDPrefix marks a run as a fix verification at a glance, in log
// lines and the verification page.
const verificationIDPrefix = "verify-"

// VerificationRunID mints the id of the run that verifies one attempt's
// edits to one service. It embeds the failing release, the edited service, and
// the attempt, so it is unique per (service, attempt) and legible in log
// lines and the verification page, and so a redelivery of the same attempt
// reuses the same id — release-controller's submission is idempotent on it.
//
// Past maxVerificationRunIDLen the middle — the failing release and service —
// is shortened and given a digest of what it held. The prefix and the attempt
// number are kept whole because they are what a reader identifies the run
// by, and the digest is what keeps two services whose names diverge only past
// the cut on separate candidate schemas.
func VerificationRunID(releaseID, service string, attempt int) string {
	middle := sanitizeIDSegment(releaseID) + "-" + sanitizeIDSegment(service)
	suffix := fmt.Sprintf("-a%d", attempt)
	if len(verificationIDPrefix)+len(middle)+len(suffix) <= maxVerificationRunIDLen {
		return verificationIDPrefix + middle + suffix
	}
	digest := sha256.Sum256([]byte(middle))
	tail := hex.EncodeToString(digest[:4]) // 8 hex chars
	// Reserve the digest and its separator before cutting, so shortening eats
	// the readable head rather than the part that restores uniqueness.
	budget := maxVerificationRunIDLen - len(verificationIDPrefix) - len(suffix) - len(tail) - 1
	if budget < 0 {
		budget = 0
	}
	return verificationIDPrefix + strings.TrimRight(middle[:budget], "-") + "-" + tail + suffix
}

// sanitizeIDSegment reduces a value to the characters a run id and the S3
// key derived from it can safely carry, replacing every other character with a
// dash. Dots, dashes, and underscores survive, so a service name reads
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
