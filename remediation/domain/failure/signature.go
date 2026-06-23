package failure

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
)

var (
	// candidate_<hex|digits> — the per-release validation schema name.
	reCandidateSchema = regexp.MustCompile(`candidate_[a-z0-9]+`)
	// UUIDs / invocation ids.
	reUUID = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	// ISO-ish timestamps and clock times.
	reTimestamp = regexp.MustCompile(`\d{4}-\d{2}-\d{2}[t ]\d{2}:\d{2}:\d{2}[.,0-9]*z?`)
	// "got 14 results" → "got N results".
	reGotResults = regexp.MustCompile(`got \d+ results?`)
	// trailing "(line 12, col 3)" style positions.
	reLineCol = regexp.MustCompile(`(line|col(umn)?)\s+\d+`)
	// any standalone run of digits left over.
	reDigits = regexp.MustCompile(`\d+`)
	// collapse whitespace runs.
	reWhitespace = regexp.MustCompile(`\s+`)
)

// NormalizeSignature produces a stable dedup key for a failed node. It folds
// the category together with the error text after stripping the volatile parts
// that differ release-to-release (candidate schema name, UUIDs, timestamps,
// row counts, line/column numbers, leftover digits). The same underlying error
// in two different releases yields the same signature.
func NormalizeSignature(category Category, rawErrorLine string) string {
	s := strings.ToLower(rawErrorLine)
	s = reCandidateSchema.ReplaceAllString(s, "candidate_schema")
	s = reUUID.ReplaceAllString(s, "uuid")
	s = reTimestamp.ReplaceAllString(s, "ts")
	s = reGotResults.ReplaceAllString(s, "got n results")
	s = reLineCol.ReplaceAllString(s, "$1 n")
	s = reDigits.ReplaceAllString(s, "n")
	s = reWhitespace.ReplaceAllString(s, " ")
	s = strings.TrimSpace(s)

	sum := sha256.Sum256([]byte(string(category) + "|" + s))
	return hex.EncodeToString(sum[:])
}
