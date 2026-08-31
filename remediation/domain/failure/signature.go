package failure

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
)

var (
	// The database's echoed statement context: everything from the first
	// "LINE <n>:" marker to the end of the message. Postgres appends the
	// statement that failed, which for a model build is
	// `CREATE TABLE "<schema>"."<node>" AS (SELECT ...)` — so the text embeds
	// the failing node's OWN relation name, plus a caret line whose indentation
	// tracks that name's length. Two models broken by one upstream change then
	// produce different error text and sign differently, and the shared-upstream
	// grouping downstream (which clusters only on identical signatures) can
	// never form a cluster. The marker onwards is echoed context, not diagnosis,
	// so it is cut. Spans newlines so the caret line goes with it.
	reEchoedStatement = regexp.MustCompile(`(?is)\s+line\s+\d+:.*$`)
	// candidate_<hex|digits> — the per-release validation schema name.
	reCandidateSchema = regexp.MustCompile(`candidate_[a-z0-9]+`)
	// UUIDs / invocation ids.
	reUUID = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	// ISO-ish timestamps and clock times.
	reTimestamp = regexp.MustCompile(`\d{4}-\d{2}-\d{2}[t ]\d{2}:\d{2}:\d{2}[.,0-9]*z?`)
	// "got 14 results" → "got N results".
	reGotResults = regexp.MustCompile(`got \d+ results?`)
	// "line N" / "col N" / "column N" position markers anywhere in the error text.
	reLineCol = regexp.MustCompile(`(line|col(umn)?)\s+\d+`)
	// any standalone run of digits left over.
	reDigits = regexp.MustCompile(`\d+`)
	// collapse whitespace runs.
	reWhitespace = regexp.MustCompile(`\s+`)
)

// NormalizeSignature produces a stable dedup key for a failed node. It folds
// the category together with the error text after stripping the parts that do
// not identify the failure itself: the database's echoed statement, and the
// volatile tokens that differ release-to-release (candidate schema name, UUIDs,
// timestamps, row counts, line/column numbers, leftover digits). The same
// underlying error yields the same signature in two different releases, and
// also across two different nodes that failed for the same reason.
func NormalizeSignature(category Category, rawErrorLine string) string {
	s := strings.ToLower(rawErrorLine)
	// Cut the echoed statement first: it carries the failing node's own name,
	// so leaving it in would key the signature on the victim rather than on
	// the fault.
	s = reEchoedStatement.ReplaceAllString(s, "")
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
