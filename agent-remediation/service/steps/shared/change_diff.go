package shared

import (
	"context"
	"log/slog"
	"unicode/utf8"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// maxChangeDiffBytes caps a change diff (what this release changed in a node
// relative to the code that runs now) so one large refactor cannot dominate the
// prompt.
const maxChangeDiffBytes = 8192

// ChangeDiffer diffs a node's source in the release against the version that
// runs now.
type ChangeDiffer struct {
	Versions  ports.VersionReader
	Sanitizer ports.LogSanitizer
	Logger    *slog.Logger
}

// Diff is the unified diff from nodeID's promoted version to candidateSource,
// sanitized before it is truncated, like every other source string sent to the
// model, so a secret in a changed line is redacted rather than sent. It is empty
// when the node has no promoted version or the version read fails, which is
// logged.
func (d ChangeDiffer) Diff(ctx context.Context, nodeID, candidateSource string) string {
	cur, ok, err := d.Versions.CurrentVersion(ctx, nodeID)
	if err != nil {
		d.Logger.Warn("current version unavailable; omitting own-change diff", "node", nodeID, "error", err)
		return ""
	}
	if !ok {
		return ""
	}
	return truncateDiff(d.Sanitizer.Sanitize(proposal.ComputeUnifiedDiff(cur.RawCode, candidateSource, nodeID)), maxChangeDiffBytes)
}

// truncateDiff caps a diff at max bytes, appending a marker when it was cut.
// The cut is backed up to a UTF-8 rune boundary so truncation never splits a
// multibyte character, which would render as U+FFFD in the prompt.
func truncateDiff(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "\n… (diff truncated)"
}
