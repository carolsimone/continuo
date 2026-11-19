package shared

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/pkg/sanitize"
)

// maxChangeDiffBytes caps a change diff (what this release changed in a node
// relative to the code that runs now) so one large refactor cannot dominate the
// prompt.
const maxChangeDiffBytes = 8192

// ChangeDiffer diffs a node's source in the release against the version that
// runs now.
type ChangeDiffer struct {
	Versions ports.VersionReader
	Logger   *slog.Logger
}

// Diff is the unified diff from nodeID's promoted version to candidateSource,
// capped. The catalog stores a version's code cleaned by sanitize.Text, so the
// diff compares that text with the candidate cleaned the same way, and the
// catalog's redaction never shows as a change. It is empty when the node has no
// promoted version or the version read fails, which is logged.
func (d ChangeDiffer) Diff(ctx context.Context, nodeID, candidateSource string) string {
	cur, ok, err := d.Versions.CurrentVersion(ctx, nodeID)
	if err != nil {
		d.Logger.Warn("current version unavailable; omitting own-change diff", "node", nodeID, "error", err)
		return ""
	}
	if !ok {
		return ""
	}
	return prompt.TruncateDiff(proposal.ComputeUnifiedDiff(sanitize.Text(cur.RawCode), sanitize.Text(candidateSource), nodeID), maxChangeDiffBytes)
}
