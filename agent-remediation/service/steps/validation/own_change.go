package validation

import (
	"context"
	"fmt"
	"log/slog"
	"unicode/utf8"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// maxDiffBytes caps a change diff (what this release changed in a node relative
// to the code that runs now) so one large refactor cannot dominate the prompt.
const maxDiffBytes = 8192

// OwnChangeDiff is the context source `own_change_diff`: what this release
// changed in the failing node.
type OwnChangeDiff struct {
	changes changeDiffer
}

var _ step.ContextSource = (*OwnChangeDiff)(nil)

// NewOwnChangeDiff builds the own_change_diff step. It calls no port until
// Gather.
func NewOwnChangeDiff(versions ports.VersionReader, sanitizer ports.LogSanitizer, logger *slog.Logger) *OwnChangeDiff {
	return &OwnChangeDiff{changes: changeDiffer{versions: versions, sanitizer: sanitizer, logger: loggerOrDefault(logger)}}
}

// Meta describes the step.
func (*OwnChangeDiff) Meta() step.Meta {
	return step.Meta{
		Name: "own_change_diff",
		Kind: step.KindContext,
		Description: "Diffs the failing node's source in the release's code bundle against the version that runs now, sanitized and capped. " +
			"Empty for a new node or one whose source could not be read.",
		Needs:    []remediation.EvidenceKey{remediation.KeyFailure, remediation.KeyCodeBundleSource},
		Provides: []remediation.EvidenceKey{remediation.KeyOwnChangeDiff},
	}
}

// Gather diffs the candidate source the code_bundle_source step found. With no
// source there is nothing to diff, and the text is empty. A node with no
// promoted version (it is new) or an unreadable version also gives an empty
// text, which omits the section rather than blocking the fix.
func (s *OwnChangeDiff) Gather(ctx context.Context, in step.ContextInput) (step.ContextOutput, error) {
	src, ok := remediation.Get[remediation.CandidateSource](in.Evidence, remediation.KeyCodeBundleSource)
	if !ok {
		return step.ContextOutput{}, fmt.Errorf("own_change_diff: the evidence holds no code bundle source")
	}
	diff := ""
	if src.Source != "" {
		diff = s.changes.diff(ctx, in.Failure.Target.NodeID, src.Source)
	}
	return step.ContextOutput{Evidence: map[remediation.EvidenceKey]any{
		remediation.KeyOwnChangeDiff: remediation.Text(diff),
	}}, nil
}

// changeDiffer diffs a node's source in the release against the version that
// runs now.
type changeDiffer struct {
	versions  ports.VersionReader
	sanitizer ports.LogSanitizer
	logger    *slog.Logger
}

// diff is the unified diff from nodeID's promoted version to candidateSource,
// sanitized before it is truncated, like every other source string sent to the
// model, so a secret in a changed line is redacted rather than sent. It is
// empty when the node has no promoted version or the version read fails, which
// is logged.
func (d changeDiffer) diff(ctx context.Context, nodeID, candidateSource string) string {
	cur, ok, err := d.versions.CurrentVersion(ctx, nodeID)
	if err != nil {
		d.logger.Warn("current version unavailable; omitting own-change diff", "node", nodeID, "error", err)
		return ""
	}
	if !ok {
		return ""
	}
	return truncateDiff(d.sanitizer.Sanitize(proposal.ComputeUnifiedDiff(cur.RawCode, candidateSource, nodeID)), maxDiffBytes)
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
