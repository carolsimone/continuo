package validation

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
)

// OwnChangeDiff is the context source `own_change_diff`: what this release
// changed in the failing node.
type OwnChangeDiff struct {
	changes shared.ChangeDiffer
}

var _ step.ContextSource = (*OwnChangeDiff)(nil)

// NewOwnChangeDiff builds the own_change_diff step. It calls no port until
// Gather.
func NewOwnChangeDiff(versions ports.VersionReader, sanitizer ports.LogSanitizer, logger *slog.Logger) *OwnChangeDiff {
	return &OwnChangeDiff{changes: shared.ChangeDiffer{Versions: versions, Sanitizer: sanitizer, Logger: shared.LoggerOrDefault(logger)}}
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
		diff = s.changes.Diff(ctx, in.Failure.Target.NodeID, src.Source)
	}
	return step.ContextOutput{Evidence: map[remediation.EvidenceKey]any{
		remediation.KeyOwnChangeDiff: remediation.Text(diff),
	}}, nil
}
