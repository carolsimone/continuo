package validation

import (
	"context"
	"log/slog"
	"sort"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// maxChangedAncestors caps how many changed ancestors the prompt shows, nearest
// first.
const maxChangedAncestors = 3

// ChangedAncestorDiffs is the context source `changed_ancestor_diffs`: what this
// release changed in the failing node's nearest changed ancestors.
type ChangedAncestorDiffs struct {
	candidates ports.CandidateSourceReader
	changes    changeDiffer
	logger     *slog.Logger
}

var _ step.ContextSource = (*ChangedAncestorDiffs)(nil)

// NewChangedAncestorDiffs builds the changed_ancestor_diffs step. It calls no
// port until Gather.
func NewChangedAncestorDiffs(candidates ports.CandidateSourceReader, versions ports.VersionReader, sanitizer ports.LogSanitizer, logger *slog.Logger) *ChangedAncestorDiffs {
	logger = loggerOrDefault(logger)
	return &ChangedAncestorDiffs{
		candidates: candidates,
		changes:    changeDiffer{versions: versions, sanitizer: sanitizer, logger: logger},
		logger:     logger,
	}
}

// Meta describes the step.
func (*ChangedAncestorDiffs) Meta() step.Meta {
	return step.Meta{
		Name: "changed_ancestor_diffs",
		Kind: step.KindContext,
		Description: "Diffs the code bundle source of the failing node's three nearest changed ancestors against their promoted versions, " +
			"which shows what the rejected release changed upstream.",
		Needs:    []remediation.EvidenceKey{remediation.KeyFailure},
		Provides: []remediation.EvidenceKey{remediation.KeyChangedAncestorDiffs},
	}
}

// Gather renders what this release changed in the failing node's nearest
// changed ancestors: each ancestor's bundle source diffed against its promoted
// version. The promoted graph's history cannot show it, since a rejected
// release was never promoted. An ancestor whose bundle source or promoted
// version is unavailable is listed with an empty diff, and none of it declines
// the cluster.
func (s *ChangedAncestorDiffs) Gather(ctx context.Context, in step.ContextInput) (step.ContextOutput, error) {
	f := in.Failure
	ancestors := append([]remediation.ChangedAncestor(nil), f.Target.ChangedAncestors...)
	sort.Slice(ancestors, func(i, j int) bool {
		if ancestors[i].Depth != ancestors[j].Depth {
			return ancestors[i].Depth < ancestors[j].Depth
		}
		return ancestors[i].NodeID < ancestors[j].NodeID
	})
	if len(ancestors) > maxChangedAncestors {
		ancestors = ancestors[:maxChangedAncestors]
	}
	out := make([]prompt.ReleaseUpstreamChange, 0, len(ancestors))
	for _, a := range ancestors {
		diff := ""
		if src, err := s.candidates.NodeSource(ctx, f.Trigger.CodeBundleURI, a.NodeID, f.Trigger.ReleaseID); err != nil {
			s.logger.Warn("changed ancestor source unavailable; listing it without a diff",
				"node", f.Target.NodeID, "ancestor", a.NodeID, "error", err)
		} else {
			diff = s.changes.diff(ctx, a.NodeID, src.RawCode)
		}
		out = append(out, prompt.ReleaseUpstreamChange{NodeID: a.NodeID, Service: a.Service, Depth: a.Depth, Diff: diff})
	}
	return step.ContextOutput{Evidence: map[remediation.EvidenceKey]any{
		remediation.KeyChangedAncestorDiffs: out,
	}}, nil
}
