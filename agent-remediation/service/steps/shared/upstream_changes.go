package shared

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// UpstreamChanges is the context source `upstream_changes`: the diffs of the
// failing node's most recently changed ancestors, from the live topology.
type UpstreamChanges struct {
	upstream ports.UpstreamChangeReader
	logger   *slog.Logger
}

var _ step.ContextSource = (*UpstreamChanges)(nil)

// NewUpstreamChanges builds the upstream_changes step. It calls no port until
// Gather.
func NewUpstreamChanges(upstream ports.UpstreamChangeReader, logger *slog.Logger) *UpstreamChanges {
	return &UpstreamChanges{upstream: upstream, logger: LoggerOrDefault(logger)}
}

// Meta describes the step.
func (*UpstreamChanges) Meta() step.Meta {
	return step.Meta{
		Name:        "upstream_changes",
		Kind:        step.KindContext,
		Description: "Reads the recent code and config diffs of the failing node's changed ancestors from the catalog.",
		Needs:       []remediation.EvidenceKey{remediation.KeyFailure},
		Provides:    []remediation.EvidenceKey{remediation.KeyUpstreamChanges},
	}
}

// Gather reads the target node's upstream changes, as the catalog caps them. A
// read error degrades to no upstream context and never declines the cluster.
func (s *UpstreamChanges) Gather(ctx context.Context, in step.ContextInput) (step.ContextOutput, error) {
	nodeID := in.Failure.Target.NodeID
	changes, err := s.upstream.UpstreamChanges(ctx, nodeID)
	if err != nil {
		s.logger.Warn("upstream changes unavailable; proceeding without upstream context",
			"node", nodeID, "error", err)
		changes = nil
	}
	out := make([]prompt.UpstreamChange, len(changes))
	copy(out, changes)
	return step.ContextOutput{Evidence: map[remediation.EvidenceKey]any{
		remediation.KeyUpstreamChanges: out,
	}}, nil
}
