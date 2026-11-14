// Package validation holds the steps of the flow for a dbt node whose release
// failed validation: the context sources that read the candidate SQL, the
// node's source from the release's code bundle and what the release changed in
// the node and in its ancestors, and the prompt that asks the model for a
// diagnosis and then applies it to the node's source. Each step depends only on
// the ports it reads through.
package validation

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// CandidateSQL is the context source `candidate_sql`: the pre-compiled SQL the
// release's validation of the failing node ran, from object storage.
type CandidateSQL struct {
	evidence ports.EvidenceReader
	logger   *slog.Logger
}

var _ step.ContextSource = (*CandidateSQL)(nil)

// NewCandidateSQL builds the candidate_sql step. It calls no port until Gather.
func NewCandidateSQL(evidence ports.EvidenceReader, logger *slog.Logger) *CandidateSQL {
	return &CandidateSQL{evidence: evidence, logger: shared.LoggerOrDefault(logger)}
}

// Meta describes the step.
func (*CandidateSQL) Meta() step.Meta {
	return step.Meta{
		Name: "candidate_sql",
		Kind: step.KindContext,
		Description: "Reads the pre-compiled SQL of the failing dbt node from the validation candidate artifact. " +
			"Declines a python node, a failure with no candidate artifact and one whose artifact cannot be read.",
		Needs:    []remediation.EvidenceKey{remediation.KeyFailure},
		Provides: []remediation.EvidenceKey{remediation.KeyCandidateSQL},
	}
}

// Gather fetches the candidate SQL the failure's target names. A python node is
// declined before anything is read: its candidate artifact is a JSON validation
// spec, not SQL, and its bundle entry is a contract entry, so neither a
// diagnosis nor a source fix can be built from them. A failure with no
// candidate artifact, or one whose object is gone for good, has no SQL to fix
// and is declined too. Any other read error is transient and is returned so the
// work is tried again.
func (s *CandidateSQL) Gather(ctx context.Context, in step.ContextInput) (step.ContextOutput, error) {
	n := in.Failure.Target
	if pkg_model.NodeType(n.NodeType).IsPython() {
		s.logger.Info("validation fix: failing node is a python node; skipping — "+
			"its candidate artifact is a validation spec and its bundle entry is a contract "+
			"entry, neither of which is model source a fix can be proposed against",
			"node", n.NodeID)
		return step.ContextOutput{Declined: true}, nil
	}
	if n.CandidateArtifactURI == "" {
		return step.ContextOutput{Declined: true}, nil
	}
	sql, err := s.evidence.Fetch(ctx, n.CandidateArtifactURI)
	if errors.Is(err, ports.ErrNotFound) {
		s.logger.Info("validation fix: candidate SQL unreadable; skipping",
			"node", n.NodeID, "candidate_uri", n.CandidateArtifactURI, "error", err)
		return step.ContextOutput{Declined: true}, nil
	}
	if err != nil {
		return step.ContextOutput{}, fmt.Errorf("fetch candidate sql: %w", err)
	}
	return step.ContextOutput{Evidence: map[remediation.EvidenceKey]any{
		remediation.KeyCandidateSQL: remediation.Text(sql),
	}}, nil
}
