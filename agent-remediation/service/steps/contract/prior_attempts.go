package contract

import (
	"context"
	"log/slog"
	"sort"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// PriorAttempts is the context source `prior_attempts`: the earlier attempts at
// the failing node, with the error each one's verification reported and the
// diffs it applied.
type PriorAttempts struct {
	attempts  repository.AttemptLister
	evidence  ports.EvidenceReader
	sanitizer ports.LogSanitizer
	logger    *slog.Logger
}

var _ step.ContextSource = (*PriorAttempts)(nil)

// NewPriorAttempts builds the prior_attempts step. A nil attempts reader gives
// no prior attempts. It calls no port until Gather.
func NewPriorAttempts(attempts repository.AttemptLister, evidence ports.EvidenceReader, sanitizer ports.LogSanitizer, logger *slog.Logger) *PriorAttempts {
	return &PriorAttempts{attempts: attempts, evidence: evidence, sanitizer: sanitizer, logger: loggerOrDefault(logger)}
}

// Meta describes the step.
func (*PriorAttempts) Meta() step.Meta {
	return step.Meta{
		Name: "prior_attempts",
		Kind: step.KindContext,
		Description: "Lists the earlier attempts at the failing node in this release, oldest first, each with the sanitized error its verification run " +
			"reported and the diffs it applied. Leaves out the attempt being made, and provides none when the history cannot be read.",
		Needs:    []remediation.EvidenceKey{remediation.KeyFailure},
		Provides: []remediation.EvidenceKey{remediation.KeyPriorAttempts},
	}
}

// Gather returns the earlier attempts at this same failure, oldest first. This
// is what makes attempt N+1 better informed than attempt N. It is best-effort,
// so an unreadable attempt history costs the section rather than the heal. The
// attempt being made, and any later one, is left out: it is not precedent for
// itself. No row limit is applied: the per-failure attempt cap already bounds
// how many rows can exist.
func (s *PriorAttempts) Gather(ctx context.Context, in step.ContextInput) (step.ContextOutput, error) {
	return step.ContextOutput{Evidence: map[remediation.EvidenceKey]any{
		remediation.KeyPriorAttempts: s.load(ctx, in.Failure),
	}}, nil
}

func (s *PriorAttempts) load(ctx context.Context, f remediation.Failure) []prompt.PriorAttempt {
	if s.attempts == nil {
		return nil
	}
	nodeID := f.Target.NodeID
	views, err := s.attempts.List(ctx, repository.ProposalFilter{
		ReleaseID: f.Trigger.ReleaseID,
		Source:    f.Trigger.Source,
		NodeID:    nodeID,
	})
	if err != nil {
		s.logger.Warn("prior attempts unavailable; proceeding without them", "node", nodeID, "error", err)
		return nil
	}
	out := make([]prompt.PriorAttempt, 0, len(views))
	for _, v := range views {
		if v.Attempt >= f.Attempt {
			continue
		}
		out = append(out, prompt.PriorAttempt{
			Attempt:     v.Attempt,
			VerifyError: s.sanitizer.Sanitize(v.VerifyError),
			Diffs:       s.diffs(ctx, v),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Attempt < out[j].Attempt })
	return out
}

// diffs resolves one recorded attempt's edits to their stored unified diffs. A
// diff that cannot be read is omitted: the attempt still shows as tried, with
// its error, which is the more important half.
func (s *PriorAttempts) diffs(ctx context.Context, v proposal.View) []prompt.AttemptDiff {
	out := make([]prompt.AttemptDiff, 0, len(v.Edits))
	for _, e := range v.Edits {
		if e.DiffURI == "" {
			continue
		}
		raw, err := s.evidence.Fetch(ctx, e.DiffURI)
		if err != nil {
			s.logger.Warn("prior attempt diff unavailable; omitting it",
				"attempt", v.Attempt, "diff_uri", e.DiffURI, "error", err)
			continue
		}
		out = append(out, prompt.AttemptDiff{Path: e.Path, Diff: s.sanitizer.Sanitize(raw)})
	}
	return out
}
