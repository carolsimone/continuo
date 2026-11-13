package shared

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// maxPrecedentsFetch bounds how many precedents one lookup requests; the prompt
// renderer separately bounds how many carry full diffs.
const maxPrecedentsFetch = 10

// Precedents is the context source `failure_precedents`: past rejections that
// look like this failure, and the change that fixed them when one did.
type Precedents struct {
	precedents ports.PrecedentReader
	sanitizer  ports.LogSanitizer
	logger     *slog.Logger
}

var _ step.ContextSource = (*Precedents)(nil)

// NewPrecedents builds the failure_precedents step. It calls no port until
// Gather.
func NewPrecedents(precedents ports.PrecedentReader, sanitizer ports.LogSanitizer, logger *slog.Logger) *Precedents {
	return &Precedents{precedents: precedents, sanitizer: sanitizer, logger: loggerOrDefault(logger)}
}

// Meta describes the step.
func (*Precedents) Meta() step.Meta {
	return step.Meta{
		Name:        "failure_precedents",
		Kind:        step.KindContext,
		Description: "Looks up past rejections with the same error signature, or else the same category and reason, with the changes that fixed them.",
		Needs:       []remediation.EvidenceKey{remediation.KeyFailure},
		Provides:    []remediation.EvidenceKey{remediation.KeyFailurePrecedents},
	}
}

// Gather fetches precedent for the failure's precedent subject: by exact
// signature first, then, when removing this failure's own recorded rejection
// leaves the exact-signature result empty, by the broader (category, reason)
// class. The case base holds this same trigger's rejection, so on a first
// occurrence of a signature the exact match can be nothing but that row: the
// self-filter therefore runs on each fetch before the fallback decision, or the
// fallback would be skipped precisely when it is the only thing that could
// help. A lookup error degrades to no precedent and never declines the
// cluster.
func (s *Precedents) Gather(ctx context.Context, in step.ContextInput) (step.ContextOutput, error) {
	subject := in.Failure.PrecedentSubject()
	releaseID := in.Failure.Trigger.ReleaseID
	fetch := func(q ports.PrecedentQuery) []prompt.Precedent {
		ps, err := s.precedents.Precedents(ctx, q)
		if err != nil {
			s.logger.Warn("precedent lookup unavailable; proceeding without precedent",
				"node", subject.NodeID, "error", err)
			return nil
		}
		return withoutSelf(ps, releaseID, subject.NodeID, s.sanitizer)
	}
	var ps []prompt.Precedent
	if subject.ErrorSignature != "" {
		ps = fetch(ports.PrecedentQuery{Signature: subject.ErrorSignature, Limit: maxPrecedentsFetch})
	}
	if len(ps) == 0 && subject.Category != "" && subject.Reason != "" {
		ps = fetch(ports.PrecedentQuery{Category: subject.Category, Reason: subject.Reason, Limit: maxPrecedentsFetch})
	}
	return step.ContextOutput{Evidence: map[remediation.EvidenceKey]any{
		remediation.KeyFailurePrecedents: ps,
	}}, nil
}

// withoutSelf drops this failure's own recorded rejection from a precedent
// result set (it is this failure, not precedent for it) and sanitizes the
// free-text fields of each remaining precedent, its edited diffs included: a
// merged fix PR's diff can carry the same secrets a proposal's diff can. It
// builds a fresh slice, since ps is backed by whatever the reader returned and
// reusing its array would corrupt that value across calls.
func withoutSelf(ps []prompt.Precedent, releaseID, nodeID string, sanitizer ports.LogSanitizer) []prompt.Precedent {
	out := make([]prompt.Precedent, 0, len(ps))
	for _, p := range ps {
		if p.ReleaseID == releaseID && p.NodeID == nodeID {
			continue
		}
		p.ResolutionDiff = sanitizer.Sanitize(p.ResolutionDiff)
		p.ErrorExcerpt = sanitizer.Sanitize(p.ErrorExcerpt)
		if len(p.Edited) > 0 {
			edited := make([]prompt.EditedPrecedent, len(p.Edited))
			for i, e := range p.Edited {
				e.Diff = sanitizer.Sanitize(e.Diff)
				edited[i] = e
			}
			p.Edited = edited
		}
		out = append(out, p)
	}
	return out
}
