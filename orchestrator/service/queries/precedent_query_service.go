package queries

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/orchestrator/domain/casebase"
)

// precedentDefaultLimit/precedentMaxLimit bound GetPrecedents. Precedents feed
// LLM prompts, so the default is deliberately small; the max keeps a broad
// signature from loading the whole case base's code.
const (
	precedentDefaultLimit = 5
	precedentMaxLimit     = 20
)

// PrecedentReader is the read surface over the failure-precedent case base.
type PrecedentReader interface {
	// Precedents returns rejections matching the signature — or, when
	// signature is empty, the (category, reason) pair — resolved-first then
	// newest, capped at limit BEFORE code bodies are fetched. includeCode
	// governs the failing raw_code only; resolving/prior version bodies are
	// always fetched because the diff is rendered from them.
	Precedents(ctx context.Context, signature, category, reason string, limit int32, includeCode bool) ([]casebase.PrecedentView, error)
}

// PrecedentQueryService renders precedent rows into the capped, diff-rendered
// entries the gRPC and CLI surfaces serve.
type PrecedentQueryService struct {
	reader PrecedentReader
}

// NewPrecedentQueryService constructs a PrecedentQueryService.
func NewPrecedentQueryService(reader PrecedentReader) *PrecedentQueryService {
	return &PrecedentQueryService{reader: reader}
}

// GetPrecedents serves the precedent lookup. No match is an empty slice, not
// an error: a signature is a search key and "no precedent" is a valid answer.
func (s *PrecedentQueryService) GetPrecedents(
	ctx context.Context,
	signature, category, reason string,
	limit int32,
	includeCode bool,
) ([]casebase.Precedent, error) {
	limit = clampPrecedentLimit(limit)
	views, err := s.reader.Precedents(ctx, signature, category, reason, limit, includeCode)
	if err != nil {
		return nil, fmt.Errorf("PrecedentQueryService.GetPrecedents: %w", err)
	}
	out := make([]casebase.Precedent, 0, len(views))
	for _, v := range views {
		p := casebase.Precedent{
			Rejection: v.Rejection,
			// A precedent is resolved by an own-timeline version, by a merged
			// PR's drawn edits, or — even when that PR drew no edits because
			// every edit target was absent from the graph — by the presence of
			// the [:RESOLVED_BY]->(:Proposal) edge itself, so Resolved agrees
			// with the identity query's resolved-first ordering.
			Resolved:         v.ResolvingVersion != nil || len(v.Edited) > 0 || v.ResolvedByProposal,
			ResolvingVersion: v.ResolvingVersion,
			Proposals:        v.Proposals,
		}
		// The own-timeline resolution diff is rendered as before and preferred
		// when present: it is the authoritative record when a resolving version
		// exists on the node's own timeline.
		if v.ResolvingVersion != nil && v.PriorVersion != nil {
			p.ResolutionDiff, p.ResolutionDiffTruncated = renderUnifiedDiff(
				v.PriorVersion.RawCode, v.ResolvingVersion.RawCode,
				v.PriorVersion.VersionSeq, v.ResolvingVersion.VersionSeq)
		}
		p.Edited = renderEditedDiffs(v.Edited)
		if !includeCode && p.ResolvingVersion != nil {
			stripped := *p.ResolvingVersion
			stripped.RawCode, stripped.CompiledCode = "", ""
			p.ResolvingVersion = &stripped
		}
		out = append(out, p)
	}
	return out, nil
}

// renderEditedDiffs copies each edited-provenance entry, replacing its Diff
// with the merged-truth diff (the version the merge superseded vs the promoted
// merged version) when the edit was amended and a straddling version was
// selected; otherwise it keeps the edge's own stored proposal diff. The merged
// diff is capped at diffByteCap like every other rendered diff.
func renderEditedDiffs(edited []casebase.EditedView) []casebase.EditedView {
	if len(edited) == 0 {
		return nil
	}
	out := make([]casebase.EditedView, 0, len(edited))
	for _, e := range edited {
		if e.MergedVersion != nil {
			var priorCode string
			var priorSeq int64
			if e.MergedPrior != nil {
				priorCode, priorSeq = e.MergedPrior.RawCode, e.MergedPrior.VersionSeq
			}
			e.Diff, _ = renderUnifiedDiff(priorCode, e.MergedVersion.RawCode, priorSeq, e.MergedVersion.VersionSeq)
		}
		out = append(out, e)
	}
	return out
}

func clampPrecedentLimit(limit int32) int32 {
	if limit <= 0 {
		return precedentDefaultLimit
	}
	if limit > precedentMaxLimit {
		return precedentMaxLimit
	}
	return limit
}
