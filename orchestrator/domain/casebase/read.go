// Read-side views over the case base, mirroring codeversion/read.go: reader
// rows first, then the rendered shape frontends receive.
package casebase

import "github.com/carolsimone/continuo/orchestrator/domain/codeversion"

// ProposalView is one fix PR attached to a precedent. PrState reflects the
// linked :PullRequest node's live state when present (open until the close-loop
// mirrors a terminal outcome, merged/rejected afterwards), falling back to the
// legacy inline snapshot on the :Proposal for rows recorded before the per-PR
// split.
type ProposalView struct {
	ProposalID string
	PrURL      string
	PrNumber   int
	PrState    string
}

// EditedView is one provenance edit a merged fix PR made to a node, read from
// the (:Proposal)-[:EDITED]->(:Table) edge. NodeID is the edited node — which
// may be an upstream ancestor of the rejection rather than the rejected node
// itself. Diff is the edge's stored proposal-time diff. When the edit was
// Amended and a :NodeVersion was promoted after the PR closed, MergedPrior and
// MergedVersion carry the versions straddling the merge so the service can
// render the amended/merged-truth diff from real promoted code.
//
// DiffIsShipped reports whether the rendered Diff is that merged-truth diff (a
// post-close NodeVersion straddle was found) rather than the proposal-time
// fallback. It is set by the query service when it renders the diff, so a
// consumer can tell an amended edit whose shipped code is recorded from one
// whose merged version has not been promoted yet.
type EditedView struct {
	NodeID        string
	Path          string
	Amended       bool
	Diff          string
	DiffIsShipped bool
	MergedPrior   *codeversion.VersionView
	MergedVersion *codeversion.VersionView
}

// PrecedentView is one reader-level precedent row: the rejection plus the
// resolving version and the version it superseded (next-older by promoted_at)
// so the service can render the resolution diff, plus any edited-node
// provenance carried by a merged fix PR that resolved it.
//
// ResolvedByProposal reports whether the rejection carries a
// [:RESOLVED_BY]->(:Proposal) edge — the same proposal-resolved signal the
// identity query uses for resolved-first ordering. It is surfaced here so the
// service's Resolved boolean agrees with that ordering even when the merged PR
// drew no [:EDITED] edges (all edit targets were absent from the graph), so a
// genuinely-fixed rejection is never rendered as unresolved.
type PrecedentView struct {
	Rejection          Rejection
	ResolvingVersion   *codeversion.VersionView
	PriorVersion       *codeversion.VersionView
	Proposals          []ProposalView
	Edited             []EditedView
	ResolvedByProposal bool
}

// Precedent is the rendered precedent entry served to frontends: the
// resolution rendered as a diff so every caller sees the same thing. A
// precedent is Resolved when it has an own-timeline resolving version OR a
// merged fix PR recorded at least one edit. Each Edited entry's Diff is the
// merged-truth diff when the edit was amended and a promoted version straddles
// the merge, otherwise the proposal's own edit diff.
type Precedent struct {
	Rejection               Rejection
	Resolved                bool
	ResolvingVersion        *codeversion.VersionView
	ResolutionDiff          string
	ResolutionDiffTruncated bool
	Proposals               []ProposalView
	Edited                  []EditedView
}
