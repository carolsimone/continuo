// Package casebase holds the failure-precedent vocabulary: rejections, their
// classified error signatures, the fix proposals opened for them, and the
// links to the code versions that resolved them.
package casebase

import "time"

// Rejection is one classified failure of one node in one rejected release.
// Its identity is (ReleaseID, NodeID), matching the classifier's dedup key.
type Rejection struct {
	ReleaseID string
	NodeID    string
	// Stage is the classifier source: "validation" | "seed_build" | "compile"
	// | "duplicate_table".
	Stage        string
	Category     string
	Reason       string
	Signature    string
	ErrorExcerpt string // sanitized before storage
	DBTLogURI    string
	// At is when the classifier decided (the trigger's classified_at).
	At time.Time
	// RawCode and ContentHash are the failing candidate's code, read from the
	// release's code bundle. Both empty when no bundle exists — a compile-stage
	// failure happens before any parse, so there is nothing to read; the
	// rejection is recorded anyway (excerpt + fixing-PR link are still
	// precedent).
	RawCode     string
	ContentHash string
}

// Proposal is one fix attempt (identified by ProposalID) for a rejected
// release. A batched attempt spanning several nodes MERGEs onto the same
// :Proposal, reached by one [:PROPOSED] edge per resolved :Rejection —
// ReleaseID and NodeID here identify that one rejection's edge, not the
// :Proposal itself. PR facts (url/number/state/opened_*) live on the
// [:HAS_PR]->(:PullRequest) node, not here; legacy pre-split :Proposal nodes
// still carry them inline for backward reads.
type Proposal struct {
	ProposalID string
	ReleaseID  string
	NodeID     string
}

// PullRequest is the PR facts for one proposal's fix, scoped to the service
// the PR targets. A batched proposal resolves many nodes with one PR, so the
// PR's facts are recorded once on this node — [:HAS_PR] from the shared
// :Proposal — rather than duplicated per resolved node.
type PullRequest struct {
	ProposalID string
	Service    string
	PrURL      string
	PrNumber   int
	State      string
	OpenedBy   string
	OpenedAt   time.Time
}

// EditOutcome is one file a merged fix PR edited, as it stood at merge.
// Amended reports whether a human changed this edit before merge; Diff is the
// proposal-time unified diff of the edit (the precedent read renders the
// merged-truth diff instead when the edit is amended). TargetNodeID is the node
// the edit fixes — the :Table the [:EDITED] edge points at.
type EditOutcome struct {
	Path         string
	TargetNodeID string
	Amended      bool
	Diff         string
}

// PullRequestOutcome is one fix PR's terminal state, scoped to the owning
// service. Outcome is "merged" or "rejected". On a merged outcome the case
// base draws provenance edges: [:RESOLVED_BY] from each resolved rejection to
// the shared :Proposal, and [:EDITED] from that :Proposal to each edit's
// :Table. A rejected outcome only stamps the :PullRequest's terminal state and
// draws no edges; ResolvedNodeIDs and Edits are empty for it.
type PullRequestOutcome struct {
	ProposalID      string
	ReleaseID       string
	Service         string
	Outcome         string
	ClosedAt        time.Time
	ResolvedNodeIDs []string
	Edits           []EditOutcome
}
