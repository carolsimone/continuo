// Package casebase holds the failure-precedent vocabulary: rejections, their
// classified error signatures, the fixes proposed for them, and the
// links to the code versions that resolved them.
package casebase

import "time"

// Rejection is one classified failure of one node in one rejected release.
// Its identity is (ReleaseID, NodeID), matching the classifier's dedup key.
type Rejection struct {
	ReleaseID string
	NodeID    string
	// Stage is the classifier source: "parse" | "validation" | "seed_build" |
	// "compile" | "duplicate_table".
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

// Fix is one fix (identified by FixID) continuo proposed for a rejected
// release. A fix spanning several nodes MERGEs onto the same :Fix, reached
// by one [:PROPOSED] edge per resolved :Rejection — ReleaseID and NodeID
// here identify that one rejection's edge, not the :Fix itself. PR facts
// (url/number/state/opened_*) live on the [:HAS_PR]->(:PullRequest) node,
// never on the :Fix.
type Fix struct {
	FixID     string
	ReleaseID string
	NodeID    string
}

// PullRequest is the PR facts of one fix, scoped to the project the PR
// targets. A batched fix resolves many nodes with one PR, so the
// PR's facts are recorded once on this node — [:HAS_PR] from the shared
// :Fix — rather than duplicated per resolved node.
type PullRequest struct {
	FixID    string
	Project  string
	PrURL    string
	PrNumber int
	State    string
	OpenedBy string
	OpenedAt time.Time
}

// EditOutcome is one file a merged fix PR edited, as it stood at merge.
// Amended reports whether a human changed this edit before merge; Diff is the
// unified diff of the edit as proposed (the precedent read renders the
// merged-truth diff instead when the edit is amended). TargetNodeID is the node
// the edit fixes — the :Node the [:EDITED] edge points at.
type EditOutcome struct {
	Path         string
	TargetNodeID string
	Amended      bool
	Diff         string
}

// PullRequestOutcome is one fix PR's terminal state, scoped to the owning
// project. Outcome is "merged" or "rejected". On a merged outcome the case
// base draws provenance edges: [:RESOLVED_BY] from each resolved rejection to
// the shared :Fix, and [:EDITED] from that :Fix to each edit's
// :Node. A rejected outcome only stamps the :PullRequest's terminal state and
// draws no edges; ResolvedNodeIDs and Edits are empty for it.
//
// PrURL and PrNumber are the PR's identifying facts, carried on the close event
// too. They let the outcome fill those facts when it lands before the PR's open
// event (independent consumer groups process the two out of order), so a
// close-first :PullRequest is not left permanently blank.
type PullRequestOutcome struct {
	FixID           string
	ReleaseID       string
	Project         string
	Outcome         string
	ClosedAt        time.Time
	PrURL           string
	PrNumber        int
	ResolvedNodeIDs []string
	Edits           []EditOutcome
}
