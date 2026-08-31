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

// Proposal is one fix PR opened for a rejection. Its identity here is
// (ReleaseID, NodeID): one :Proposal per resolved node, all sharing the one
// PR's facts, which now live on the linked PullRequest instead of inline on
// this type — pre-existing :Proposal nodes keep their own pr_* properties for
// legacy reads.
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
