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

// Proposal is one fix PR opened for a rejection.
type Proposal struct {
	ProposalID string
	ReleaseID  string
	NodeID     string
	PrURL      string
	PrNumber   int
	PrState    string
	OpenedBy   string
	OpenedAt   time.Time
}
