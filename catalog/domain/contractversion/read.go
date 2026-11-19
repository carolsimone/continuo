package contractversion

import (
	"errors"
	"time"
)

// ErrNotFound is returned by a contract read that finds nothing: a node with
// no current contract (one never promoted with a contract, a retired node or
// a node the tenant does not hold), or a version the node has no promotion
// edge for.
var ErrNotFound = errors.New("contract not found")

// WriteResult counts what one promotion's write created in the graph.
type WriteResult struct {
	// VersionsCreated counts the stored contracts the graph did not hold yet.
	VersionsCreated int
	// EdgesCreated counts the promotions recorded as a change of a node's
	// contract.
	EdgesCreated int
	// PointersMoved counts the nodes whose current-contract pointer moved.
	PointersMoved int
}

// VersionView is one promotion of a node's contract history as a reader sees
// it: the change's counter and facts, and the stored contract it made
// current.
type VersionView struct {
	UniqueID string
	// Version is the counter the promotion gave this change.
	Version      int64
	ContractHash string
	ReleaseID    string
	PromotionSeq int64
	PromotedAt   time.Time
	// ContentHash is the node's code fingerprint at the promotion.
	ContentHash       string
	ContinuoContracts string
	OntologyVersion   string
	// SameAsVersion is, on a revert, the earliest version holding the same
	// contract hash; 0 otherwise.
	SameAsVersion int64
	SourcePath    string
	// IsCurrent marks the version the node's pointer names. A retired node has
	// no current version.
	IsCurrent bool
	// Document is the stored form of the contract; nil where a read leaves it
	// out.
	Document map[string]any
}

// LiveContract is the current contract of a live node, with the provenance of
// the promotion that made it current.
type LiveContract struct {
	UniqueID     string
	Version      int64
	ContractHash string
	Document     map[string]any
	Provenance   Provenance
	// PhysicalSchema is the schema the node is declared in and builds in, in
	// the spelling the topology swap stored on the node, which can differ in
	// case from the schema part of its lower-case contract id. It is empty
	// while no swap has written the node.
	PhysicalSchema string
}
