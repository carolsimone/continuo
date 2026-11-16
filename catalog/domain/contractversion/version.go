// Package contractversion holds the vocabulary of a node's contract history:
// the immutable stored contracts, the promotions that made each one current,
// the plan that turns a promotion into writes, the field diff between two
// stored contracts, and the published form external catalogs read.
//
// A contract document is a decoded JSON value: objects are map[string]any,
// arrays are []any and scalars are strings, numbers, booleans or nil. Adapters
// decode and encode documents; this package never does.
package contractversion

import "time"

// Version is one immutable stored contract. Its identity is (UniqueID,
// ContractHash): the same contract promoted twice, or promoted again after
// another one, is one Version.
type Version struct {
	UniqueID     string
	ContractHash string
	Family       string
	Service      string
	// FirstReleaseID is the release whose promotion first carried this
	// contract into the graph.
	FirstReleaseID string
	// Document is the stored form of the contract.
	Document map[string]any
}

// Promotion is the release promotion a bundle of contracts belongs to.
type Promotion struct {
	TenantID          string
	ReleaseID         string
	ContinuoContracts string
	OntologyVersion   string
	// PromotionSeq is the per-install sequence number release-controller
	// allocates to each promotion. A node's contract watermark is the largest
	// PromotionSeq that reached it.
	PromotionSeq int64
	PromotedAt   time.Time
}

// Incoming is one contract of a promotion's bundle.
type Incoming struct {
	UniqueID     string
	ContractHash string
	ContentHash  string
	Family       string
	Service      string
	// SourcePath is the file the contract was read from.
	SourcePath string
	Document   map[string]any
}

// NodeState is what the graph holds about one node's contract history before
// a promotion writes. A node the graph has no contract for is the zero value.
type NodeState struct {
	UniqueID string
	// Watermark is the largest promotion seq that has reached the node.
	Watermark int64
	// CurrentHash is the contract hash the node's pointer names; empty when
	// the node has no contract yet.
	CurrentHash string
	// CurrentVersion is the counter the pointer carries.
	CurrentVersion int64
	// MaxVersion is the counter of the node's newest promotion edge.
	MaxVersion int64
	// FirstVersionByHash maps each contract hash the node has held to the
	// earliest version number a promotion edge gave that contract.
	FirstVersionByHash map[string]int64
}

// Edge is one promotion that changed a node's current contract.
type Edge struct {
	UniqueID     string
	ContractHash string
	ContentHash  string
	SourcePath   string
	// Version is the counter this change takes.
	Version int64
	// SameAsVersion is, on a revert, the earliest version holding the same
	// contract hash; 0 when no earlier version holds it.
	SameAsVersion int64
}

// WritePlan is what a promotion writes: the contract of every node no newer
// promotion reached as a Version (created only when missing), an Edge for
// each node whose current contract changes, and the counts of the nodes it
// leaves alone.
type WritePlan struct {
	Versions []Version
	Edges    []Edge
	// Superseded counts the nodes a newer promotion already reached.
	Superseded int
	// Unchanged counts the nodes whose current contract is the incoming one.
	Unchanged int
}
