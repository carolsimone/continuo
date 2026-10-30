// Package codeversion holds the vocabulary of the code-version history behind
// the :Node topology: what code a node ran, which shared code was in scope, and
// the release that promoted it.
package codeversion

import (
	"time"

	"github.com/carolsimone/continuo/catalog/domain/topology"
)

// UnitRef identifies one exact shared-code version a node version used.
type UnitRef struct {
	UnitID   string
	Checksum string
}

// NodeVersion is one immutable code state of a node. Its identity is
// (UniqueID, ContentHash): the same code promoted twice is one version, and a
// revert points the node back at the version that already exists.
type NodeVersion struct {
	UniqueID       string
	ContentHash    string
	SourceHash     string
	SharedCodeHash string
	ConfigHash     string
	Runtime        string
	RawCode        string
	CompiledCode   string
	// CompiledTruncated marks compiled code cut down to the ingestion size
	// guard: the stored text is a prefix, not the whole compilation.
	CompiledTruncated bool
	// ConfigJSON is the node's full resolved config, canonically encoded, so a
	// reader can see a materialization or partition change without diffing SQL.
	ConfigJSON string
	// Healed marks a version whose commit and release stamps are approximate:
	// the release that carried it into the graph is not the one that authored it.
	Healed   bool
	UnitRefs []UnitRef
}

// CodeUnitVersion is one immutable state of a shared-code unit — a dbt macro
// today, a Python module later — identified by (UnitID, Checksum).
type CodeUnitVersion struct {
	UnitID   string
	Checksum string
	Source   string
}

// WriteInput is one batch of a release's version ingestion. Nodes carries
// bundle nodes of the release: which of them produce a version is decided
// against the graph, not here.
type WriteInput struct {
	ReleaseID string
	// PromotionSeq is the promotion this ingestion belongs to. It is the
	// version watermark of every node and shared-code unit the batch matches:
	// a node's :CURRENT moves only while its watermark equals this seq.
	PromotionSeq int64
	Repo         string
	CommitSHA    string
	PromotedAt   time.Time
	Nodes        []NodeVersion
	Units        []CodeUnitVersion
	// Topology holds the release's nodes as the topology swap writes them,
	// keyed by unique_id. A node of Nodes the graph does not hold yet is created
	// inactive from its entry; a node of Nodes with no entry is skipped.
	Topology map[string]topology.ReleasePromotedTopologyNode
}

// WriteResult reports what a write actually did, so the caller can log it.
type WriteResult struct {
	// NodesCreated counts the inactive :Node created for bundle nodes the graph
	// did not hold yet.
	NodesCreated         int
	NodeVersionsCreated  int
	UnitVersionsCreated  int
	CurrentPointersMoved int
	// RejectionsResolved counts open :Rejection nodes this write linked to a
	// newly-current version via [:RESOLVED_BY].
	RejectionsResolved int
}
