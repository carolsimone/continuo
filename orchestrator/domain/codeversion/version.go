// Package codeversion holds the vocabulary of the code-version history behind
// the :Table topology: what code a node ran, which shared code was in scope, and
// the release that promoted it.
package codeversion

import "time"

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

// WriteInput is one release's complete version-ingestion request. Nodes carries
// EVERY node in the release's code bundle: which of them actually produce a
// version is decided against the graph, not here.
type WriteInput struct {
	ReleaseID  string
	Repo       string
	CommitSHA  string
	PromotedAt time.Time
	Nodes      []NodeVersion
	Units      []CodeUnitVersion
}

// WriteResult reports what a write actually did, so the caller can log it and
// decide whether the release's topology swap has landed yet.
type WriteResult struct {
	NodeVersionsCreated  int
	UnitVersionsCreated  int
	CurrentPointersMoved int
	// UnmatchedNodeIDs are bundle nodes with no :Table in the graph.
	UnmatchedNodeIDs []string
	// GraphReleaseID is the release the topology currently reflects — empty on a
	// graph that has never been promoted to.
	GraphReleaseID string
}
