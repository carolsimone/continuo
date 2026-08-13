// File: orchestrator/domain/codeversion/read.go
//
// Read-side view types over the code-version graph. They mirror
// NodeVersion/CodeUnitVersion but shaped for a reader: chain position, diffs,
// and run provenance rather than write-path identity.
package codeversion

import "time"

// VersionView is one recorded code state as a reader sees it: the identity and
// hash parts, the provenance stamp, and the code itself.
type VersionView struct {
	UniqueID          string
	VersionSeq        int64
	ContentHash       string
	SourceHash        string
	SharedCodeHash    string
	ConfigHash        string
	Runtime           string
	RawCode           string
	CompiledCode      string
	CompiledTruncated bool
	ConfigJSON        string
	Repo              string
	CommitSHA         string
	ReleaseID         string
	PromotedAt        time.Time
	Healed            bool
	Backfilled        bool
	// IsCurrent marks the version the node runs now. Exactly one version in a
	// chain walk carries it; a node whose :Table was retired has none.
	IsCurrent bool
}

// VersionDiff is a server-side comparison of two versions of one node. The diffs
// are rendered by the service so every frontend shows the same thing.
type VersionDiff struct {
	UniqueID          string
	From              VersionView
	To                VersionView
	RawCodeDiff       string // unified diff, "" when the source is unchanged
	ConfigDiff        string // unified diff over canonical config JSON, "" when unchanged
	SourceChanged     bool
	SharedCodeChanged bool
	ConfigChanged     bool
	// Truncated marks a diff cut to the per-diff byte cap.
	Truncated bool
}

// AncestorVersions is one upstream node with its most recent versions, newest
// first (at most two: the latest and the one before it).
type AncestorVersions struct {
	UniqueID string
	Depth    int32
	Versions []VersionView
}

// UpstreamChange is one ancestor of a node together with its most recent code
// change, ordered most-recently-changed first.
type UpstreamChange struct {
	UniqueID string
	Depth    int32
	Diff     VersionDiff
}

// UnitVersionView is one recorded state of a shared-code unit.
type UnitVersionView struct {
	UnitID     string
	Checksum   string
	Source     string
	Repo       string
	CommitSHA  string
	ReleaseID  string
	PromotedAt time.Time
	IsCurrent  bool
}

// RunExecution is one run that executed a node, with the code it actually ran.
type RunExecution struct {
	RunID        string
	TaskID       string
	Status       string
	ScheduleName string
	Operation    string
	ImageTag     string
	// ContentHash is the code the run executed, stamped on the :EXECUTES edge at
	// snapshot time. Empty for runs that predate the stamp.
	ContentHash string
	CreatedAt   time.Time
	CompletedAt time.Time
}
