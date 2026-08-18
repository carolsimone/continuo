// Package proposal holds the domain model for a remediation fix proposal: the
// suggested replacement SQL for a failed dbt node, its rationale, and the
// audit status of the attempt.
package proposal

import "time"

// Status is the lifecycle outcome of one proposal attempt.
type Status string

const (
	// StatusGenerating is the in-flight state: the agent has committed to calling
	// the model for a healable failure but the attempt has not yet resolved. It is
	// finalized to one of the terminal states below once the outcome is known.
	StatusGenerating Status = "generating"
	StatusProposed   Status = "proposed"
	StatusSkipped    Status = "skipped"
	StatusFailed     Status = "failed"
	StatusEscalated  Status = "escalated"
)

// FileEdit is one proposed change to a single file: its repository-relative
// path and the S3 URIs of the proposed content and unified diff.
type FileEdit struct {
	Path       string
	ContentURI string
	DiffURI    string
}

// Confidence is the model's self-reported confidence in the proposed fix.
type Confidence string

const (
	ConfidenceLow    Confidence = "low"
	ConfidenceMedium Confidence = "medium"
	ConfidenceHigh   Confidence = "high"
)

// Proposal is the append-only record of one fix-proposal attempt for a failed
// node. One row per attempt; unique on (release_id, node_id, attempt).
type Proposal struct {
	Source         string
	ReleaseID      string
	NodeID         string
	ErrorSignature string
	Attempt        int
	Status         Status
	Confidence     Confidence
	Rationale      string
	ProposedSQLURI string
	DiffURI        string
	// CandidateFixSQLURI is the S3 URI of the SQL fix generated from the real
	// model source fetched from version control (empty when the step was skipped).
	CandidateFixSQLURI string
	// CandidateFixDiffURI is the S3 URI of the unified diff between the
	// original source and the candidate fix (empty when the step was skipped).
	CandidateFixDiffURI string
	// SourceResolved indicates that the real-source fix step produced a
	// confident result; false when the step was skipped or inconclusive.
	SourceResolved bool
	// Repo is the version-control repository (owner/name) from which the source
	// file was fetched (e.g. "owner/continuo-dbt-demo"). Empty when not resolved.
	Repo string
	// CommitSHA is the git commit hash at which the source file was fetched.
	// Empty when not resolved.
	CommitSHA string
	// FilePath is the repository-relative path of the dbt model source file
	// (e.g. "services/service-3/models/orders_d.sql"). Empty when not resolved.
	FilePath string
	// Edits is the list of proposed file changes for this attempt. A row
	// written before this field existed has an empty list; readers fall back
	// to the single-file scalar fields above (FilePath, ProposedSQLURI,
	// DiffURI) to synthesize one edit.
	Edits     []FileEdit
	Model     string
	CreatedAt time.Time
}
