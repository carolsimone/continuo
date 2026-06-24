// Package proposal holds the domain model for a remediation fix proposal: the
// suggested replacement SQL for a failed dbt node, its rationale, and the
// audit status of the attempt.
package proposal

import "time"

// Status is the lifecycle outcome of one proposal attempt.
type Status string

const (
	StatusProposed  Status = "proposed"
	StatusSkipped   Status = "skipped"
	StatusFailed    Status = "failed"
	StatusEscalated Status = "escalated"
)

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
	Source              string
	ReleaseID           string
	NodeID              string
	ErrorSignature      string
	Attempt             int
	Status              Status
	Confidence          Confidence
	Rationale           string
	ProposedSQLURI      string
	DiffURI             string
	// CandidateFixSQLURI is the S3 URI of the SQL fix generated from the real
	// model source fetched from version control (empty when the step was skipped).
	CandidateFixSQLURI  string
	// CandidateFixDiffURI is the S3 URI of the unified diff between the
	// original source and the candidate fix (empty when the step was skipped).
	CandidateFixDiffURI string
	// SourceResolved indicates that the real-source fix step produced a
	// confident result; false when the step was skipped or inconclusive.
	SourceResolved      bool
	Model               string
	CreatedAt           time.Time
}
