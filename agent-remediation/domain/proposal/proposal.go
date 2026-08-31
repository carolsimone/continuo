// Package proposal holds the domain model for a remediation fix proposal: the
// suggested replacement SQL for a failed dbt node, its rationale, and the
// audit status of the attempt.
package proposal

import (
	"sort"
	"time"
)

// Status is the lifecycle outcome of one proposal attempt.
type Status string

const (
	// StatusGenerating is the in-flight state: the agent has committed to calling
	// the model for a healable failure but the attempt has not yet resolved. It is
	// finalized to one of the terminal states below once the outcome is known.
	StatusGenerating Status = "generating"
	// StatusVerifying is the in-flight state that follows StatusGenerating for a
	// fix whose correctness cannot be judged synchronously: the agent has
	// proposed a fix and posted a shadow release to run it through the full
	// parse -> candidate-schema -> validation pipeline. The row carries the
	// shadow release's id (ShadowReleaseID) and the raw trigger payload
	// (TriggerPayload) so an asynchronous reconciler can poll the release and
	// finalize the attempt to 'proposed' (the fix verified) or 'failed' (with
	// the shadow's error recorded as evidence for the next attempt).
	StatusVerifying Status = "verifying"
	StatusProposed  Status = "proposed"
	StatusSkipped   Status = "skipped"
	StatusFailed    Status = "failed"
	StatusEscalated Status = "escalated"
)

// FileEdit is one proposed change to a single file: its repository-relative
// path and the S3 URIs of the proposed content and unified diff.
type FileEdit struct {
	Path       string
	ContentURI string
	DiffURI    string
	// TargetNodeID is the node whose source this edit changes; for a
	// shared-upstream fix it is the changed ancestor, which may not itself
	// have failed.
	TargetNodeID string
}

// NodeOutcome is how one failing node's attempt ended: verifying, proposed,
// failed, skipped, or escalated, with the reason for that outcome.
type NodeOutcome struct {
	Status Status
	Reason string
}

// Verification is one shadow release posted to verify the edits made to a
// single service.
type Verification struct {
	Service         string
	Kind            string
	ShadowReleaseID string
}

// Confidence is the model's self-reported confidence in the proposed fix.
type Confidence string

const (
	ConfidenceLow    Confidence = "low"
	ConfidenceMedium Confidence = "medium"
	ConfidenceHigh   Confidence = "high"
)

// Proposal is the append-only record of one fix-proposal attempt for a
// release. One row per attempt; unique on (release_id, attempt).
type Proposal struct {
	Source    string
	ReleaseID string
	// RemediationRound is the release's remediation round this attempt belongs to; the attempt cap is counted within a round.
	RemediationRound int
	NodeID           string
	// ResolvedNodeIDs is the failing nodes this attempt addresses, sorted;
	// NodeID is their representative.
	ResolvedNodeIDs []string
	ErrorSignature  string
	Attempt         int
	Status          Status
	// NodeOutcomes is, per failing node, how this attempt ended for it — a
	// cluster whose fixer skipped or failed leaves its members
	// skipped/failed while other members verify.
	NodeOutcomes map[string]NodeOutcome
	// Verifications is one shadow release per edited service;
	// ShadowReleaseID is the view of the first.
	Verifications []Verification
	// ShadowReleaseID is the id of the shadow release posted to verify this
	// attempt's fix, written when Status is (or was) StatusVerifying. It is
	// kept when the attempt is finalized, so a resolved row still names the
	// release that judged it. Empty for an attempt that never entered
	// verification.
	ShadowReleaseID string
	// TriggerPayload is the raw remediation.requested:v1 payload that drove
	// this attempt, written when Status is (or was) StatusVerifying so a
	// reconciler can rebuild the trigger and retry with the shadow release's
	// error as new evidence. It is kept when the attempt is finalized —
	// rebuilding the retry reads it from a row that has already moved to
	// 'failed'. Empty for an attempt that never entered verification.
	TriggerPayload []byte
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
	// file was fetched (e.g. "owner/continuo-demo"). Empty when not resolved.
	Repo string
	// CommitSHA is the git commit hash at which the source file was fetched.
	// Empty when not resolved.
	CommitSHA string
	// FilePath is the repository-relative path of the dbt model source file
	// (e.g. "services/service-3/models/orders_d.sql"). Empty when not resolved.
	FilePath string
	// Edits is the list of proposed file changes for this attempt. An empty
	// list means the proposal is described only by the single-file scalar
	// fields above (FilePath, ProposedSQLURI, DiffURI); a non-empty list is
	// the proposal's full multi-file description, of which those scalar
	// fields are the single-file view (edits[0]).
	Edits     []FileEdit
	Model     string
	CreatedAt time.Time
}

// NormalizeSingleFileView enforces that FilePath, ProposedSQLURI, and DiffURI
// are the single-file view of the proposal: edits[0] when Edits is
// non-empty, otherwise the candidate-only fix artifact already held in those
// fields. When Edits is non-empty and its first entry names a path, it
// overwrites the three scalars with Edits[0]'s Path, ContentURI, and
// DiffURI, so they cannot name a different file or artifact than the first
// edit. When Edits is empty, or its first entry's Path is empty (nothing
// validates a FileEdit before it reaches here), it leaves the scalars
// untouched rather than overwriting a possibly-correct value with a blank
// one.
func (p *Proposal) NormalizeSingleFileView() {
	if len(p.Edits) == 0 {
		return
	}
	first := p.Edits[0]
	if first.Path == "" {
		return
	}
	p.FilePath = first.Path
	p.ProposedSQLURI = first.ContentURI
	p.DiffURI = first.DiffURI
}

// NormalizeRepresentativeViews derives the single-value views of the batched
// fields: ResolvedNodeIDs is sorted, NodeID defaults to its first entry,
// ShadowReleaseID defaults to the first verification's release, and the
// single-file scalars follow edits[0]. Every writer calls it before the
// aggregate is persisted or turned into an event, so the row and the event
// cannot disagree about which node or release represents the attempt.
func (p *Proposal) NormalizeRepresentativeViews() {
	sort.Strings(p.ResolvedNodeIDs)
	if p.NodeID == "" && len(p.ResolvedNodeIDs) > 0 {
		p.NodeID = p.ResolvedNodeIDs[0]
	}
	if p.ShadowReleaseID == "" && len(p.Verifications) > 0 {
		p.ShadowReleaseID = p.Verifications[0].ShadowReleaseID
	}
	p.NormalizeSingleFileView()
}
