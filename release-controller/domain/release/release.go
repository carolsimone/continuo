package release

import (
	"fmt"
	"time"
)

type Status string

const (
	StatusReceived   Status = "received"
	StatusParsing    Status = "parsing"
	StatusValidating Status = "validating"
	StatusPromoted   Status = "promoted"
	StatusRejected   Status = "rejected"
	StatusSuperseded Status = "superseded"
)

type Transition struct {
	To Status    `json:"to"`
	At time.Time `json:"at"`
}

// NodeValidationResult is the persisted per-node outcome of a candidate's dbt
// validation run. It is recorded for both the promote and reject paths.
type NodeValidationResult struct {
	NodeID     string `json:"node_id"`
	Status     string `json:"status"` // "ok" | "failed"
	DBTLogURI  string `json:"dbt_log_uri,omitempty"`
	DurationMS int64  `json:"duration_ms,omitempty"`
}

type Topology []Node

type Node struct {
	UniqueID          string   `json:"unique_id"`
	SchemaName        string   `json:"schema_name"`
	TableName         string   `json:"table_name"`
	ServiceName       string   `json:"service_name"`
	NodeType          string   `json:"node_type"`
	ContentHash       string   `json:"content_hash"`
	ImageTag          string   `json:"image_tag"`
	UpstreamUniqueIDs []string `json:"upstream_unique_ids"`
	Schedule          string   `json:"schedule"`
}

type Release struct {
	id                  string
	status              Status
	imageTags           map[string]string
	manifestsURI        string
	candidateTopology   Topology
	validationNodeIDs   []string
	perNodeResults      []NodeValidationResult
	rejectReason        string
	failingNodes        []string
	createdAt           time.Time
	parsingStartedAt    *time.Time
	validatingStartedAt *time.Time
	resolvedAt          *time.Time
	transitions         []Transition
	bootstrap           bool
}

func New(id string, imageTags map[string]string, manifestsURI string, bootstrap bool, now time.Time) *Release {
	return &Release{
		id:           id,
		status:       StatusReceived,
		imageTags:    imageTags,
		manifestsURI: manifestsURI,
		bootstrap:    bootstrap,
		createdAt:    now,
		transitions:  []Transition{{To: StatusReceived, At: now}},
	}
}

func (r *Release) ID() string                             { return r.id }
func (r *Release) Status() Status                         { return r.status }
func (r *Release) ImageTags() map[string]string           { return r.imageTags }
func (r *Release) ManifestsURI() string                   { return r.manifestsURI }
func (r *Release) IsBootstrap() bool                      { return r.bootstrap }
func (r *Release) CandidateTopology() Topology            { return r.candidateTopology }
func (r *Release) ValidationNodeIDs() []string            { return r.validationNodeIDs }
func (r *Release) RejectReason() string                   { return r.rejectReason }
func (r *Release) FailingNodes() []string                 { return r.failingNodes }
func (r *Release) PerNodeResults() []NodeValidationResult { return r.perNodeResults }
func (r *Release) CreatedAt() time.Time                   { return r.createdAt }
func (r *Release) Transitions() []Transition              { return r.transitions }

// RecordValidationResults stores the per-node validation outcomes on the
// aggregate. Called before the promote/reject branch so both paths persist them.
func (r *Release) RecordValidationResults(results []NodeValidationResult) {
	r.perNodeResults = results
}

func (r *Release) TransitionToParsing(now time.Time) error {
	if r.status != StatusReceived {
		return fmt.Errorf("cannot transition to parsing from %s", r.status)
	}
	r.status = StatusParsing
	r.parsingStartedAt = &now
	r.transitions = append(r.transitions, Transition{To: StatusParsing, At: now})
	return nil
}

func (r *Release) TransitionToValidating(topology Topology, validationNodeIDs []string, now time.Time) error {
	if r.status != StatusParsing {
		return fmt.Errorf("cannot transition to validating from %s", r.status)
	}
	r.status = StatusValidating
	r.candidateTopology = topology
	r.validationNodeIDs = validationNodeIDs
	r.validatingStartedAt = &now
	r.transitions = append(r.transitions, Transition{To: StatusValidating, At: now})
	return nil
}

func (r *Release) TransitionToPromoted(now time.Time) error {
	if r.status != StatusValidating {
		return fmt.Errorf("cannot transition to promoted from %s", r.status)
	}
	r.status = StatusPromoted
	r.resolvedAt = &now
	r.transitions = append(r.transitions, Transition{To: StatusPromoted, At: now})
	return nil
}

func (r *Release) TransitionToRejected(reason string, failingNodes []string, now time.Time) error {
	if r.status != StatusReceived && r.status != StatusParsing && r.status != StatusValidating {
		return fmt.Errorf("cannot transition to rejected from %s", r.status)
	}
	r.status = StatusRejected
	r.rejectReason = reason
	r.failingNodes = failingNodes
	r.resolvedAt = &now
	r.transitions = append(r.transitions, Transition{To: StatusRejected, At: now})
	return nil
}

// RehydrateInput carries all persisted fields needed to reconstruct a Release
// from storage without re-running state-machine validation.
type RehydrateInput struct {
	ID                string
	Status            Status
	ImageTags         map[string]string
	ManifestsURI      string
	CandidateTopology Topology
	ValidationNodeIDs []string
	PerNodeResults    []NodeValidationResult
	RejectReason      string
	FailingNodes      []string
	CreatedAt         time.Time
	Transitions       []Transition
	Bootstrap         bool
}

// Rehydrate reconstructs a Release from persistence. Bypasses state-machine
// validation — only repositories should call it.
func Rehydrate(in RehydrateInput) *Release {
	return &Release{
		id:                in.ID,
		status:            in.Status,
		imageTags:         in.ImageTags,
		manifestsURI:      in.ManifestsURI,
		candidateTopology: in.CandidateTopology,
		validationNodeIDs: in.ValidationNodeIDs,
		perNodeResults:    in.PerNodeResults,
		rejectReason:      in.RejectReason,
		failingNodes:      in.FailingNodes,
		createdAt:         in.CreatedAt,
		transitions:       in.Transitions,
		bootstrap:         in.Bootstrap,
	}
}
