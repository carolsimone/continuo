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

type Topology []Node

type Node struct {
	UniqueID          string   `json:"unique_id"`
	SchemaName        string   `json:"schema_name"`
	TableName         string   `json:"table_name"`
	ServiceName       string   `json:"service_name"`
	ImageTag          string   `json:"image_tag"`
	UpstreamUniqueIDs []string `json:"upstream_unique_ids"`
	Schedule          string   `json:"schedule"`
}

type Release struct {
	id                  string
	status              Status
	changedNodeIDs      []string
	imageTags           map[string]string
	manifestsURI        string
	candidateTopology   Topology
	validationNodeIDs   []string
	perNodeResults      map[string]string
	rejectReason        string
	failingNodes        []string
	dbtLogsURI          string
	createdAt           time.Time
	parsingStartedAt    *time.Time
	validatingStartedAt *time.Time
	resolvedAt          *time.Time
	transitions         []Transition
}

func New(id string, changedNodeIDs []string, imageTags map[string]string, manifestsURI string, now time.Time) *Release {
	return &Release{
		id:             id,
		status:         StatusReceived,
		changedNodeIDs: changedNodeIDs,
		imageTags:      imageTags,
		manifestsURI:   manifestsURI,
		createdAt:      now,
		transitions:    []Transition{{To: StatusReceived, At: now}},
	}
}

func (r *Release) ID() string                   { return r.id }
func (r *Release) Status() Status               { return r.status }
func (r *Release) ChangedNodeIDs() []string     { return r.changedNodeIDs }
func (r *Release) ImageTags() map[string]string { return r.imageTags }
func (r *Release) ManifestsURI() string         { return r.manifestsURI }
func (r *Release) CandidateTopology() Topology  { return r.candidateTopology }
func (r *Release) ValidationNodeIDs() []string  { return r.validationNodeIDs }
func (r *Release) RejectReason() string         { return r.rejectReason }
func (r *Release) FailingNodes() []string       { return r.failingNodes }
func (r *Release) DBTLogsURI() string           { return r.dbtLogsURI }
func (r *Release) CreatedAt() time.Time         { return r.createdAt }
func (r *Release) Transitions() []Transition    { return r.transitions }

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

func (r *Release) TransitionToRejected(reason string, failingNodes []string, dbtLogsURI string, now time.Time) error {
	if r.status != StatusReceived && r.status != StatusParsing && r.status != StatusValidating {
		return fmt.Errorf("cannot transition to rejected from %s", r.status)
	}
	r.status = StatusRejected
	r.rejectReason = reason
	r.failingNodes = failingNodes
	r.dbtLogsURI = dbtLogsURI
	r.resolvedAt = &now
	r.transitions = append(r.transitions, Transition{To: StatusRejected, At: now})
	return nil
}
