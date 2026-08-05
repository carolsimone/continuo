package release

import (
	"fmt"
	"time"
)

type Status string

const (
	StatusReceived     Status = "received"
	StatusCompiling    Status = "compiling"
	StatusParsing      Status = "parsing"
	StatusSeedBuilding Status = "seed_building"
	StatusValidating   Status = "validating"
	StatusPromoted     Status = "promoted"
	StatusRejected     Status = "rejected"
	StatusSuperseded   Status = "superseded"
)

type Transition struct {
	To Status    `json:"to"`
	At time.Time `json:"at"`
}

// NodeValidationResult is the persisted per-node outcome of a pipeline stage.
// Stage identifies which leg produced this result: "compile", "seed_build", or
// "validation". A single release accumulates results across all legs so that
// failure diagnostics are always available regardless of which stage rejected it.
type NodeValidationResult struct {
	// Stage identifies the pipeline leg that produced this result: "compile" |
	// "seed_build" | "validation". The failed unit generalises across legs — it
	// is a dbt node for validation/seed_build, or a service compile unit for the
	// compile leg.
	Stage         string `json:"stage,omitempty"`
	NodeID        string `json:"node_id"`
	Status        string `json:"status"` // "ok" | "failed"
	DBTLogURI     string `json:"dbt_log_uri,omitempty"`
	RunResultsURI string `json:"run_results_uri,omitempty"`
	DurationMS    int64  `json:"duration_ms,omitempty"`
	// FilePath is the optional offending source file path; populated for
	// non-node legs (e.g. compile) where the failure maps to a file rather than
	// a dbt node ID.
	FilePath string `json:"file_path,omitempty"`
}

type Topology []Node

type Node struct {
	UniqueID          string   `json:"unique_id"`
	SchemaName        string   `json:"schema_name"`
	TableName         string   `json:"table_name"`
	ServiceName       string   `json:"service_name"`
	NodeType          string   `json:"node_type"`
	ContentHash       string   `json:"content_hash"`
	TestCount         int      `json:"test_count"`
	ImageTag          string   `json:"image_tag"`
	UpstreamUniqueIDs []string `json:"upstream_unique_ids"`
	Schedule          string   `json:"schedule"`
	OriginalFilePath  string   `json:"original_file_path"`
	// CandidateSQLURI is an S3 URI pointing to the node's compiled SQL with
	// schema-qualified references rewritten to the candidate schema (produced by
	// manifest-controller). It is carried only into validation.requested so the
	// executor can fetch and execute the SQL to build the node as an empty table
	// in the candidate schema. This is transient validation data and must not be
	// persisted to current_prod or published in the promoted topology.
	CandidateSQLURI string `json:"candidate_sql_uri,omitempty"`
}

// WithoutCandidateSQLURI returns a copy of the topology with per-node
// CandidateSQLURI cleared. The URI is release-specific transient validation
// data — it must not be persisted to current_prod or published in the promoted
// topology.
func (t Topology) WithoutCandidateSQLURI() Topology {
	out := make(Topology, len(t))
	for i, n := range t {
		n.CandidateSQLURI = ""
		out[i] = n
	}
	return out
}

type Release struct {
	id                  string
	status              Status
	imageTags           map[string]string
	changedService      string
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
	repo                string
	commitSHA           string
	codeBundleURI       string
}

// New creates a new Release for a single-service delta. imageTags is initialised
// with just the changed service's tag; it is overwritten with the full assembled
// map in AdvanceQueue when the release transitions to Parsing (see
// SetAssembledImageTags for the rationale). repo (GitHub owner/name) and
// commitSHA (full SHA) record the source change and are immutable after creation.
func New(id, changedService, imageTag string, bootstrap bool, repo, commitSHA string, now time.Time) *Release {
	return &Release{
		id:             id,
		status:         StatusReceived,
		imageTags:      map[string]string{changedService: imageTag},
		changedService: changedService,
		bootstrap:      bootstrap,
		repo:           repo,
		commitSHA:      commitSHA,
		createdAt:      now,
		transitions:    []Transition{{To: StatusReceived, At: now}},
	}
}

func (r *Release) ID() string                   { return r.id }
func (r *Release) Status() Status               { return r.status }
func (r *Release) ImageTags() map[string]string { return r.imageTags }
func (r *Release) ChangedService() string       { return r.changedService }
func (r *Release) IsBootstrap() bool            { return r.bootstrap }
func (r *Release) Repo() string                 { return r.repo }
func (r *Release) CommitSHA() string            { return r.commitSHA }
func (r *Release) CodeBundleURI() string        { return r.codeBundleURI }

// SetCodeBundleURI records the S3 URI of the release's code-bundle document,
// received with the parse result and carried into release.promoted:v1.
func (r *Release) SetCodeBundleURI(uri string)            { r.codeBundleURI = uri }
func (r *Release) CandidateTopology() Topology            { return r.candidateTopology }
func (r *Release) ValidationNodeIDs() []string            { return r.validationNodeIDs }
func (r *Release) RejectReason() string                   { return r.rejectReason }
func (r *Release) FailingNodes() []string                 { return r.failingNodes }
func (r *Release) PerNodeResults() []NodeValidationResult { return r.perNodeResults }
func (r *Release) CreatedAt() time.Time                   { return r.createdAt }
func (r *Release) Transitions() []Transition              { return r.transitions }

// SetAssembledImageTags replaces the image-tags map with the fully assembled set
// built from every service's current service_prod pointer. This is called in
// AdvanceQueue (when a Received release transitions to Parsing) rather than at
// receive time, because the other services' pointers can change as earlier-queued
// releases are promoted. Reading them at advance-time guarantees we see the live
// state for all OTHER services, not a stale snapshot from when this release was
// first enqueued.
func (r *Release) SetAssembledImageTags(tags map[string]string) {
	r.imageTags = tags
}

// RecordStageResults replaces all per-node results for the given stage with
// the supplied slice, leaving results from other stages intact. Calling it
// twice for the same stage is idempotent across re-delivery of a single leg's
// aggregate event (second call replaces, not appends).
func (r *Release) RecordStageResults(stage string, results []NodeValidationResult) {
	kept := r.perNodeResults[:0:0]
	for _, n := range r.perNodeResults {
		if n.Stage != stage {
			kept = append(kept, n)
		}
	}
	for _, n := range results {
		n.Stage = stage
		kept = append(kept, n)
	}
	r.perNodeResults = kept
}

// UpsertStageResult adds or replaces a single per-node result identified by
// (stage, node_id), leaving every other entry intact. It backs the incremental
// projection: each kind:"node" message on validation.result:v1 upserts exactly
// one node, so the read model fills in as nodes settle. Re-delivery or a retry
// re-emission of the same node replaces in place (last-write), never appends a
// duplicate.
func (r *Release) UpsertStageResult(stage string, result NodeValidationResult) {
	result.Stage = stage
	for i, n := range r.perNodeResults {
		if n.Stage == stage && n.NodeID == result.NodeID {
			r.perNodeResults[i] = result
			return
		}
	}
	r.perNodeResults = append(r.perNodeResults, result)
}

// RecordValidationResults stores the per-node validation outcomes on the
// aggregate. Thin wrapper around RecordStageResults("validation", results).
// Called before the promote/reject branch so both paths persist them.
func (r *Release) RecordValidationResults(results []NodeValidationResult) {
	r.RecordStageResults("validation", results)
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

// TransitionToCompiling moves a Received release into Compiling — the leg where
// continuo runs the changed service's dbt compile to produce its manifest before
// parsing. Replaces the direct Received->Parsing step at activation.
func (r *Release) TransitionToCompiling(now time.Time) error {
	if r.status != StatusReceived {
		return fmt.Errorf("cannot transition to compiling from %s", r.status)
	}
	r.status = StatusCompiling
	r.transitions = append(r.transitions, Transition{To: StatusCompiling, At: now})
	return nil
}

// TransitionFromCompiling advances a Compiling release to Parsing once the
// manifest has been compiled + uploaded.
func (r *Release) TransitionFromCompiling(now time.Time) error {
	if r.status != StatusCompiling {
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

// TransitionToSeedBuilding moves a Parsing release into SeedBuilding and records
// the candidate topology + validation node IDs (the same data Validating needs),
// so the later seed.build.completed handler can emit validation.requested without
// re-deriving them. Emitted when the changed-closure contains new/changed seeds
// that must be built into the candidate schema before validation.
func (r *Release) TransitionToSeedBuilding(topology Topology, validationNodeIDs []string, now time.Time) error {
	if r.status != StatusParsing {
		return fmt.Errorf("cannot transition to seed_building from %s", r.status)
	}
	r.status = StatusSeedBuilding
	r.candidateTopology = topology
	r.validationNodeIDs = validationNodeIDs
	r.transitions = append(r.transitions, Transition{To: StatusSeedBuilding, At: now})
	return nil
}

// TransitionFromSeedBuilding advances a SeedBuilding release to Validating once
// candidate seeds are built. It narrows the persisted validation set to
// validationNodeIDs — the nodes actually sent to the validation leg, which
// excludes the just-built seeds (already materialised in the candidate schema).
// This keeps ValidationNodeIDs() equal to what the executor validates, so the
// terminal decision's failing/audit reflect exactly the validated nodes. It
// also stamps validatingStartedAt; the candidate topology is already set by
// TransitionToSeedBuilding.
func (r *Release) TransitionFromSeedBuilding(validationNodeIDs []string, now time.Time) error {
	if r.status != StatusSeedBuilding {
		return fmt.Errorf("cannot transition to validating from %s", r.status)
	}
	r.status = StatusValidating
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
	if r.status != StatusReceived && r.status != StatusCompiling &&
		r.status != StatusParsing && r.status != StatusSeedBuilding && r.status != StatusValidating {
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
	ChangedService    string
	CandidateTopology Topology
	ValidationNodeIDs []string
	PerNodeResults    []NodeValidationResult
	RejectReason      string
	FailingNodes      []string
	CreatedAt         time.Time
	Transitions       []Transition
	Bootstrap         bool
	Repo              string
	CommitSHA         string
	CodeBundleURI     string
}

// Rehydrate reconstructs a Release from persistence. Bypasses state-machine
// validation — only repositories should call it.
func Rehydrate(in RehydrateInput) *Release {
	return &Release{
		id:                in.ID,
		status:            in.Status,
		imageTags:         in.ImageTags,
		changedService:    in.ChangedService,
		candidateTopology: in.CandidateTopology,
		validationNodeIDs: in.ValidationNodeIDs,
		perNodeResults:    in.PerNodeResults,
		rejectReason:      in.RejectReason,
		failingNodes:      in.FailingNodes,
		createdAt:         in.CreatedAt,
		transitions:       in.Transitions,
		bootstrap:         in.Bootstrap,
		repo:              in.Repo,
		commitSHA:         in.CommitSHA,
		codeBundleURI:     in.CodeBundleURI,
	}
}
