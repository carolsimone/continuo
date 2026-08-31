package release

import (
	"errors"
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
	// StatusValidated is the terminal status for a shadow release: it completed
	// the same parse+validation pipeline as a normal release but stopped short
	// of promoting to production.
	StatusValidated Status = "validated"
)

type Transition struct {
	To Status    `json:"to"`
	At time.Time `json:"at"`
}

// MaxRemediationRounds bounds how many times remediation may be driven for one
// rejected release: the rejection itself plus two "try again"s. Beyond it only a
// new commit — a new release — starts over.
const MaxRemediationRounds = 3

var (
	ErrNotRejected     = errors.New("release is not rejected")
	ErrRoundsExhausted = errors.New("remediation rounds exhausted")
)

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
	UniqueID   string `json:"unique_id"`
	SchemaName string `json:"schema_name"`
	TableName  string `json:"table_name"`
	// ResolvedRelationID is "<schema>.<resolved name>", lowercased: the
	// physical relation this node's build actually writes. UniqueID is keyed
	// on the DECLARED name; this is keyed on the RESOLVED one — a dbt node's
	// alias when it overrides one, else the same declared name. Two nodes
	// with different declared names but the same alias write the same
	// warehouse table, a collision UniqueID alone cannot see. Empty on a
	// payload from before this field existed; DuplicateClaims falls back to
	// UniqueID in that case.
	ResolvedRelationID string   `json:"resolved_relation_id"`
	ServiceName        string   `json:"service_name"`
	NodeType           string   `json:"node_type"`
	ContentHash        string   `json:"content_hash"`
	TestCount          int      `json:"test_count"`
	ImageTag           string   `json:"image_tag"`
	UpstreamUniqueIDs  []string `json:"upstream_unique_ids"`
	Schedule           string   `json:"schedule"`
	OriginalFilePath   string   `json:"original_file_path"`
	// CandidateArtifactURI is an S3 URI pointing to the object the node's
	// validation Job must fetch to build the node as an empty table in the
	// candidate schema: for a dbt node the compiled SQL with schema-qualified
	// references rewritten to the candidate schema, for a python node the
	// validation spec (declared reads plus output columns). Which shape it is
	// follows from NodeType. This is transient validation data and must not be
	// persisted to current_prod or published in the promoted topology.
	CandidateArtifactURI string `json:"candidate_artifact_uri,omitempty"`
}

// WithoutCandidateArtifactURI returns a copy of the topology with per-node
// CandidateArtifactURI cleared. The URI is release-specific transient
// validation data — it must not be persisted to current_prod or published in
// the promoted topology.
func (t Topology) WithoutCandidateArtifactURI() Topology {
	out := make(Topology, len(t))
	for i, n := range t {
		n.CandidateArtifactURI = ""
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
	rejectDetail        string
	failingNodes        []string
	createdAt           time.Time
	parsingStartedAt    *time.Time
	validatingStartedAt *time.Time
	resolvedAt          *time.Time
	transitions         []Transition
	bootstrap           bool
	shadow              bool
	repo                string
	commitSHA           string
	codeBundleURI       string
	manifestKind        ManifestKind
	remediationRound    int
	rejectionPayload    []byte
	sourceOverlayURI    string
	verifiesReleaseID   string
}

// New creates a new Release for a single-service delta. imageTags is initialised
// with just the changed service's tag; it is overwritten with the full assembled
// map in AdvanceQueue when the release transitions to Parsing (see
// SetAssembledImageTags for the rationale). repo (GitHub owner/name) and
// commitSHA (full SHA) record the source change and are immutable after creation.
// kind records how the service's artifact is parsed (dbt manifest.json or python
// contract.yaml) and is immutable after creation. shadow marks a fix-verification
// release posted by agent-remediation: it runs the normal pipeline but stops at
// StatusValidated instead of promoting, and is immutable after creation like
// bootstrap.
func New(id, changedService, imageTag string, bootstrap, shadow bool, repo, commitSHA string, kind ManifestKind, now time.Time) *Release {
	return &Release{
		id:               id,
		status:           StatusReceived,
		imageTags:        map[string]string{changedService: imageTag},
		changedService:   changedService,
		bootstrap:        bootstrap,
		shadow:           shadow,
		repo:             repo,
		commitSHA:        commitSHA,
		createdAt:        now,
		transitions:      []Transition{{To: StatusReceived, At: now}},
		manifestKind:     kind,
		remediationRound: 1,
	}
}

func (r *Release) ID() string                   { return r.id }
func (r *Release) Status() Status               { return r.status }
func (r *Release) ImageTags() map[string]string { return r.imageTags }
func (r *Release) ChangedService() string       { return r.changedService }
func (r *Release) IsBootstrap() bool            { return r.bootstrap }
func (r *Release) IsShadow() bool               { return r.shadow }
func (r *Release) Repo() string                 { return r.repo }
func (r *Release) CommitSHA() string            { return r.commitSHA }
func (r *Release) CodeBundleURI() string        { return r.codeBundleURI }
func (r *Release) ManifestKind() ManifestKind   { return r.manifestKind }

// SetCodeBundleURI records the S3 URI of the release's code-bundle document,
// received with the parse result and carried into release.promoted:v1.
func (r *Release) SetCodeBundleURI(uri string) { r.codeBundleURI = uri }

// SourceOverlayURI locates the tarball of source files a shadow release lays
// over the service's checked-in project before its compile leg runs, so the
// release compiles and validates a proposed fix rather than the committed
// source. Empty for every non-shadow release.
func (r *Release) SourceOverlayURI() string       { return r.sourceOverlayURI }
func (r *Release) SetSourceOverlayURI(uri string) { r.sourceOverlayURI = uri }

// VerifiesReleaseID names the rejected release a shadow release was posted to
// verify a fix for. The shadow carries the fix in its OWN service, which is not
// necessarily the service whose release was rejected — a fix to a downstream
// service repairs an upstream rejection — so the rejected release's changed
// service is assembled from THAT release's candidate rather than from the live
// production pointer. Empty for every non-shadow release, and for a shadow
// posted without naming what it verifies.
func (r *Release) VerifiesReleaseID() string      { return r.verifiesReleaseID }
func (r *Release) SetVerifiesReleaseID(id string) { r.verifiesReleaseID = id }
func (r *Release) CandidateTopology() Topology    { return r.candidateTopology }
func (r *Release) ValidationNodeIDs() []string    { return r.validationNodeIDs }
func (r *Release) RejectReason() string           { return r.rejectReason }

// RejectDetail is the operator-facing explanation of why the release was
// rejected — the same string carried in release.rejected:v1's error_detail.
// Empty when the reject path supplied none.
func (r *Release) RejectDetail() string                   { return r.rejectDetail }
func (r *Release) FailingNodes() []string                 { return r.failingNodes }
func (r *Release) PerNodeResults() []NodeValidationResult { return r.perNodeResults }
func (r *Release) CreatedAt() time.Time                   { return r.createdAt }
func (r *Release) Transitions() []Transition              { return r.transitions }
func (r *Release) RemediationRound() int                  { return r.remediationRound }
func (r *Release) RejectionPayload() []byte               { return r.rejectionPayload }

// SetRejectionPayload records the exact release.rejected:v1 payload emitted for
// this rejection, so a later remediation round can replay it verbatim.
func (r *Release) SetRejectionPayload(p []byte) { r.rejectionPayload = p }

// StartRemediationRound records that a human asked for another remediation
// round on this rejected release and returns the new round number.
func (r *Release) StartRemediationRound(now time.Time) (int, error) {
	if r.status != StatusRejected {
		return 0, ErrNotRejected
	}
	if r.remediationRound >= MaxRemediationRounds {
		return 0, ErrRoundsExhausted
	}
	r.remediationRound++
	r.transitions = append(r.transitions, Transition{To: "remediation_retry", At: now})
	return r.remediationRound, nil
}

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

// TransitionToParsing moves a Received release directly into Parsing at
// activation, skipping the Compiling leg. This is the python path: CI already
// compiled and uploaded the contract artifact before POST /releases, so there
// is no dbt compile step to wait for. The dbt path instead goes through
// TransitionToCompiling and reaches Parsing later, via TransitionFromCompiling.
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
// parsing. This is the dbt path only; a python release has no compile leg and
// activates straight into Parsing via TransitionToParsing instead.
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

// TransitionToPromoted ships a validated candidate to production. A shadow
// release is refused: it carries a fix nobody has reviewed, submitted by
// agent-remediation purely to find out whether the fix passes validation, so
// promoting it would put unreviewed content into production. Its only terminal
// success is Validated (TransitionToValidated).
func (r *Release) TransitionToPromoted(now time.Time) error {
	if r.status != StatusValidating {
		return fmt.Errorf("cannot transition to promoted from %s", r.status)
	}
	if r.shadow {
		return fmt.Errorf("release %s is a shadow release and cannot be promoted", r.id)
	}
	r.status = StatusPromoted
	r.resolvedAt = &now
	r.transitions = append(r.transitions, Transition{To: StatusPromoted, At: now})
	return nil
}

// TransitionToValidated ends a shadow release in Validated: it completed
// validation but, unlike a normal release, stops here instead of promoting.
// Only a shadow release may take it — Validated is terminal and never
// promotes, so a normal release that reached it would stall there forever,
// neither shipped nor rejected.
func (r *Release) TransitionToValidated(now time.Time) error {
	if r.status != StatusValidating {
		return fmt.Errorf("cannot transition to validated from %s", r.status)
	}
	if !r.shadow {
		return fmt.Errorf("release %s is not a shadow release and cannot end in validated", r.id)
	}
	r.status = StatusValidated
	r.resolvedAt = &now
	r.transitions = append(r.transitions, Transition{To: StatusValidated, At: now})
	return nil
}

// TransitionToRejected ends the release in Rejected. detail is the
// operator-facing explanation shown on the release page and carried as
// error_detail on release.rejected:v1; pass "" when the path has none.
func (r *Release) TransitionToRejected(reason, detail string, failingNodes []string, now time.Time) error {
	if r.status != StatusReceived && r.status != StatusCompiling &&
		r.status != StatusParsing && r.status != StatusSeedBuilding && r.status != StatusValidating {
		return fmt.Errorf("cannot transition to rejected from %s", r.status)
	}
	r.status = StatusRejected
	r.rejectReason = reason
	r.rejectDetail = detail
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
	RejectDetail      string
	FailingNodes      []string
	CreatedAt         time.Time
	Transitions       []Transition
	Bootstrap         bool
	Shadow            bool
	Repo              string
	CommitSHA         string
	CodeBundleURI     string
	ManifestKind      ManifestKind
	RemediationRound  int
	RejectionPayload  []byte
	SourceOverlayURI  string
	VerifiesReleaseID string
}

// Rehydrate reconstructs a Release from persistence. Bypasses state-machine
// validation — only repositories should call it.
func Rehydrate(in RehydrateInput) *Release {
	r := &Release{
		id:                in.ID,
		status:            in.Status,
		imageTags:         in.ImageTags,
		changedService:    in.ChangedService,
		candidateTopology: in.CandidateTopology,
		validationNodeIDs: in.ValidationNodeIDs,
		perNodeResults:    in.PerNodeResults,
		rejectReason:      in.RejectReason,
		rejectDetail:      in.RejectDetail,
		failingNodes:      in.FailingNodes,
		createdAt:         in.CreatedAt,
		transitions:       in.Transitions,
		bootstrap:         in.Bootstrap,
		shadow:            in.Shadow,
		repo:              in.Repo,
		commitSHA:         in.CommitSHA,
		codeBundleURI:     in.CodeBundleURI,
		manifestKind:      in.ManifestKind,
		remediationRound:  in.RemediationRound,
		rejectionPayload:  in.RejectionPayload,
		sourceOverlayURI:  in.SourceOverlayURI,
		verifiesReleaseID: in.VerifiesReleaseID,
	}
	if r.remediationRound < 1 {
		r.remediationRound = 1
	}
	return r
}
