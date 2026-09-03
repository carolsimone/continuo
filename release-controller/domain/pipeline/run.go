// Package pipeline holds the aggregate every leg of release-controller's
// compile → parse → seed build → validate pipeline drives: a Run.
//
// A run is one of two kinds. A candidate is a release a team's CI posted; it
// promotes to production when it passes. A verification is a run
// agent-remediation posted to find out whether a proposed fix holds; it
// passes or fails and never touches production. Both kinds share the legs,
// the queue, the per-node results, and the transition history. The kind
// decides only which entry facts the run carries and which terminal statuses
// it can reach.
package pipeline

import (
	"errors"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// Kind says why a run exists.
type Kind string

const (
	KindCandidate    Kind = "candidate"
	KindVerification Kind = "verification"
)

// Status is where a run is in the pipeline. The five non-terminal statuses
// are shared; the terminal ones belong to one kind each.
type Status string

const (
	StatusReceived     Status = "received"
	StatusCompiling    Status = "compiling"
	StatusParsing      Status = "parsing"
	StatusSeedBuilding Status = "seed_building"
	StatusValidating   Status = "validating"
	// Candidate terminals.
	StatusPromoted   Status = "promoted"
	StatusRejected   Status = "rejected"
	StatusSuperseded Status = "superseded"
	// Verification terminals.
	StatusPassed Status = "passed"
	StatusFailed Status = "failed"
)

// IsTerminal reports whether s ends a run of either kind.
func (s Status) IsTerminal() bool {
	switch s {
	case StatusPromoted, StatusRejected, StatusSuperseded, StatusPassed, StatusFailed:
		return true
	}
	return false
}

// MaxRemediationRounds bounds how many times remediation may be driven for one
// rejected candidate: the rejection itself plus two "try again"s. Beyond it
// only a new commit — a new candidate — starts over.
const MaxRemediationRounds = 3

var (
	// ErrWrongKind is returned by a method that only one kind of run may take.
	ErrWrongKind       = errors.New("operation is not valid for this kind of run")
	ErrNotRejected     = errors.New("release is not rejected")
	ErrRoundsExhausted = errors.New("remediation rounds exhausted")
)

// Transition is one entry of a run's status history.
type Transition struct {
	To Status    `json:"to"`
	At time.Time `json:"at"`
}

// NodeValidationResult is the persisted per-node outcome of a pipeline stage.
// Stage identifies which leg produced it: "compile", "seed_build", or
// "validation". A run accumulates results across all legs so failure
// diagnostics are always available regardless of which stage ended it.
type NodeValidationResult struct {
	// Stage is "compile" | "seed_build" | "validation". The failed unit
	// generalises across legs — a dbt node for validation/seed_build, a
	// service compile unit for the compile leg.
	Stage         string `json:"stage,omitempty"`
	NodeID        string `json:"node_id"`
	Status        string `json:"status"` // "ok" | "failed"
	DBTLogURI     string `json:"dbt_log_uri,omitempty"`
	RunResultsURI string `json:"run_results_uri,omitempty"`
	DurationMS    int64  `json:"duration_ms,omitempty"`
	// FilePath is the optional offending source file path; populated for
	// non-node legs (e.g. compile) where the failure maps to a file rather
	// than a dbt node ID.
	FilePath string `json:"file_path,omitempty"`
}

// Candidate holds the facts only a candidate release has: where its source
// came from, whether it bootstraps production, and its remediation history.
type Candidate struct {
	Repo             string
	CommitSHA        string
	Bootstrap        bool
	RemediationRound int
	// RejectionPayload is the exact release.rejected:v1 payload the
	// candidate emitted at its rejection, kept so a later remediation round
	// can replay it verbatim.
	RejectionPayload []byte
}

// Verification holds the facts only a fix-verification run has: which
// rejected release it verifies a fix for, which attempt of that release's
// remediation it belongs to, and the overlay of proposed source its compile
// and seed-build Jobs lay over the service's project.
type Verification struct {
	VerifiesReleaseID string
	Attempt           int
	SourceOverlayURI  string
}

// Run is one pass of one service's delta through the pipeline.
type Run struct {
	id                string
	kind              Kind
	status            Status
	imageTags         map[string]string
	changedService    string
	manifestKind      release.ManifestKind
	candidateTopology release.Topology
	validationNodeIDs []string
	perNodeResults    []NodeValidationResult
	failReason        string
	failDetail        string
	failingNodes      []string
	codeBundleURI     string
	createdAt         time.Time
	transitions       []Transition
	candidate         *Candidate    // non-nil iff kind == KindCandidate
	verification      *Verification // non-nil iff kind == KindVerification
}

func newRun(id string, kind Kind, service, imageTag string, manifestKind release.ManifestKind, now time.Time) *Run {
	return &Run{
		id:             id,
		kind:           kind,
		status:         StatusReceived,
		imageTags:      map[string]string{service: imageTag},
		changedService: service,
		manifestKind:   manifestKind,
		createdAt:      now,
		transitions:    []Transition{{To: StatusReceived, At: now}},
	}
}

// NewCandidate creates a candidate release for a single-service delta.
// imageTags starts with just the changed service's tag and is overwritten
// with the full assembled map at activation (SetAssembledImageTags). repo
// (GitHub owner/name) and commitSHA record the source change; kind records
// how the service's artifact is parsed. All three are immutable.
func NewCandidate(id, service, imageTag string, bootstrap bool, repo, commitSHA string, kind release.ManifestKind, now time.Time) *Run {
	r := newRun(id, KindCandidate, service, imageTag, kind, now)
	r.candidate = &Candidate{Repo: repo, CommitSHA: commitSHA, Bootstrap: bootstrap, RemediationRound: 1}
	return r
}

// NewVerification creates a fix-verification run: the same single-service
// delta shape as a candidate, but carrying the rejected release it verifies,
// the attempt it belongs to, and (for a dbt service) the overlay of proposed
// source. It has no repository provenance and never bootstraps.
func NewVerification(id, service, imageTag, verifiesReleaseID string, attempt int, sourceOverlayURI string, kind release.ManifestKind, now time.Time) *Run {
	r := newRun(id, KindVerification, service, imageTag, kind, now)
	r.verification = &Verification{VerifiesReleaseID: verifiesReleaseID, Attempt: attempt, SourceOverlayURI: sourceOverlayURI}
	return r
}

func (r *Run) ID() string                             { return r.id }
func (r *Run) Kind() Kind                             { return r.kind }
func (r *Run) Status() Status                         { return r.status }
func (r *Run) ImageTags() map[string]string           { return r.imageTags }
func (r *Run) ChangedService() string                 { return r.changedService }
func (r *Run) ManifestKind() release.ManifestKind     { return r.manifestKind }
func (r *Run) CodeBundleURI() string                  { return r.codeBundleURI }
func (r *Run) CandidateTopology() release.Topology    { return r.candidateTopology }
func (r *Run) ValidationNodeIDs() []string            { return r.validationNodeIDs }
func (r *Run) FailReason() string                     { return r.failReason }
func (r *Run) FailDetail() string                     { return r.failDetail }
func (r *Run) FailingNodes() []string                 { return r.failingNodes }
func (r *Run) PerNodeResults() []NodeValidationResult { return r.perNodeResults }
func (r *Run) CreatedAt() time.Time                   { return r.createdAt }
func (r *Run) Transitions() []Transition              { return r.transitions }

// Candidate returns the candidate-only facts, nil for a verification.
func (r *Run) Candidate() *Candidate { return r.candidate }

// Verification returns the verification-only facts, nil for a candidate.
func (r *Run) Verification() *Verification { return r.verification }

// Convenience readers of the kind-specific facts. Each returns its zero
// value on the other kind, so a caller that only formats or logs a run need
// not branch on kind; a caller that decides on kind uses Kind().
func (r *Run) IsBootstrap() bool {
	return r.candidate != nil && r.candidate.Bootstrap
}
func (r *Run) Repo() string {
	if r.candidate == nil {
		return ""
	}
	return r.candidate.Repo
}
func (r *Run) CommitSHA() string {
	if r.candidate == nil {
		return ""
	}
	return r.candidate.CommitSHA
}
func (r *Run) RemediationRound() int {
	if r.candidate == nil {
		return 0
	}
	return r.candidate.RemediationRound
}
func (r *Run) RejectionPayload() []byte {
	if r.candidate == nil {
		return nil
	}
	return r.candidate.RejectionPayload
}
func (r *Run) VerifiesReleaseID() string {
	if r.verification == nil {
		return ""
	}
	return r.verification.VerifiesReleaseID
}
func (r *Run) Attempt() int {
	if r.verification == nil {
		return 0
	}
	return r.verification.Attempt
}
func (r *Run) SourceOverlayURI() string {
	if r.verification == nil {
		return ""
	}
	return r.verification.SourceOverlayURI
}

// ActivatedAt is when the run left the queue: the time of its first
// transition into any status other than received. ok is false while the run
// has only ever been received.
func (r *Run) ActivatedAt() (time.Time, bool) {
	for _, t := range r.transitions {
		if t.To != StatusReceived {
			return t.At, true
		}
	}
	return time.Time{}, false
}

// FinishedAt is the time of the run's terminal transition; ok is false while
// the run is not terminal.
func (r *Run) FinishedAt() (time.Time, bool) {
	for i := len(r.transitions) - 1; i >= 0; i-- {
		if r.transitions[i].To.IsTerminal() {
			return r.transitions[i].At, true
		}
	}
	return time.Time{}, false
}

// SetCodeBundleURI records the S3 URI of the run's code-bundle document,
// received with the parse result.
func (r *Run) SetCodeBundleURI(uri string) { r.codeBundleURI = uri }

// SetRejectionPayload records the exact release.rejected:v1 payload a
// candidate emitted, so a later remediation round can replay it. A
// verification emits no such payload; the call is a no-op for it.
func (r *Run) SetRejectionPayload(p []byte) {
	if r.candidate != nil {
		r.candidate.RejectionPayload = p
	}
}

// SetAssembledImageTags replaces the image-tags map with the fully assembled
// set built from every service's live service_prod pointer, read at
// activation so earlier-queued promotions are seen.
func (r *Run) SetAssembledImageTags(tags map[string]string) { r.imageTags = tags }

// RecordStageResults replaces all per-node results for the given stage,
// leaving other stages intact. Idempotent across re-delivery of one leg's
// aggregate event.
func (r *Run) RecordStageResults(stage string, results []NodeValidationResult) {
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
// (stage, node_id); it backs the incremental projection of validation.result:v1
// kind:"node" messages. A re-delivery replaces in place, never appends.
func (r *Run) UpsertStageResult(stage string, result NodeValidationResult) {
	result.Stage = stage
	for i, n := range r.perNodeResults {
		if n.Stage == stage && n.NodeID == result.NodeID {
			r.perNodeResults[i] = result
			return
		}
	}
	r.perNodeResults = append(r.perNodeResults, result)
}

// RecordValidationResults stores the per-node validation outcomes.
func (r *Run) RecordValidationResults(results []NodeValidationResult) {
	r.RecordStageResults("validation", results)
}

func (r *Run) move(to Status, now time.Time) {
	r.status = to
	r.transitions = append(r.transitions, Transition{To: to, At: now})
}

// TransitionToParsing moves a received run straight into parsing: the python
// path, whose artifact CI already compiled, so there is no compile leg.
func (r *Run) TransitionToParsing(now time.Time) error {
	if r.status != StatusReceived {
		return fmt.Errorf("cannot transition to parsing from %s", r.status)
	}
	r.move(StatusParsing, now)
	return nil
}

// TransitionToCompiling moves a received run into compiling: the dbt path.
func (r *Run) TransitionToCompiling(now time.Time) error {
	if r.status != StatusReceived {
		return fmt.Errorf("cannot transition to compiling from %s", r.status)
	}
	r.move(StatusCompiling, now)
	return nil
}

// TransitionFromCompiling advances a compiling run to parsing once its
// manifest is compiled and uploaded.
func (r *Run) TransitionFromCompiling(now time.Time) error {
	if r.status != StatusCompiling {
		return fmt.Errorf("cannot transition to parsing from %s", r.status)
	}
	r.move(StatusParsing, now)
	return nil
}

// TransitionToValidating records the candidate topology and validation set
// and moves a parsing run into validating.
func (r *Run) TransitionToValidating(topology release.Topology, validationNodeIDs []string, now time.Time) error {
	if r.status != StatusParsing {
		return fmt.Errorf("cannot transition to validating from %s", r.status)
	}
	r.candidateTopology = topology
	r.validationNodeIDs = validationNodeIDs
	r.move(StatusValidating, now)
	return nil
}

// TransitionToSeedBuilding records the candidate topology and validation set
// and moves a parsing run into seed_building, for a run whose changed closure
// holds new or changed seeds that must be built before validation.
func (r *Run) TransitionToSeedBuilding(topology release.Topology, validationNodeIDs []string, now time.Time) error {
	if r.status != StatusParsing {
		return fmt.Errorf("cannot transition to seed_building from %s", r.status)
	}
	r.candidateTopology = topology
	r.validationNodeIDs = validationNodeIDs
	r.move(StatusSeedBuilding, now)
	return nil
}

// TransitionFromSeedBuilding advances a seed_building run to validating and
// narrows the persisted validation set to the nodes actually sent to the
// validation leg (the just-built seeds are excluded).
func (r *Run) TransitionFromSeedBuilding(validationNodeIDs []string, now time.Time) error {
	if r.status != StatusSeedBuilding {
		return fmt.Errorf("cannot transition to validating from %s", r.status)
	}
	r.validationNodeIDs = validationNodeIDs
	r.move(StatusValidating, now)
	return nil
}

// Promote ships a validated candidate to production. Only a candidate may
// promote: a verification carries a fix nobody has reviewed.
func (r *Run) Promote(now time.Time) error {
	if r.kind != KindCandidate {
		return fmt.Errorf("run %s: promote: %w", r.id, ErrWrongKind)
	}
	if r.status != StatusValidating {
		return fmt.Errorf("cannot transition to promoted from %s", r.status)
	}
	r.move(StatusPromoted, now)
	return nil
}

// Pass ends a verification that survived the pipeline. Only a verification
// may pass: passed is terminal and never promotes, so a candidate that
// reached it would stall there forever.
func (r *Run) Pass(now time.Time) error {
	if r.kind != KindVerification {
		return fmt.Errorf("run %s: pass: %w", r.id, ErrWrongKind)
	}
	if r.status != StatusValidating {
		return fmt.Errorf("cannot transition to passed from %s", r.status)
	}
	r.move(StatusPassed, now)
	return nil
}

// Fail ends a run of either kind that did not survive the pipeline: a
// candidate lands on rejected, a verification on failed. reason is the
// machine-readable token, detail the operator-facing explanation ("" when
// the path has none), failingNodes the nodes that did not pass.
func (r *Run) Fail(reason, detail string, failingNodes []string, now time.Time) error {
	switch r.status {
	case StatusReceived, StatusCompiling, StatusParsing, StatusSeedBuilding, StatusValidating:
	default:
		return fmt.Errorf("cannot fail from %s", r.status)
	}
	r.failReason = reason
	r.failDetail = detail
	r.failingNodes = failingNodes
	if r.kind == KindVerification {
		r.move(StatusFailed, now)
	} else {
		r.move(StatusRejected, now)
	}
	return nil
}

// StartRemediationRound records that a human asked for another remediation
// round on a rejected candidate and returns the new round number.
func (r *Run) StartRemediationRound(now time.Time) (int, error) {
	if r.kind != KindCandidate {
		return 0, fmt.Errorf("run %s: start remediation round: %w", r.id, ErrWrongKind)
	}
	if r.status != StatusRejected {
		return 0, ErrNotRejected
	}
	if r.candidate.RemediationRound >= MaxRemediationRounds {
		return 0, ErrRoundsExhausted
	}
	r.candidate.RemediationRound++
	r.transitions = append(r.transitions, Transition{To: "remediation_retry", At: now})
	return r.candidate.RemediationRound, nil
}

// RehydrateInput carries every persisted field needed to reconstruct a Run
// without re-running state-machine validation. Kind selects which of the
// candidate-only or verification-only fields are read; the others are ignored.
type RehydrateInput struct {
	ID                string
	Kind              Kind
	Status            Status
	ImageTags         map[string]string
	ChangedService    string
	ManifestKind      release.ManifestKind
	CandidateTopology release.Topology
	ValidationNodeIDs []string
	PerNodeResults    []NodeValidationResult
	FailReason        string
	FailDetail        string
	FailingNodes      []string
	CodeBundleURI     string
	CreatedAt         time.Time
	Transitions       []Transition
	// Candidate-only.
	Bootstrap        bool
	Repo             string
	CommitSHA        string
	RemediationRound int
	RejectionPayload []byte
	// Verification-only.
	VerifiesReleaseID string
	Attempt           int
	SourceOverlayURI  string
}

// Rehydrate reconstructs a Run from persistence. Only repositories call it.
func Rehydrate(in RehydrateInput) *Run {
	r := &Run{
		id:                in.ID,
		kind:              in.Kind,
		status:            in.Status,
		imageTags:         in.ImageTags,
		changedService:    in.ChangedService,
		manifestKind:      in.ManifestKind,
		candidateTopology: in.CandidateTopology,
		validationNodeIDs: in.ValidationNodeIDs,
		perNodeResults:    in.PerNodeResults,
		failReason:        in.FailReason,
		failDetail:        in.FailDetail,
		failingNodes:      in.FailingNodes,
		codeBundleURI:     in.CodeBundleURI,
		createdAt:         in.CreatedAt,
		transitions:       in.Transitions,
	}
	switch in.Kind {
	case KindVerification:
		r.verification = &Verification{VerifiesReleaseID: in.VerifiesReleaseID, Attempt: in.Attempt, SourceOverlayURI: in.SourceOverlayURI}
	default:
		round := in.RemediationRound
		if round < 1 {
			round = 1
		}
		r.candidate = &Candidate{Repo: in.Repo, CommitSHA: in.CommitSHA, Bootstrap: in.Bootstrap, RemediationRound: round, RejectionPayload: in.RejectionPayload}
	}
	return r
}
