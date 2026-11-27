// Package releasecheck holds the aggregate every leg of release-controller's
// release check (compile → parse → seed build → validate) drives: a Check.
//
// A check is one of two kinds. A candidate is a release a team's CI posted; it
// promotes to production when it passes. A fix is a check agent-remediation
// posted to find out whether a proposed fix holds; it passes or fails and
// never touches production. Both kinds share the legs, the queue, the per-node
// results, and the transition history. The kind decides only which entry facts
// the check carries and which terminal statuses it can reach.
package releasecheck

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// Kind says why a check exists.
type Kind string

const (
	KindCandidate Kind = "candidate"
	KindFix       Kind = "fix"
)

// Status is where a check is in its release check. The five non-terminal statuses
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
	// Fix terminals.
	StatusPassed Status = "passed"
	StatusFailed Status = "failed"
)

// IsTerminal reports whether s ends a check of either kind.
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
	// ErrWrongKind is returned by a method that only one kind of check may take.
	ErrWrongKind       = errors.New("operation is not valid for this kind of check")
	ErrNotRejected     = errors.New("release is not rejected")
	ErrRoundsExhausted = errors.New("remediation rounds exhausted")
)

// Transition is one entry of a check's status history.
type Transition struct {
	To Status
	At time.Time
}

// NodeValidationResult is the persisted per-node outcome of a release check stage.
// Stage identifies which leg produced it (see the Stage field comment below).
// A check accumulates results across all legs so failure diagnostics are always
// available regardless of which stage ended it.
type NodeValidationResult struct {
	// Stage is "parse" | "compile" | "seed_build" | "validation". The failed
	// unit generalises across legs — a dbt node for parse/validation/seed_build,
	// a project compile unit for the compile leg.
	Stage         string
	NodeID        string
	Status        string // "ok" | "failed"
	DBTLogURI     string
	RunResultsURI string
	DurationMS    int64
	// FilePath is the optional offending source file path. The parse leg
	// carries the candidate's declared path for the failing node; the compile
	// leg carries the path the dbt log names, since its failed unit is a
	// project rather than a node.
	FilePath string
	// NodeType is the node's kind as the candidate topology declares it, so a
	// reader can tell a test's bind check from a model's build without the
	// topology.
	NodeType string
	// Detail is this leg's own diagnostic for this node. The parse leg carries
	// the parser's error text, with the line and column it rejected, since no
	// Job ran and so no log exists to point a reader at. Empty when the leg
	// reports through a log instead.
	Detail string
}

// Candidate holds the facts only a candidate release has: where its source
// came from, whether it bootstraps production, whether that bootstrap
// re-baselines fingerprints, and its remediation history.
type Candidate struct {
	Repo             string
	CommitSHA        string
	Bootstrap        bool
	Rebaseline       bool
	RemediationRound int
	// RejectionPayload is the exact release.rejected:v1 payload the
	// candidate emitted at its rejection, kept so a later remediation round
	// can replay it verbatim.
	RejectionPayload []byte
}

// Fix holds the facts only a fix's release check has: which
// rejected release it verifies a fix for, which attempt of that release's
// remediation it belongs to, and the overlay of proposed source its compile
// and seed-build Jobs lay over the project's source.
type Fix struct {
	VerifiesReleaseID string
	Attempt           int
	SourceOverlayURI  string
}

// Check is one pass of one project's delta through the release check.
type Check struct {
	id                string
	kind              Kind
	status            Status
	imageTags         map[string]string
	changedProject    string
	manifestKind      release.ManifestKind
	candidateTopology release.TopologyRef
	validationNodeIDs []string
	perNodeResults    []NodeValidationResult
	failReason        string
	failDetail        string
	failingNodes      []string
	codeBundleURI     string
	contractBundle    release.ContractBundleRef
	continuoContracts string
	parseTimings      release.ParseTimings
	createdAt         time.Time
	transitions       []Transition
	notices           []release.Notice
	candidate         *Candidate // non-nil iff kind == KindCandidate
	fix               *Fix       // non-nil iff kind == KindFix
}

func newCheck(id string, kind Kind, project, imageTag string, manifestKind release.ManifestKind, now time.Time) *Check {
	return &Check{
		id:             id,
		kind:           kind,
		status:         StatusReceived,
		imageTags:      map[string]string{project: imageTag},
		changedProject: project,
		manifestKind:   manifestKind,
		createdAt:      now,
		transitions:    []Transition{{To: StatusReceived, At: now}},
	}
}

// NewCandidate creates a candidate release for a single-project delta.
// imageTags starts with just the changed project's tag and is overwritten
// with the full assembled map at activation (SetAssembledImageTags). repo
// (GitHub owner/name) and commitSHA record the source change; kind records
// how the project's artifact is parsed. All three are immutable. rebaseline
// marks a bootstrap that re-baselines fingerprints; it is valid only together
// with bootstrap.
func NewCandidate(id, project, imageTag string, bootstrap, rebaseline bool, repo, commitSHA string, kind release.ManifestKind, now time.Time) *Check {
	r := newCheck(id, KindCandidate, project, imageTag, kind, now)
	r.candidate = &Candidate{Repo: repo, CommitSHA: commitSHA, Bootstrap: bootstrap, Rebaseline: rebaseline, RemediationRound: 1}
	return r
}

// NewFix creates a fix's release check: the same single-project
// delta shape as a candidate, but carrying the rejected release it verifies,
// the attempt it belongs to, and (for a dbt project) the overlay of proposed
// source. It has no repository provenance and never bootstraps.
func NewFix(id, project, imageTag, verifiesReleaseID string, attempt int, sourceOverlayURI string, kind release.ManifestKind, now time.Time) *Check {
	r := newCheck(id, KindFix, project, imageTag, kind, now)
	r.fix = &Fix{VerifiesReleaseID: verifiesReleaseID, Attempt: attempt, SourceOverlayURI: sourceOverlayURI}
	return r
}

func (r *Check) ID() string                                { return r.id }
func (r *Check) Kind() Kind                                { return r.kind }
func (r *Check) Status() Status                            { return r.status }
func (r *Check) ImageTags() map[string]string              { return r.imageTags }
func (r *Check) ChangedProject() string                    { return r.changedProject }
func (r *Check) ManifestKind() release.ManifestKind        { return r.manifestKind }
func (r *Check) CodeBundleURI() string                     { return r.codeBundleURI }
func (r *Check) CandidateTopologyRef() release.TopologyRef { return r.candidateTopology }
func (r *Check) ValidationNodeIDs() []string               { return r.validationNodeIDs }
func (r *Check) FailReason() string                        { return r.failReason }
func (r *Check) FailDetail() string                        { return r.failDetail }
func (r *Check) FailingNodes() []string                    { return r.failingNodes }
func (r *Check) PerNodeResults() []NodeValidationResult    { return r.perNodeResults }
func (r *Check) CreatedAt() time.Time                      { return r.createdAt }
func (r *Check) Transitions() []Transition                 { return r.transitions }

// Notices returns the check's seed reload notices, in node-id order.
func (r *Check) Notices() []release.Notice { return r.notices }

// SetNotices records a copy of the seed reload notices of the release,
// computed against the production topology it would replace.
func (r *Check) SetNotices(notices []release.Notice) { r.notices = slices.Clone(notices) }

// ApplyWarehouseFindings records what the preflight of seed nodeID found in
// the warehouse on that seed's notice. It reports false when the check holds no
// notice for the seed.
func (r *Check) ApplyWarehouseFindings(nodeID string, f seedreload.Findings) bool {
	for i := range r.notices {
		if r.notices[i].NodeID == nodeID {
			r.notices[i].ApplyFindings(f)
			return true
		}
	}
	return false
}

// Candidate returns the candidate-only facts, nil for a fix.
func (r *Check) Candidate() *Candidate { return r.candidate }

// Fix returns the fix-only facts, nil for a candidate.
func (r *Check) Fix() *Fix { return r.fix }

// Convenience readers of the kind-specific facts. Each returns its zero
// value on the other kind, so a caller that only formats or logs a check need
// not branch on kind; a caller that decides on kind uses Kind().
func (r *Check) IsBootstrap() bool {
	return r.candidate != nil && r.candidate.Bootstrap
}
func (r *Check) IsRebaseline() bool {
	return r.candidate != nil && r.candidate.Rebaseline
}
func (r *Check) Repo() string {
	if r.candidate == nil {
		return ""
	}
	return r.candidate.Repo
}
func (r *Check) CommitSHA() string {
	if r.candidate == nil {
		return ""
	}
	return r.candidate.CommitSHA
}
func (r *Check) RemediationRound() int {
	if r.candidate == nil {
		return 0
	}
	return r.candidate.RemediationRound
}
func (r *Check) RejectionPayload() []byte {
	if r.candidate == nil {
		return nil
	}
	return r.candidate.RejectionPayload
}
func (r *Check) VerifiesReleaseID() string {
	if r.fix == nil {
		return ""
	}
	return r.fix.VerifiesReleaseID
}
func (r *Check) Attempt() int {
	if r.fix == nil {
		return 0
	}
	return r.fix.Attempt
}
func (r *Check) SourceOverlayURI() string {
	if r.fix == nil {
		return ""
	}
	return r.fix.SourceOverlayURI
}

// CandidateSubmission is what a team's CI states when it submits a candidate
// release: the changed project, its image tag, how its artifact is parsed,
// whether it bootstraps production, whether that bootstrap re-baselines
// fingerprints, and the source change it was built from.
type CandidateSubmission struct {
	Project    string
	ImageTag   string
	Kind       release.ManifestKind
	Bootstrap  bool
	Rebaseline bool
	Repo       string
	CommitSHA  string
}

// MatchesSubmission reports whether s describes this candidate, so that a
// release id submitted again names the same release rather than a different
// one. The image tag compared is the one this check holds for its own changed
// project, which activation keeps when it fills in every other project's tag;
// the answer therefore does not depend on how far the check has advanced. A
// fix's release check matches no candidate submission.
func (r *Check) MatchesSubmission(s CandidateSubmission) bool {
	if r.kind != KindCandidate {
		return false
	}
	return r.changedProject == s.Project &&
		r.imageTags[r.changedProject] == s.ImageTag &&
		r.manifestKind == s.Kind &&
		r.candidate.Bootstrap == s.Bootstrap &&
		r.candidate.Rebaseline == s.Rebaseline &&
		r.candidate.Repo == s.Repo &&
		r.candidate.CommitSHA == s.CommitSHA
}

// ActivatedAt is when the check left the queue: the time of its first
// transition into any status other than received. ok is false while the check
// has only ever been received.
func (r *Check) ActivatedAt() (time.Time, bool) {
	for _, t := range r.transitions {
		if t.To != StatusReceived {
			return t.At, true
		}
	}
	return time.Time{}, false
}

// FinishedAt is the time of the check's terminal transition; ok is false while
// the check is not terminal.
func (r *Check) FinishedAt() (time.Time, bool) {
	for i := len(r.transitions) - 1; i >= 0; i-- {
		if r.transitions[i].To.IsTerminal() {
			return r.transitions[i].At, true
		}
	}
	return time.Time{}, false
}

// StageDuration is the time the check spent in stage: from its last transition
// into stage to the transition after it. It is zero while the check is still in
// stage and when it never entered it.
func (r *Check) StageDuration(stage Status) time.Duration {
	for i := len(r.transitions) - 1; i >= 0; i-- {
		if r.transitions[i].To != stage {
			continue
		}
		if i+1 == len(r.transitions) {
			return 0
		}
		return r.transitions[i+1].At.Sub(r.transitions[i].At)
	}
	return 0
}

// SetCodeBundleURI records the S3 URI of the check's code-bundle document,
// received with the parse result.
func (r *Check) SetCodeBundleURI(uri string) { r.codeBundleURI = uri }

// CandidateContractBundle returns the reference to the check's contract bundle:
// empty until a parse that ended ok names one.
func (r *Check) CandidateContractBundle() release.ContractBundleRef { return r.contractBundle }

// SetCandidateContractBundle records the reference to the contract bundle the
// parse result names.
func (r *Check) SetCandidateContractBundle(ref release.ContractBundleRef) { r.contractBundle = ref }

// ContinuoContracts returns the version of the continuo-contracts library that
// read the check's contracts: empty until the parse ends, ok or failed.
func (r *Check) ContinuoContracts() string { return r.continuoContracts }

// SetContinuoContracts records the continuo-contracts version the parse result
// names.
func (r *Check) SetContinuoContracts(version string) { r.continuoContracts = version }

// ParseTimings returns what the parse result reported about how long the parse
// took and how many contracts it wrote: the zero value until the parse ends, and
// after it when its producer reports none.
func (r *Check) ParseTimings() release.ParseTimings { return r.parseTimings }

// SetParseTimings records the timings the parse result carries, whether the
// parse ended ok or failed.
func (r *Check) SetParseTimings(t release.ParseTimings) { r.parseTimings = t }

// SetRejectionPayload records the exact release.rejected:v1 payload a
// candidate emitted, so a later remediation round can replay it. A
// fix emits no such payload; the call is a no-op for it.
func (r *Check) SetRejectionPayload(p []byte) {
	if r.candidate != nil {
		r.candidate.RejectionPayload = p
	}
}

// SetAssembledImageTags replaces the image-tags map with the fully assembled
// set built from every project's live service_prod pointer, read at
// activation so earlier-queued promotions are seen.
func (r *Check) SetAssembledImageTags(tags map[string]string) { r.imageTags = tags }

// RecordStageResults replaces all per-node results for the given stage,
// leaving other stages intact. Idempotent across re-delivery of one leg's
// aggregate event.
func (r *Check) RecordStageResults(stage string, results []NodeValidationResult) {
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
func (r *Check) UpsertStageResult(stage string, result NodeValidationResult) {
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
func (r *Check) RecordValidationResults(results []NodeValidationResult) {
	r.RecordStageResults("validation", results)
}

func (r *Check) move(to Status, now time.Time) {
	r.status = to
	r.transitions = append(r.transitions, Transition{To: to, At: now})
}

// TransitionToParsing moves a received check straight into parsing: the python
// path, whose artifact CI already compiled, so there is no compile leg.
func (r *Check) TransitionToParsing(now time.Time) error {
	if r.status != StatusReceived {
		return fmt.Errorf("cannot transition to parsing from %s", r.status)
	}
	r.move(StatusParsing, now)
	return nil
}

// TransitionToCompiling moves a received check into compiling: the dbt path.
func (r *Check) TransitionToCompiling(now time.Time) error {
	if r.status != StatusReceived {
		return fmt.Errorf("cannot transition to compiling from %s", r.status)
	}
	r.move(StatusCompiling, now)
	return nil
}

// TransitionFromCompiling advances a compiling check to parsing once its
// manifest is compiled and uploaded.
func (r *Check) TransitionFromCompiling(now time.Time) error {
	if r.status != StatusCompiling {
		return fmt.Errorf("cannot transition to parsing from %s", r.status)
	}
	r.move(StatusParsing, now)
	return nil
}

// TransitionToValidating records the reference to the candidate topology
// artifact and the validation set, and moves a parsing check into validating.
func (r *Check) TransitionToValidating(topology release.TopologyRef, validationNodeIDs []string, now time.Time) error {
	if r.status != StatusParsing {
		return fmt.Errorf("cannot transition to validating from %s", r.status)
	}
	r.candidateTopology = topology
	r.validationNodeIDs = validationNodeIDs
	r.move(StatusValidating, now)
	return nil
}

// TransitionToSeedBuilding records the reference to the candidate topology
// artifact and the validation set, and moves a parsing check into
// seed_building, for a check whose changed closure holds new or changed seeds
// that must be built before validation.
func (r *Check) TransitionToSeedBuilding(topology release.TopologyRef, validationNodeIDs []string, now time.Time) error {
	if r.status != StatusParsing {
		return fmt.Errorf("cannot transition to seed_building from %s", r.status)
	}
	r.candidateTopology = topology
	r.validationNodeIDs = validationNodeIDs
	r.move(StatusSeedBuilding, now)
	return nil
}

// TransitionFromSeedBuilding advances a seed_building check to validating and
// narrows the persisted validation set to the nodes actually sent to the
// validation leg (the just-built seeds are excluded).
func (r *Check) TransitionFromSeedBuilding(validationNodeIDs []string, now time.Time) error {
	if r.status != StatusSeedBuilding {
		return fmt.Errorf("cannot transition to validating from %s", r.status)
	}
	r.validationNodeIDs = validationNodeIDs
	r.move(StatusValidating, now)
	return nil
}

// Promote ships a validated candidate to production. Only a candidate may
// promote: a fix's release check carries a fix nobody has reviewed.
func (r *Check) Promote(now time.Time) error {
	if r.kind != KindCandidate {
		return fmt.Errorf("check %s: promote: %w", r.id, ErrWrongKind)
	}
	if r.status != StatusValidating {
		return fmt.Errorf("cannot transition to promoted from %s", r.status)
	}
	r.move(StatusPromoted, now)
	return nil
}

// Pass ends a fix's release check that every leg passed. Only a fix's
// release check may pass: passed is terminal and never promotes, so a
// candidate that reached it would stall there forever.
func (r *Check) Pass(now time.Time) error {
	if r.kind != KindFix {
		return fmt.Errorf("check %s: pass: %w", r.id, ErrWrongKind)
	}
	if r.status != StatusValidating {
		return fmt.Errorf("cannot transition to passed from %s", r.status)
	}
	r.move(StatusPassed, now)
	return nil
}

// Fail ends a check of either kind that did not pass every leg: a
// candidate lands on rejected, a fix on failed. reason is the
// machine-readable token, detail the operator-facing explanation ("" when
// the path has none), failingNodes the nodes that did not pass.
func (r *Check) Fail(reason, detail string, failingNodes []string, now time.Time) error {
	switch r.status {
	case StatusReceived, StatusCompiling, StatusParsing, StatusSeedBuilding, StatusValidating:
	default:
		return fmt.Errorf("cannot fail from %s", r.status)
	}
	r.failReason = reason
	r.failDetail = detail
	r.failingNodes = failingNodes
	if r.kind == KindFix {
		r.move(StatusFailed, now)
	} else {
		r.move(StatusRejected, now)
	}
	return nil
}

// StartRemediationRound records that a human asked for another remediation
// round on a rejected candidate and returns the new round number.
func (r *Check) StartRemediationRound(now time.Time) (int, error) {
	if r.kind != KindCandidate {
		return 0, fmt.Errorf("check %s: start remediation round: %w", r.id, ErrWrongKind)
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

// RehydrateInput carries every persisted field needed to reconstruct a Check
// without re-running state-machine validation. Kind selects which of the
// candidate-only or fix-only fields are read; the others are ignored.
type RehydrateInput struct {
	ID                string
	Kind              Kind
	Status            Status
	ImageTags         map[string]string
	ChangedProject    string
	ManifestKind      release.ManifestKind
	CandidateTopology release.TopologyRef
	ValidationNodeIDs []string
	PerNodeResults    []NodeValidationResult
	FailReason        string
	FailDetail        string
	FailingNodes      []string
	CodeBundleURI     string
	// CandidateContractBundle, ContinuoContracts and ParseTimings are the contract
	// bundle reference, the continuo-contracts version and the timings the parse
	// recorded.
	CandidateContractBundle release.ContractBundleRef
	ContinuoContracts       string
	ParseTimings            release.ParseTimings
	CreatedAt               time.Time
	Transitions             []Transition
	Notices                 []release.Notice
	// Candidate-only.
	Bootstrap        bool
	Rebaseline       bool
	Repo             string
	CommitSHA        string
	RemediationRound int
	RejectionPayload []byte
	// Fix-only.
	VerifiesReleaseID string
	Attempt           int
	SourceOverlayURI  string
}

// Rehydrate reconstructs a Check from persistence. Only repositories call it.
func Rehydrate(in RehydrateInput) *Check {
	r := &Check{
		id:                in.ID,
		kind:              in.Kind,
		status:            in.Status,
		imageTags:         in.ImageTags,
		changedProject:    in.ChangedProject,
		manifestKind:      in.ManifestKind,
		candidateTopology: in.CandidateTopology,
		validationNodeIDs: in.ValidationNodeIDs,
		perNodeResults:    in.PerNodeResults,
		failReason:        in.FailReason,
		failDetail:        in.FailDetail,
		failingNodes:      in.FailingNodes,
		codeBundleURI:     in.CodeBundleURI,
		contractBundle:    in.CandidateContractBundle,
		continuoContracts: in.ContinuoContracts,
		parseTimings:      in.ParseTimings,
		createdAt:         in.CreatedAt,
		transitions:       in.Transitions,
		notices:           in.Notices,
	}
	switch in.Kind {
	case KindFix:
		r.fix = &Fix{VerifiesReleaseID: in.VerifiesReleaseID, Attempt: in.Attempt, SourceOverlayURI: in.SourceOverlayURI}
	default:
		round := in.RemediationRound
		if round < 1 {
			round = 1
		}
		r.candidate = &Candidate{Repo: in.Repo, CommitSHA: in.CommitSHA, Bootstrap: in.Bootstrap, Rebaseline: in.Rebaseline, RemediationRound: round, RejectionPayload: in.RejectionPayload}
	}
	return r
}
