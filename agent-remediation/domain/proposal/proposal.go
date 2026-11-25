// Package proposal holds the domain model for a remediation fix proposal: the
// suggested replacement SQL for a failed dbt node, its rationale, and the
// audit status of the attempt.
package proposal

import (
	"fmt"
	"sort"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/sanitize"
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
	// proposed a fix and posted a fix-verification run to carry it through the
	// full parse -> candidate-schema -> validation pipeline. The row carries the
	// run's id (VerificationRunID).
	// The gate poller polls the run and finalizes the attempt to 'proposed' (the
	// fix verified) or 'failed' (with the run's error recorded as evidence for
	// the next try).
	StatusVerifying Status = "verifying"
	StatusProposed  Status = "proposed"
	StatusSkipped   Status = "skipped"
	StatusFailed    Status = "failed"
	StatusEscalated Status = "escalated"
)

// NoRemediationInFlightReason is the reason the startup sweep fails an attempt
// row with it: a row in flight that belongs to no remediation, which nothing
// drives. A generating row stores it as its rationale, a verifying row as its
// verify error, and each of its node outcomes still in flight as the outcome's
// reason, so the release page shows it and offers "Try again". No gate judged
// such a row, so the earlier-tries evidence leaves it out.
const NoRemediationInFlightReason = "continuo changed its remediation engine while this attempt was in flight; use Try again"

// Phase is where a fix-verification run stands, as this service last read it
// from release-controller. queued: waiting its turn in the pipeline's FIFO.
// running: in one of the legs. passed / failed: the verdict.
type Phase string

const (
	PhaseQueued  Phase = "queued"
	PhaseRunning Phase = "running"
	PhasePassed  Phase = "passed"
	PhaseFailed  Phase = "failed"
)

// FileEdit is one proposed change to a single file: its repository-relative
// path and the S3 URIs of the proposed content and unified diff. RenamedFrom
// is the repository-relative path the file is moved from: the pull request
// deletes it and writes Path. It is empty for an edit that changes Path in
// place.
type FileEdit struct {
	Path        string
	RenamedFrom string
	ContentURI  string
	DiffURI     string
	// TargetNodeID is the node whose source this edit changes; for a
	// shared-upstream fix it is the changed ancestor, which may not itself
	// have failed.
	TargetNodeID string
	// MemberNodeIDs are the failing nodes this edit's cluster resolves; the
	// pull-request split attributes a node to the PR that carries its fixing
	// edit, so a per-project PR's resolved set is the union of its edits'
	// members. Empty on rows written before the split. Persistence is owned by
	// the adapter's fileEditRow DTO, so this aggregate carries no wire tags.
	MemberNodeIDs []string
}

// NodeOutcome is how one failing node's attempt ended: verifying, proposed,
// failed, skipped, or escalated, with the reason for that outcome.
type NodeOutcome struct {
	Status Status
	// Reason is the human-readable message for the outcome.
	Reason string
	// Code is the reason the node was handed over when its outcome is not part
	// of a verified fix. It is empty when the outcome carries no code.
	Code model.HandoverReason
}

// Cleaned is the outcome with its reason cleaned when it carries a code. A coded
// reason is a hand-over message, which reaches the release page and the
// remediation.handed_over:v1 event, and it can echo what a failure printed, so
// every credential and email address in it is replaced by its fixed marker. An
// outcome with no code is returned as it is.
func (o NodeOutcome) Cleaned() NodeOutcome {
	if o.Code != "" {
		o.Reason = sanitize.Text(o.Reason)
	}
	return o
}

// CleanedOutcomes is a copy of outcomes in which every outcome is Cleaned. The
// map it is given is not changed. A nil map gives nil.
func CleanedOutcomes(outcomes map[string]NodeOutcome) map[string]NodeOutcome {
	if outcomes == nil {
		return nil
	}
	out := make(map[string]NodeOutcome, len(outcomes))
	for node, o := range outcomes {
		out[node] = o.Cleaned()
	}
	return out
}

// CleanedReason is text an attempt's row records as why the attempt failed, such as
// the errors of a verification run or the reason a remediation stopped, with every
// credential, private key and email address replaced by its fixed marker. Such a
// text can echo what a warehouse, a build or a log printed, and the release page
// and the Proposal read show it.
func CleanedReason(reason string) string {
	return sanitize.Text(reason)
}

// Verification is one fix-verification run posted to judge the edits made to
// a single project, and the durable summary of how it went. The summary
// outlives the run itself: release-controller prunes finished runs on its
// retention window, and this is what still explains an old attempt after
// that.
type Verification struct {
	Project string
	Kind    string
	RunID   string
	// Phase is the run's last-read phase; "" on a row the gate poller has
	// not yet read since the run was recorded.
	Phase       Phase
	ActivatedAt *time.Time
	// Error is the named per-node errors the run reported, or the run's own
	// reason and detail when it named no node; empty unless the run failed.
	Error string
}

// FailedRunReason is the reason recorded for a verification that failed
// without naming a node: the first failed run and the error it reported, or,
// when it reported none, that it failed without a per-node error. A list with
// no failed run names its first run.
func FailedRunReason(runs []Verification) string {
	if len(runs) == 0 {
		return "verification failed without a per-node error"
	}
	failed := runs[0]
	for _, r := range runs {
		if r.Phase == PhaseFailed {
			failed = r
			break
		}
	}
	if failed.Error != "" {
		return fmt.Sprintf("verification run %s failed: %s", failed.RunID, failed.Error)
	}
	return fmt.Sprintf("verification run %s failed without a per-node error", failed.RunID)
}

// UnionProjects merges project-name sets into one sorted, de-duplicated
// slice with empty names dropped.
func UnionProjects(sets ...[]string) []string {
	seen := map[string]bool{}
	for _, set := range sets {
		for _, s := range set {
			if s != "" {
				seen[s] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
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
	// RemediationID is the id of the runner's remediation this attempt belongs
	// to. It is empty for an attempt row that belongs to no remediation.
	RemediationID string
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
	// cluster the runner skipped or failed leaves its members
	// skipped/failed while other members verify.
	NodeOutcomes map[string]NodeOutcome
	// Approvers is who must approve what the attempt did with each node a
	// remediation settled at its end, fixed or handed over, one entry per node
	// sorted by node id. It is empty for an attempt row that belongs to no remediation.
	Approvers []Approver
	// Verifications is one fix-verification run per edited project;
	// VerificationRunID is the view of the first.
	Verifications []Verification
	// VerificationRunID is the run id of the first verification — the
	// single-run view of Verifications. Written when Status is (or was)
	// StatusVerifying. It is kept when the attempt is finalized, so a
	// resolved row still names the run that judged it. Empty for an attempt
	// that never entered verification.
	VerificationRunID string
	// Projects is every project this attempt touched: the failing nodes'
	// projects plus the edited ones.
	Projects       []string
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
// VerificationRunID defaults to the first verification's run, and the
// single-file scalars follow edits[0]. Every writer calls it before the
// aggregate is persisted or turned into an event, so the row and the event
// cannot disagree about which node or run represents the attempt.
func (p *Proposal) NormalizeRepresentativeViews() {
	sort.Strings(p.ResolvedNodeIDs)
	if p.NodeID == "" && len(p.ResolvedNodeIDs) > 0 {
		p.NodeID = p.ResolvedNodeIDs[0]
	}
	if p.VerificationRunID == "" && len(p.Verifications) > 0 {
		p.VerificationRunID = p.Verifications[0].RunID
	}
	p.NormalizeSingleFileView()
}
