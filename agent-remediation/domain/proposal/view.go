package proposal

import "time"

// PullRequest is a pull request opened from a proposal, carrying all fields
// mirrored from GitHub. One row per (proposal, service) combination when the
// proposal was split by service.
type PullRequest struct {
	// Service is the owning-service group this PR covers; "" for legacy
	// whole-proposal PRs.
	Service string
	// Repo is the version-control repository (owner/name) into which the PR was
	// opened.
	Repo string
	// Branch is the head branch name of this PR.
	Branch string
	// PrURL is the full GitHub URL of the pull request.
	PrURL string
	// PrNumber is the GitHub-assigned pull request number.
	PrNumber int
	// PrState is the terminal state from GitHub: "merged" or "rejected".
	PrState string
	// PrOpenedAt is when the PR was opened on GitHub.
	PrOpenedAt *time.Time
	// PrClosedAt is when the PR was closed on GitHub; nil while it remains
	// open or terminal state is not yet known.
	PrClosedAt *time.Time
	// PrOpenedBy is the GitHub username that opened the PR.
	PrOpenedBy string
	// PrClaimedAt is when this pull request was claimed for opening, mirroring
	// proposal.pr_claimed_at; used to detect stale claims.
	PrClaimedAt *time.Time
}

// View is a read-only projection of a proposal row, including all PR-lifecycle
// columns. It is returned by ProposalRepository.Get and ProposalRepository.List.
type View struct {
	ID        string
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
	// Verifications is one fix-verification run per edited service;
	// VerificationRunID is the view of the first.
	Verifications []Verification
	// VerificationRunID is the run id of the first verification — the
	// single-run view of Verifications. Written when Status is (or was)
	// 'verifying'. It is kept when the attempt is finalized, so a resolved row
	// still names the run that judged it. Empty for an attempt that never
	// entered verification.
	VerificationRunID string
	// Services is every service this attempt touched: the failing nodes'
	// services plus the edited ones.
	Services []string
	// VerifyError is the verification run's failure reason, recorded by
	// MarkVerifyFailed; empty unless verification failed.
	VerifyError string
	// TriggerPayload is the raw remediation.requested:v1 payload this attempt
	// was triggered by, written when Status is (or was) 'verifying' so a
	// reconciler can rebuild the trigger for a retry. It is kept when the
	// attempt is finalized — the reconciler reads it from a row it has already
	// moved to 'failed'. Empty for an attempt that never entered verification.
	TriggerPayload      []byte
	Confidence          Confidence
	Rationale           string
	ProposedSQLURI      string
	DiffURI             string
	CandidateFixSQLURI  string
	CandidateFixDiffURI string
	SourceResolved      bool
	Repo                string
	CommitSHA           string
	FilePath            string
	// Edits is the list of proposed file changes for this attempt: either the
	// value written for this row, or — when the row predates this field and
	// carries an empty list — one edit synthesized from FilePath,
	// ProposedSQLURI, and DiffURI.
	Edits     []FileEdit
	Model     string
	CreatedAt time.Time
	// PR-lifecycle columns.
	PrURL      string
	PrNumber   int
	PrState    string
	PrOpenedAt *time.Time
	PrOpenedBy string
	// PrClosedAt is when GitHub closed the PR; nil while pr_state is not terminal.
	PrClosedAt *time.Time
	// PullRequests is one row per (proposal, service) PR; empty for rows that
	// never entered the PR lifecycle.
	PullRequests []PullRequest
}

// FixedNodeIDs is the failing nodes this attempt actually repaired: the
// resolved nodes whose own outcome is 'proposed', in the resolved set's order.
// It is what a pull request may be said to fix, and therefore what the
// PR-lifecycle events name — an attempt that skipped or failed a node carries
// no fix for it, so attaching the PR to that node's rejection would report a
// remediation nobody proposed.
//
// A row carrying no per-node outcomes at all — one written before the column
// existed — has nothing to filter on and reports its whole resolved set, which
// is how such a row has always been read.
func (v View) FixedNodeIDs() []string {
	return FixedNodeIDs(v.ResolvedNodeIDs, v.NodeOutcomes)
}

// FixedNodeIDs filters a resolved node set to the nodes the attempt repaired,
// keeping the resolved set's order. It is the one implementation behind every
// reader of "which nodes does this pull request fix" — the View helper above
// and the PR claim the repository hands to the caller that opens the PR.
func FixedNodeIDs(resolved []string, outcomes map[string]NodeOutcome) []string {
	if len(outcomes) == 0 {
		return resolved
	}
	fixed := make([]string, 0, len(resolved))
	for _, id := range resolved {
		if outcomes[id].Status == StatusProposed {
			fixed = append(fixed, id)
		}
	}
	return fixed
}

// PRClaim carries the data needed to open a GitHub pull-request for a proposal
// that has been atomically claimed by BeginPR. The Branch field is set by the
// caller rather than read from the database.
type PRClaim struct {
	ID             string
	Repo           string
	CommitSHA      string
	FilePath       string
	ProposedSQLURI string
	DiffURI        string
	// Edits is the list of proposed file changes for this attempt, with the
	// same legacy-synthesis fallback as View.Edits.
	Edits     []FileEdit
	ReleaseID string
	NodeID    string
	// ResolvedNodeIDs is the failing nodes this attempt addresses, sorted;
	// NodeID is their representative.
	ResolvedNodeIDs []string
	Attempt         int
	Rationale       string
	Confidence      Confidence
	Model           string
	// ClaimedAt is the pr_claimed_at value BeginPR's CAS persisted for this
	// claim, read back from the row rather than trusted from the caller's
	// clock. The caller carries it forward and must present it back to
	// FailStuckOpeningPR to release this exact claim — never a fresher one
	// taken by someone else since.
	ClaimedAt time.Time
	// Branch is populated by the caller of BeginPR, not from the DB.
	Branch string
	// Service is the owning-service group this claim covers; '' = legacy
	// whole-proposal claim.
	Service string
}

// PROutcome is a terminal pull-request outcome mirrored from GitHub.
type PROutcome string

const (
	// PROutcomeMerged marks a PR that GitHub reports closed with merged=true.
	PROutcomeMerged PROutcome = "merged"
	// PROutcomeRejected marks a PR closed on GitHub without being merged.
	PROutcomeRejected PROutcome = "rejected"
)

// OpenPR identifies a proposal whose opened pull request awaits a terminal
// outcome; it carries exactly the fields the PR reconciler needs.
type OpenPR struct {
	ID        string
	Repo      string
	PRNumber  int
	ReleaseID string
	NodeID    string
	Attempt   int
	Service   string
}

// OpeningPR identifies a proposal claimed for PR creation (pr_state='opening')
// whose fate the reconciler's opening sweep resolves: either GitHub already
// has the pull request (the claim's recording step failed after creation
// succeeded), or the claim is stale and safe to release back to 'failed' for
// retry. ClaimedAt is the wall-clock moment the claim was taken
// (pr_claimed_at), and is essentially always non-nil — every claim carries a
// value regardless of which writer took it. A nil ClaimedAt means the claim
// time is unknown, and the sweep treats it as such: an unmeasurable claim is
// never judged stale, so it is never swept, only logged and left for the next
// pass. CreatedAt is the proposal row's own creation time (immutable across
// pr_state transitions), used as the primary key of the sweep's pagination
// ordering — see repository.OpeningCursor.
type OpeningPR struct {
	ID        string
	Repo      string
	ReleaseID string
	NodeID    string
	Attempt   int
	ClaimedAt *time.Time
	CreatedAt time.Time
	Service   string
}
