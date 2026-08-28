package proposal

import "time"

// View is a read-only projection of a proposal row, including all PR-lifecycle
// columns. It is returned by ProposalRepository.Get and ProposalRepository.List.
type View struct {
	ID        string
	Source    string
	ReleaseID string
	// RemediationRound is the release's remediation round this attempt belongs to; the attempt cap is counted within a round.
	RemediationRound int
	NodeID           string
	ErrorSignature   string
	Attempt          int
	Status           Status
	// ShadowReleaseID is the id of the shadow release posted to verify this
	// attempt's fix, written when Status is (or was) 'verifying'. It is kept
	// when the attempt is finalized, so a resolved row still names the release
	// that judged it. Empty for an attempt that never entered verification.
	ShadowReleaseID string
	// VerifyError is the shadow release's failure reason, recorded by
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
	Edits      []FileEdit
	ReleaseID  string
	NodeID     string
	Attempt    int
	Rationale  string
	Confidence Confidence
	Model      string
	// ClaimedAt is the pr_claimed_at value BeginPR's CAS persisted for this
	// claim, read back from the row rather than trusted from the caller's
	// clock. The caller carries it forward and must present it back to
	// FailStuckOpeningPR to release this exact claim — never a fresher one
	// taken by someone else since.
	ClaimedAt time.Time
	// Branch is populated by the caller of BeginPR, not from the DB.
	Branch string
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
}
