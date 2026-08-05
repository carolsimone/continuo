package proposal

import "time"

// View is a read-only projection of a proposal row, including all PR-lifecycle
// columns. It is returned by ProposalRepository.Get and ProposalRepository.List.
type View struct {
	ID                  string
	Source              string
	ReleaseID           string
	NodeID              string
	ErrorSignature      string
	Attempt             int
	Status              Status
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
	Model               string
	CreatedAt           time.Time
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
	ReleaseID      string
	NodeID         string
	Attempt        int
	Rationale      string
	Confidence     Confidence
	Model          string
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
// retry. ClaimedAt is the wall-clock moment the claim was taken (pr_claimed_at).
// A database trigger stamps it the instant a row's pr_state transitions to
// 'opening' if it is still NULL at that point, so every claim — including one
// taken by a proposal-service binary that predates the column and never sets
// it — carries a value; ClaimedAt is nil only if that trigger did not run
// (schema corruption or a manual edit), and the sweep treats a nil the same
// way regardless of cause: an unmeasurable claim is never failed. CreatedAt is
// the proposal row's own creation time (immutable across pr_state
// transitions), used as the primary key of the sweep's pagination ordering —
// see repository.OpeningCursor.
type OpeningPR struct {
	ID        string
	Repo      string
	ReleaseID string
	NodeID    string
	Attempt   int
	ClaimedAt *time.Time
	CreatedAt time.Time
}
