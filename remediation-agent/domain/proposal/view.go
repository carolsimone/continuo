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
