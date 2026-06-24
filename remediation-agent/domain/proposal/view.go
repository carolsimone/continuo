package proposal

import "time"

// View is a read-only projection of a proposal row, including all PR-lifecycle
// columns. It is returned by ProposalRepository.Get and ProposalRepository.List.
type View struct {
	ID                  string     `db:"id"`
	Source              string     `db:"source"`
	ReleaseID           string     `db:"release_id"`
	NodeID              string     `db:"node_id"`
	ErrorSignature      string     `db:"error_signature"`
	Attempt             int        `db:"attempt"`
	Status              Status     `db:"status"`
	Confidence          Confidence `db:"confidence"`
	Rationale           string     `db:"rationale"`
	ProposedSQLURI      string     `db:"proposed_sql_uri"`
	DiffURI             string     `db:"diff_uri"`
	CandidateFixSQLURI  string     `db:"candidate_fix_sql_uri"`
	CandidateFixDiffURI string     `db:"candidate_fix_diff_uri"`
	SourceResolved      bool       `db:"source_resolved"`
	Repo                string     `db:"repo"`
	CommitSHA           string     `db:"commit_sha"`
	FilePath            string     `db:"file_path"`
	Model               string     `db:"model"`
	CreatedAt           time.Time  `db:"created_at"`
	// PR-lifecycle columns.
	PrURL      string     `db:"pr_url"`
	PrNumber   int        `db:"pr_number"`
	PrState    string     `db:"pr_state"`
	PrOpenedAt *time.Time `db:"pr_opened_at"`
	PrOpenedBy string     `db:"pr_opened_by"`
}

// PRClaim carries the data needed to open a GitHub pull-request for a proposal
// that has been atomically claimed by BeginPR. The Branch field is set by the
// caller rather than read from the database.
type PRClaim struct {
	ID             string     `db:"id"`
	Repo           string     `db:"repo"`
	CommitSHA      string     `db:"commit_sha"`
	FilePath       string     `db:"file_path"`
	ProposedSQLURI string     `db:"proposed_sql_uri"`
	DiffURI        string     `db:"diff_uri"`
	ReleaseID      string     `db:"release_id"`
	NodeID         string     `db:"node_id"`
	Attempt        int        `db:"attempt"`
	Rationale      string     `db:"rationale"`
	Confidence     Confidence `db:"confidence"`
	Model          string     `db:"model"`
	// Branch is populated by the caller of BeginPR, not from the DB.
	Branch string `db:"-"`
}
