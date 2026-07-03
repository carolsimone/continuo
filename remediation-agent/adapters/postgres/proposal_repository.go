package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/domain/repository"
)

// Queryer is the minimal sqlx surface satisfied by both *sqlx.DB and *sqlx.Tx.
// SelectContext is required for multi-row reads (List).
type Queryer interface {
	GetContext(ctx context.Context, dest any, query string, args ...any) error
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	SelectContext(ctx context.Context, dest any, query string, args ...any) error
}

// ProposalRepository is the Postgres-backed ProposalRepository port.
type ProposalRepository struct{ q Queryer }

var _ repository.ProposalRepository = (*ProposalRepository)(nil)

// NewProposalRepository binds a repository to a Queryer (pass *sqlx.Tx for the
// transactional write path).
func NewProposalRepository(q Queryer) *ProposalRepository {
	return &ProposalRepository{q: q}
}

// CountAttempts returns the number of TERMINAL proposal attempts recorded for
// the given (source, nodeID, errorSignature) triplet. In-flight 'generating'
// rows are excluded so the in-progress attempt does not inflate the attempt cap
// or shift the attempt number on a redelivery.
func (r *ProposalRepository) CountAttempts(ctx context.Context, source, nodeID, errorSignature string) (int, error) {
	const query = `SELECT count(*) FROM proposal
		WHERE source=$1 AND node_id=$2 AND error_signature=$3 AND status <> 'generating'`
	var count int
	if err := r.q.GetContext(ctx, &count, query, source, nodeID, errorSignature); err != nil {
		return 0, fmt.Errorf("count proposal attempts: %w", err)
	}
	return count, nil
}

// InsertGenerating persists an in-flight 'generating' row for the attempt right
// before the model is called. It is idempotent: ON CONFLICT on the natural key
// (release_id, source, node_id, attempt) DO NOTHING, so a redelivery that
// re-runs the same attempt leaves the single generating row untouched. Only the
// identity + status columns are written; the remaining columns take their
// defaults and are populated when Insert finalizes the row.
func (r *ProposalRepository) InsertGenerating(ctx context.Context, p proposal.Proposal) error {
	const stmt = `
		INSERT INTO proposal
			(source, release_id, node_id, error_signature, attempt, status, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (release_id, source, node_id, attempt) DO NOTHING`
	_, err := r.q.ExecContext(ctx, stmt,
		p.Source, p.ReleaseID, p.NodeID, p.ErrorSignature, p.Attempt,
		proposal.StatusGenerating, p.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert generating proposal: %w", err)
	}
	return nil
}

// Upsert records the terminal outcome of a proposal attempt on the natural key
// (release_id, source, node_id, attempt): when an in-flight generating row
// exists (the common healable path) it is finalized in place via
// ON CONFLICT … DO UPDATE; otherwise the row is plain-inserted (instant paths —
// e.g. attempt-cap escalation — that never marked generating).
func (r *ProposalRepository) Upsert(ctx context.Context, p proposal.Proposal) error {
	const stmt = `
		INSERT INTO proposal
			(source, release_id, node_id, error_signature, attempt,
			 status, confidence, rationale, proposed_sql_uri, diff_uri,
			 candidate_fix_sql_uri, candidate_fix_diff_uri, source_resolved,
			 model, created_at,
			 repo, commit_sha, file_path)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
		ON CONFLICT (release_id, source, node_id, attempt) DO UPDATE SET
			status                 = EXCLUDED.status,
			confidence             = EXCLUDED.confidence,
			rationale              = EXCLUDED.rationale,
			proposed_sql_uri       = EXCLUDED.proposed_sql_uri,
			diff_uri               = EXCLUDED.diff_uri,
			candidate_fix_sql_uri  = EXCLUDED.candidate_fix_sql_uri,
			candidate_fix_diff_uri = EXCLUDED.candidate_fix_diff_uri,
			source_resolved        = EXCLUDED.source_resolved,
			model                  = EXCLUDED.model,
			created_at             = EXCLUDED.created_at,
			repo                   = EXCLUDED.repo,
			commit_sha             = EXCLUDED.commit_sha,
			file_path              = EXCLUDED.file_path`
	_, err := r.q.ExecContext(ctx, stmt,
		p.Source, p.ReleaseID, p.NodeID, p.ErrorSignature, p.Attempt,
		p.Status, p.Confidence, p.Rationale, p.ProposedSQLURI, p.DiffURI,
		p.CandidateFixSQLURI, p.CandidateFixDiffURI, p.SourceResolved,
		p.Model, p.CreatedAt,
		p.Repo, p.CommitSHA, p.FilePath,
	)
	if err != nil {
		return fmt.Errorf("insert proposal: %w", err)
	}
	return nil
}

// proposalRow is the persistence DTO for a full proposal projection. The db
// tags (a persistence concern) live here in the adapter so the domain
// proposal.View stays free of storage annotations.
type proposalRow struct {
	ID                  string     `db:"id"`
	Source              string     `db:"source"`
	ReleaseID           string     `db:"release_id"`
	NodeID              string     `db:"node_id"`
	ErrorSignature      string     `db:"error_signature"`
	Attempt             int        `db:"attempt"`
	Status              string     `db:"status"`
	Confidence          string     `db:"confidence"`
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
	PrURL               string     `db:"pr_url"`
	PrNumber            int        `db:"pr_number"`
	PrState             string     `db:"pr_state"`
	PrOpenedAt          *time.Time `db:"pr_opened_at"`
	PrOpenedBy          string     `db:"pr_opened_by"`
	PrClosedAt          *time.Time `db:"pr_closed_at"`
}

func (row proposalRow) toView() proposal.View {
	return proposal.View{
		ID:                  row.ID,
		Source:              row.Source,
		ReleaseID:           row.ReleaseID,
		NodeID:              row.NodeID,
		ErrorSignature:      row.ErrorSignature,
		Attempt:             row.Attempt,
		Status:              proposal.Status(row.Status),
		Confidence:          proposal.Confidence(row.Confidence),
		Rationale:           row.Rationale,
		ProposedSQLURI:      row.ProposedSQLURI,
		DiffURI:             row.DiffURI,
		CandidateFixSQLURI:  row.CandidateFixSQLURI,
		CandidateFixDiffURI: row.CandidateFixDiffURI,
		SourceResolved:      row.SourceResolved,
		Repo:                row.Repo,
		CommitSHA:           row.CommitSHA,
		FilePath:            row.FilePath,
		Model:               row.Model,
		CreatedAt:           row.CreatedAt,
		PrURL:               row.PrURL,
		PrNumber:            row.PrNumber,
		PrState:             row.PrState,
		PrOpenedAt:          row.PrOpenedAt,
		PrOpenedBy:          row.PrOpenedBy,
		PrClosedAt:          row.PrClosedAt,
	}
}

const proposalColumns = `id, source, release_id, node_id, error_signature, attempt,
		       status, confidence, rationale, proposed_sql_uri, diff_uri,
		       candidate_fix_sql_uri, candidate_fix_diff_uri, source_resolved,
		       repo, commit_sha, file_path, model, created_at,
		       pr_url, pr_number, pr_state, pr_opened_at, pr_opened_by, pr_closed_at`

// claimRow is the persistence DTO for the BeginPR RETURNING projection.
type claimRow struct {
	ID             string `db:"id"`
	Repo           string `db:"repo"`
	CommitSHA      string `db:"commit_sha"`
	FilePath       string `db:"file_path"`
	ProposedSQLURI string `db:"proposed_sql_uri"`
	DiffURI        string `db:"diff_uri"`
	ReleaseID      string `db:"release_id"`
	NodeID         string `db:"node_id"`
	Attempt        int    `db:"attempt"`
	Rationale      string `db:"rationale"`
	Confidence     string `db:"confidence"`
	Model          string `db:"model"`
}

func (row claimRow) toClaim(branch string) proposal.PRClaim {
	return proposal.PRClaim{
		ID:             row.ID,
		Repo:           row.Repo,
		CommitSHA:      row.CommitSHA,
		FilePath:       row.FilePath,
		ProposedSQLURI: row.ProposedSQLURI,
		DiffURI:        row.DiffURI,
		ReleaseID:      row.ReleaseID,
		NodeID:         row.NodeID,
		Attempt:        row.Attempt,
		Rationale:      row.Rationale,
		Confidence:     proposal.Confidence(row.Confidence),
		Model:          row.Model,
		Branch:         branch,
	}
}

// Get returns the full View for the given proposal id.
// Returns ErrNotFound if no row exists.
func (r *ProposalRepository) Get(ctx context.Context, id string) (proposal.View, error) {
	query := `SELECT ` + proposalColumns + ` FROM proposal WHERE id = $1`
	var row proposalRow
	if err := r.q.GetContext(ctx, &row, query, id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return proposal.View{}, repository.ErrNotFound
		}
		return proposal.View{}, fmt.Errorf("get proposal: %w", err)
	}
	return row.toView(), nil
}

// List returns proposals matching the filter, ordered by created_at DESC.
// Empty filter fields are treated as "no constraint". Limit=0 means no limit.
func (r *ProposalRepository) List(ctx context.Context, filter repository.ProposalFilter) ([]proposal.View, error) {
	q := `SELECT ` + proposalColumns + ` FROM proposal WHERE 1=1`
	args := make([]any, 0, 3)

	if filter.Status != "" {
		args = append(args, filter.Status)
		q += fmt.Sprintf(" AND status = $%d", len(args))
	}
	if filter.PRState != "" {
		args = append(args, filter.PRState)
		q += fmt.Sprintf(" AND pr_state = $%d", len(args))
	}
	q += " ORDER BY created_at DESC"
	if filter.Limit > 0 {
		args = append(args, filter.Limit)
		q += fmt.Sprintf(" LIMIT $%d", len(args))
	}

	var rows []proposalRow
	if err := r.q.SelectContext(ctx, &rows, q, args...); err != nil {
		return nil, fmt.Errorf("list proposals: %w", err)
	}
	views := make([]proposal.View, 0, len(rows))
	for _, row := range rows {
		views = append(views, row.toView())
	}
	return views, nil
}

// BeginPR atomically claims a proposal for PR creation: pr_state '' or 'failed'
// -> 'opening'. The UPDATE…RETURNING is the single-winner guard; concurrent
// callers see 0 rows and receive ErrPRConflict. Returns ErrNotSourceResolved
// when source_resolved=false, ErrNotFound when the id is unknown.
func (r *ProposalRepository) BeginPR(ctx context.Context, id, branch string) (proposal.PRClaim, error) {
	// Distinguish "not source-resolved" from "already claimed" for a precise error.
	var sr bool
	if err := r.q.GetContext(ctx, &sr, `SELECT source_resolved FROM proposal WHERE id=$1`, id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return proposal.PRClaim{}, repository.ErrNotFound
		}
		return proposal.PRClaim{}, fmt.Errorf("begin pr lookup: %w", err)
	}
	if !sr {
		return proposal.PRClaim{}, repository.ErrNotSourceResolved
	}

	const stmt = `
		UPDATE proposal SET pr_state = 'opening'
		WHERE id = $1
		  AND source_resolved
		  AND pr_state IN ('', 'failed')
		RETURNING id, repo, commit_sha, file_path, proposed_sql_uri, diff_uri,
		          release_id, node_id, attempt, rationale, confidence, model`
	var row claimRow
	if err := r.q.GetContext(ctx, &row, stmt, id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return proposal.PRClaim{}, repository.ErrPRConflict
		}
		return proposal.PRClaim{}, fmt.Errorf("begin pr cas: %w", err)
	}
	return row.toClaim(branch), nil
}

// RecordPR records the opened PR and flips pr_state to 'open'.
// Returns ErrNotFound when the id does not exist.
func (r *ProposalRepository) RecordPR(ctx context.Context, id, prURL string, prNumber int, openedBy string, openedAt time.Time) error {
	res, err := r.q.ExecContext(ctx,
		`UPDATE proposal
		 SET pr_state='open', pr_url=$2, pr_number=$3, pr_opened_by=$4, pr_opened_at=$5
		 WHERE id=$1`,
		id, prURL, prNumber, openedBy, openedAt)
	if err != nil {
		return fmt.Errorf("record pr: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return repository.ErrNotFound
	}
	return nil
}

// FailPR resets a stuck 'opening' claim back to 'failed' so the action can be
// retried. It is a no-op when pr_state is not 'opening'.
func (r *ProposalRepository) FailPR(ctx context.Context, id string) error {
	_, err := r.q.ExecContext(ctx,
		`UPDATE proposal SET pr_state='failed' WHERE id=$1 AND pr_state='opening'`, id)
	if err != nil {
		return fmt.Errorf("fail pr: %w", err)
	}
	return nil
}

// openPRRow is the persistence DTO for the ListOpenPullRequests projection.
type openPRRow struct {
	ID        string `db:"id"`
	Repo      string `db:"repo"`
	PRNumber  int    `db:"pr_number"`
	ReleaseID string `db:"release_id"`
	NodeID    string `db:"node_id"`
	Attempt   int    `db:"attempt"`
}

// ListOpenPullRequests returns proposals with pr_state='open', oldest-opened
// first, so the reconciler checks the longest-waiting PRs before newer ones.
func (r *ProposalRepository) ListOpenPullRequests(ctx context.Context, limit int) ([]proposal.OpenPR, error) {
	q := `SELECT id, repo, pr_number, release_id, node_id, attempt
	      FROM proposal WHERE pr_state = 'open' ORDER BY pr_opened_at ASC`
	args := []any{}
	if limit > 0 {
		args = append(args, limit)
		q += " LIMIT $1"
	}
	var rows []openPRRow
	if err := r.q.SelectContext(ctx, &rows, q, args...); err != nil {
		return nil, fmt.Errorf("list open pull requests: %w", err)
	}
	out := make([]proposal.OpenPR, 0, len(rows))
	for _, row := range rows {
		out = append(out, proposal.OpenPR{
			ID:        row.ID,
			Repo:      row.Repo,
			PRNumber:  row.PRNumber,
			ReleaseID: row.ReleaseID,
			NodeID:    row.NodeID,
			Attempt:   row.Attempt,
		})
	}
	return out, nil
}

// RecordPROutcome atomically transitions pr_state 'open' -> outcome. The WHERE
// pr_state='open' guard makes concurrent or repeated calls single-winner: only
// the first caller sees rows-affected=1; every later call is a no-op false.
func (r *ProposalRepository) RecordPROutcome(ctx context.Context, id string, outcome proposal.PROutcome, closedAt time.Time) (bool, error) {
	res, err := r.q.ExecContext(ctx,
		`UPDATE proposal SET pr_state=$2, pr_closed_at=$3 WHERE id=$1 AND pr_state='open'`,
		id, string(outcome), closedAt)
	if err != nil {
		return false, fmt.Errorf("record pr outcome: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("record pr outcome: rows affected: %w", err)
	}
	return n > 0, nil
}
