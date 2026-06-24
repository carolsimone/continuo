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

// CountAttempts returns the number of proposal attempts recorded for the
// given (source, nodeID, errorSignature) triplet.
func (r *ProposalRepository) CountAttempts(ctx context.Context, source, nodeID, errorSignature string) (int, error) {
	const query = `SELECT count(*) FROM proposal WHERE source=$1 AND node_id=$2 AND error_signature=$3`
	var count int
	if err := r.q.GetContext(ctx, &count, query, source, nodeID, errorSignature); err != nil {
		return 0, fmt.Errorf("count proposal attempts: %w", err)
	}
	return count, nil
}

// Insert persists a new proposal attempt row. The proposal's (release_id,
// node_id, attempt) triplet is unique; a duplicate attempt number returns an
// error so the caller's UnitOfWork can roll back and the trigger redelivers.
func (r *ProposalRepository) Insert(ctx context.Context, p proposal.Proposal) error {
	const stmt = `
		INSERT INTO proposal
			(source, release_id, node_id, error_signature, attempt,
			 status, confidence, rationale, proposed_sql_uri, diff_uri,
			 candidate_fix_sql_uri, candidate_fix_diff_uri, source_resolved,
			 model, created_at,
			 repo, commit_sha, file_path)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`
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

// Get returns the full View for the given proposal id.
// Returns ErrNotFound if no row exists.
func (r *ProposalRepository) Get(ctx context.Context, id string) (proposal.View, error) {
	const query = `
		SELECT id, source, release_id, node_id, error_signature, attempt,
		       status, confidence, rationale, proposed_sql_uri, diff_uri,
		       candidate_fix_sql_uri, candidate_fix_diff_uri, source_resolved,
		       repo, commit_sha, file_path, model, created_at,
		       pr_url, pr_number, pr_state, pr_opened_at, pr_opened_by
		FROM proposal
		WHERE id = $1`
	var v proposal.View
	if err := r.q.GetContext(ctx, &v, query, id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return proposal.View{}, repository.ErrNotFound
		}
		return proposal.View{}, fmt.Errorf("get proposal: %w", err)
	}
	return v, nil
}

// List returns proposals matching the filter, ordered by created_at DESC.
// Empty filter fields are treated as "no constraint". Limit=0 means no limit.
func (r *ProposalRepository) List(ctx context.Context, filter repository.ProposalFilter) ([]proposal.View, error) {
	q := `
		SELECT id, source, release_id, node_id, error_signature, attempt,
		       status, confidence, rationale, proposed_sql_uri, diff_uri,
		       candidate_fix_sql_uri, candidate_fix_diff_uri, source_resolved,
		       repo, commit_sha, file_path, model, created_at,
		       pr_url, pr_number, pr_state, pr_opened_at, pr_opened_by
		FROM proposal
		WHERE 1=1`
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

	var views []proposal.View
	if err := r.q.SelectContext(ctx, &views, q, args...); err != nil {
		return nil, fmt.Errorf("list proposals: %w", err)
	}
	if views == nil {
		views = []proposal.View{}
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
	var c proposal.PRClaim
	if err := r.q.GetContext(ctx, &c, stmt, id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return proposal.PRClaim{}, repository.ErrPRConflict
		}
		return proposal.PRClaim{}, fmt.Errorf("begin pr cas: %w", err)
	}
	c.Branch = branch
	return c, nil
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
