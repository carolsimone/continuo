package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/domain/repository"
)

// Queryer is the minimal sqlx surface satisfied by both *sqlx.DB and *sqlx.Tx.
type Queryer interface {
	GetContext(ctx context.Context, dest any, query string, args ...any) error
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
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
			 model, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`
	_, err := r.q.ExecContext(ctx, stmt,
		p.Source, p.ReleaseID, p.NodeID, p.ErrorSignature, p.Attempt,
		p.Status, p.Confidence, p.Rationale, p.ProposedSQLURI, p.DiffURI,
		p.CandidateFixSQLURI, p.CandidateFixDiffURI, p.SourceResolved,
		p.Model, p.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert proposal: %w", err)
	}
	return nil
}
