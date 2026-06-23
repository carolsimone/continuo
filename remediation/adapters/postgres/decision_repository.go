package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/carolsimone/continuo/remediation/domain/repository"
)

// Queryer is the minimal sqlx surface satisfied by both *sqlx.DB and *sqlx.Tx.
type Queryer interface {
	GetContext(ctx context.Context, dest any, query string, args ...any) error
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// DecisionRepository is the Postgres-backed ClassificationDecisionRepository.
type DecisionRepository struct{ q Queryer }

var _ repository.ClassificationDecisionRepository = (*DecisionRepository)(nil)

// NewDecisionRepository binds a repository to a Queryer (pass *sqlx.Tx for the
// transactional write path).
func NewDecisionRepository(q Queryer) *DecisionRepository { return &DecisionRepository{q: q} }

// Upsert inserts the decision; ON CONFLICT on the natural key it does nothing
// and reports inserted=false, giving idempotency on redelivery. The repository
// is bound to its transaction at construction (r.q), so no tx is passed here.
func (r *DecisionRepository) Upsert(ctx context.Context, d repository.ClassificationDecision) (bool, error) {
	const stmt = `
		INSERT INTO classification_decision
			(source, release_id, node_id, category, error_signature, decision, reason, dbt_log_uri, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (source, release_id, node_id) DO NOTHING`
	res, err := r.q.ExecContext(ctx, stmt,
		d.Source, d.ReleaseID, d.NodeID, d.Category, d.ErrorSignature,
		d.Decision, d.Reason, d.DBTLogURI, d.CreatedAt)
	if err != nil {
		return false, fmt.Errorf("insert classification_decision: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("rows affected: %w", err)
	}
	return n == 1, nil
}
