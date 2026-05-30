package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	"github.com/carolsimone/continuo/pkg/outbox"
)

// validationAggregateRepository is the Postgres adapter implementing
// repository.ValidationAggregateRepository over the validation_aggregates
// sentinel table.
type validationAggregateRepository struct {
	exec outbox.Executor
}

var _ repository.ValidationAggregateRepository = (*validationAggregateRepository)(nil)

// NewValidationAggregateRepository constructs a
// repository.ValidationAggregateRepository over validation_aggregates. Pass
// *sqlx.DB for autocommit or *sqlx.Tx for transactional use; outbox.Executor
// abstracts both.
func NewValidationAggregateRepository(exec outbox.Executor) repository.ValidationAggregateRepository {
	return &validationAggregateRepository{exec: exec}
}

// LockRelease takes a transaction-scoped advisory lock keyed on releaseID via
// pg_advisory_xact_lock. hashtext maps the release_id string to the int8 the
// advisory-lock API needs. The lock auto-releases at commit/rollback, so it
// only serializes callers that hold it inside an open transaction — concurrent
// gate runs for the same release block here until the holder commits, then
// proceed and see the now-consistent pending count.
func (r *validationAggregateRepository) LockRelease(ctx context.Context, releaseID string) error {
	const q = `SELECT pg_advisory_xact_lock(hashtext($1))`
	if _, err := r.exec.ExecContext(ctx, q, releaseID); err != nil {
		return fmt.Errorf("lock release %s: %w", releaseID, err)
	}
	return nil
}

// ClaimEmission inserts the per-release sentinel row. The INSERT ... ON CONFLICT
// DO NOTHING makes the claim atomic: exactly one concurrent caller affects a
// row (true) and may emit validation.completed:v1; losers see zero rows
// affected (false).
func (r *validationAggregateRepository) ClaimEmission(ctx context.Context, releaseID string, now time.Time) (bool, error) {
	const q = `
		INSERT INTO validation_aggregates (release_id, aggregate_emitted_at)
		VALUES ($1, $2)
		ON CONFLICT (release_id) DO NOTHING`
	res, err := r.exec.ExecContext(ctx, q, releaseID, now)
	if err != nil {
		return false, fmt.Errorf("claim validation aggregate emission: %w", err)
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}
