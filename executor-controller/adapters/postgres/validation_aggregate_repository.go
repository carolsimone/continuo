package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
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

// LockRelease takes a transaction-scoped advisory lock keyed on (releaseID, mode)
// via pg_advisory_xact_lock. hashtext maps the composite string to the int8 the
// advisory-lock API needs. The lock auto-releases at commit/rollback, so it only
// serializes callers that hold it inside an open transaction — concurrent gate
// runs for the same (release, leg) block here until the holder commits, then
// proceed and see the now-consistent pending count. Keying on mode keeps the
// validation and seed-build legs of one release from blocking each other.
func (r *validationAggregateRepository) LockRelease(ctx context.Context, releaseID string, mode model.Mode) error {
	const q = `SELECT pg_advisory_xact_lock(hashtext($1))`
	if _, err := r.exec.ExecContext(ctx, q, string(mode)+":"+releaseID); err != nil {
		return fmt.Errorf("lock release %s (%s): %w", releaseID, mode, err)
	}
	return nil
}

// ClaimEmission inserts the per-(release, mode) sentinel row. The
// INSERT ... ON CONFLICT DO NOTHING makes the claim atomic: exactly one
// concurrent caller affects a row (true) and may emit the leg's completion
// event; losers see zero rows affected (false). The (release_id, mode) key lets
// a single release emit both seed.build.completed:v1 and validation.completed:v1.
func (r *validationAggregateRepository) ClaimEmission(ctx context.Context, releaseID string, mode model.Mode, now time.Time) (bool, error) {
	const q = `
		INSERT INTO validation_aggregates (release_id, mode, aggregate_emitted_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (release_id, mode) DO NOTHING`
	res, err := r.exec.ExecContext(ctx, q, releaseID, string(mode), now)
	if err != nil {
		return false, fmt.Errorf("claim aggregate emission %s (%s): %w", releaseID, mode, err)
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}
