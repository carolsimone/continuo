package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/domain/repository"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

// inFlight binds model.InFlightStatuses as a text[] query parameter, so no
// query repeats the set as literals.
func inFlight() any { return pq.Array(model.StatusStrings(model.InFlightStatuses())) }

// admissionRepository is the Postgres adapter implementing
// repository.AdmissionRepository over admission_capacity and deployments.
type admissionRepository struct {
	exec outbox.Executor
	rows *deploymentsRepository // reconstitutes aggregates from rows
}

var _ repository.AdmissionRepository = (*admissionRepository)(nil)

// NewAdmissionRepository constructs a repository.AdmissionRepository. Pass the
// *sqlx.Tx of the claim or launch transaction, or *sqlx.DB for the
// reconciler.
func NewAdmissionRepository(exec outbox.Executor, logger *slog.Logger) repository.AdmissionRepository {
	return &admissionRepository{exec: exec, rows: &deploymentsRepository{exec: exec, logger: logger}}
}

func (r *admissionRepository) LockScope(ctx context.Context, scope string) error {
	var got string
	err := r.exec.QueryRowContext(ctx, `SELECT scope FROM admission_capacity WHERE scope = $1 FOR UPDATE`, scope).Scan(&got)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("admission scope %q has no capacity record: run the execution migrations", scope)
	}
	if err != nil {
		return fmt.Errorf("lock admission scope %q: %w", scope, err)
	}
	return nil
}

func (r *admissionRepository) CountInFlight(ctx context.Context) (int, error) {
	var n int
	if err := r.exec.QueryRowContext(ctx,
		`SELECT count(*) FROM deployments WHERE status = ANY($1)`, inFlight()).Scan(&n); err != nil {
		return 0, fmt.Errorf("count in-flight deployments: %w", err)
	}
	return n, nil
}

// reserveNextQuery ranks due pending rows in three steps. grp_turn numbers each
// row within its run (production, by run_id) or release (candidates, by
// release_id). lane_turn interleaves the runs or releases of one lane by those
// turns. The final order alternates the two lanes, so a lane with many queued
// rows cannot starve the other, and one large run or release cannot starve the
// rest of its lane. A production row of a fenced run (in cancelled_runs) is
// never due: no slot is taken for it, and the fence ends it stopped. The
// UPDATE re-checks status under the row lock.
const reserveNextQuery = `
	WITH due AS (
		SELECT id, next_attempt_at,
		       CASE WHEN mode = 'production' THEN 'production' ELSE 'candidate' END AS lane,
		       CASE WHEN mode = 'production' THEN run_id::text ELSE release_id END AS grp
		FROM deployments d
		WHERE status = $2 AND next_attempt_at <= NOW()
		  AND (mode <> 'production' OR NOT EXISTS (SELECT 1 FROM cancelled_runs c WHERE c.run_id = d.run_id))
	), turns AS (
		SELECT id, lane, next_attempt_at,
		       row_number() OVER (PARTITION BY lane, grp ORDER BY next_attempt_at, id) AS grp_turn
		FROM due
	), lanes AS (
		SELECT id, lane,
		       row_number() OVER (PARTITION BY lane ORDER BY grp_turn, next_attempt_at, id) AS lane_turn
		FROM turns
	), picked AS (
		SELECT id FROM lanes ORDER BY lane_turn, lane LIMIT $1
	)
	UPDATE deployments d
	SET status = $3, state_changed_at = NOW()
	FROM picked
	WHERE d.id = picked.id AND d.status = $2
	RETURNING d.id`

func (r *admissionRepository) ReserveNext(ctx context.Context, limit int) ([]uuid.UUID, error) {
	var ids []uuid.UUID
	if err := r.exec.SelectContext(ctx, &ids, reserveNextQuery,
		limit, string(model.StatusPending), string(model.StatusReserved)); err != nil {
		return nil, fmt.Errorf("reserve next deployments: %w", err)
	}
	return ids, nil
}

func (r *admissionRepository) LoadReserved(ctx context.Context, id uuid.UUID) (*model.Deployment, error) {
	return r.loadLocked(ctx, id, pq.Array([]string{string(model.StatusReserved)}))
}

// loadLocked returns row id when its status is one of statuses (a bound
// text[]), locked FOR UPDATE SKIP LOCKED, or sql.ErrNoRows.
func (r *admissionRepository) loadLocked(ctx context.Context, id uuid.UUID, statuses any) (*model.Deployment, error) {
	query := `SELECT` + validationSelectColumns + ` FROM deployments WHERE id = $1 AND status = ANY($2) FOR UPDATE SKIP LOCKED`
	var rows []*deploymentRow
	if err := r.exec.SelectContext(ctx, &rows, query, id, statuses); err != nil {
		return nil, fmt.Errorf("load deployment %s: %w", id, err)
	}
	if len(rows) == 0 {
		return nil, sql.ErrNoRows
	}
	return r.rows.toAggregate(rows[0]), nil
}

func (r *admissionRepository) ReturnStaleReserved(ctx context.Context, olderThan time.Duration) ([]uuid.UUID, error) {
	var ids []uuid.UUID
	if err := r.exec.SelectContext(ctx, &ids, `
		UPDATE deployments
		SET status = $3, state_changed_at = NOW()
		WHERE id IN (
			SELECT id FROM deployments
			WHERE status = $2 AND state_changed_at < NOW() - make_interval(secs => $1)
			FOR UPDATE SKIP LOCKED)
		RETURNING id`,
		olderThan.Seconds(), string(model.StatusReserved), string(model.StatusPending)); err != nil {
		return nil, fmt.Errorf("return stale reservations: %w", err)
	}
	return ids, nil
}
