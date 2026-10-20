package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/domain/repository"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

// observedDeployments is the Postgres adapter implementing
// repository.ObservedDeployments over the deployments table.
type observedDeployments struct {
	exec outbox.Executor
	rows *deploymentsRepository // reconstitutes aggregates from rows
}

var _ repository.ObservedDeployments = (*observedDeployments)(nil)

// NewObservedDeployments constructs a repository.ObservedDeployments; pass
// *sqlx.DB, since it reads outside any transaction.
func NewObservedDeployments(exec outbox.Executor, logger *slog.Logger) repository.ObservedDeployments {
	return &observedDeployments{exec: exec, rows: &deploymentsRepository{exec: exec, logger: logger}}
}

// startedStatuses binds the statuses of a deployment whose Job exists and
// whose attempt has not ended.
func startedStatuses() any {
	return pq.Array(model.StatusStrings([]model.Status{model.StatusStarting, model.StatusRunning, model.StatusTerminating}))
}

func (r *observedDeployments) FindByJobName(ctx context.Context, jobName string) (*model.Deployment, error) {
	var rows []*deploymentRow
	if err := r.exec.SelectContext(ctx, &rows, `SELECT`+validationSelectColumns+`
		FROM deployments
		WHERE job_name = $1
		ORDER BY created_at DESC
		LIMIT 1`, jobName); err != nil {
		return nil, fmt.Errorf("find deployment for job %s: %w", jobName, err)
	}
	if len(rows) == 0 {
		return nil, sql.ErrNoRows
	}
	return r.rows.toAggregate(rows[0]), nil
}

func (r *observedDeployments) ListObservedJobNames(ctx context.Context) ([]string, error) {
	names := []string{}
	if err := r.exec.SelectContext(ctx, &names, `
		SELECT job_name FROM deployments
		WHERE job_name IS NOT NULL
		  AND (status = ANY($1) OR (status = $2 AND outcome IS NULL))`,
		startedStatuses(), string(model.StatusDone)); err != nil {
		return nil, fmt.Errorf("list observed deployments: %w", err)
	}
	return names, nil
}

func (r *observedDeployments) StateAge(ctx context.Context, id uuid.UUID) (time.Duration, error) {
	var seconds float64
	if err := r.exec.QueryRowContext(ctx,
		`SELECT EXTRACT(EPOCH FROM NOW() - state_changed_at)::float8 FROM deployments WHERE id = $1`, id).Scan(&seconds); err != nil {
		return 0, fmt.Errorf("read state age of deployment %s: %w", id, err)
	}
	return time.Duration(seconds * float64(time.Second)), nil
}
