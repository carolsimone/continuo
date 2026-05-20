package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
)

// deploymentsRepository is the Postgres adapter implementing
// repository.DeploymentRepository over the executor_deployments table.
type deploymentsRepository struct {
	exec   outbox.Executor
	logger *slog.Logger
}

// NewDeploymentsRepository constructs a repository.DeploymentRepository over
// executor_deployments. Pass *sqlx.DB for autocommit or *sqlx.Tx for
// transactional use; outbox.Executor abstracts both.
func NewDeploymentsRepository(exec outbox.Executor, logger *slog.Logger) repository.DeploymentRepository {
	return &deploymentsRepository{exec: exec, logger: logger}
}

// deploymentRow is the adapter-internal scan struct. The Deployment aggregate
// itself carries no persistence tags; mapping happens here.
type deploymentRow struct {
	ID                  uuid.UUID  `db:"id"`
	MessageProcessingID *uuid.UUID `db:"message_processing_id"`
	TaskID              uuid.UUID  `db:"task_id"`
	ScheduleID          uuid.UUID  `db:"schedule_id"`
	JobParams           []byte     `db:"job_params"`
	Status              string     `db:"status"`
	RetryCount          int        `db:"retry_count"`
	MaxRetries          int        `db:"max_retries"`
	NextAttemptAt       time.Time  `db:"next_attempt_at"`
	CreatedAt           time.Time  `db:"created_at"`
	DeployedAt          *time.Time `db:"deployed_at"`
	ErrorMessage        *string    `db:"error_message"`
}

func (r *deploymentsRepository) Add(ctx context.Context, d *model.Deployment) error {
	cmd := d.Command()
	jobParams, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal deploy command: %w", err)
	}
	taskID, scheduleID, err := commandIDs(cmd)
	if err != nil {
		return err
	}

	const query = `
		INSERT INTO executor_deployments (
			id, message_processing_id, task_id, schedule_id, job_params,
			status, retry_count, max_retries, next_attempt_at, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`
	if _, err := r.exec.ExecContext(ctx, query,
		d.ID(), d.MessageProcessingID(), taskID, scheduleID, jobParams,
		string(d.Status()), d.RetryCount(), d.MaxRetries(), d.NextAttemptAt(), d.CreatedAt(),
	); err != nil {
		return fmt.Errorf("insert executor_deployments row: %w", err)
	}
	return nil
}

func (r *deploymentsRepository) GetDueBatch(ctx context.Context, limit int) ([]*model.Deployment, error) {
	const query = `
		SELECT id, message_processing_id, task_id, schedule_id, job_params,
		       status, retry_count, max_retries, next_attempt_at,
		       created_at, deployed_at, error_message
		FROM executor_deployments
		WHERE status = 'pending' AND next_attempt_at <= NOW()
		ORDER BY next_attempt_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED`
	var rows []*deploymentRow
	if err := r.exec.SelectContext(ctx, &rows, query, limit); err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("get due deployments batch: %w", err)
	}
	out := make([]*model.Deployment, len(rows))
	for i, row := range rows {
		out[i] = r.toAggregate(row)
	}
	return out, nil
}

func (r *deploymentsRepository) Save(ctx context.Context, d *model.Deployment) error {
	const query = `
		UPDATE executor_deployments
		SET status = $2, retry_count = $3, next_attempt_at = $4, deployed_at = $5, error_message = $6
		WHERE id = $1`
	res, err := r.exec.ExecContext(ctx, query,
		d.ID(), string(d.Status()), d.RetryCount(), d.NextAttemptAt(), d.DeployedAt(), d.ErrorMessage(),
	)
	if err != nil {
		return fmt.Errorf("save deployment %s: %w", d.ID(), err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("deployment %s not found for save", d.ID())
	}
	return nil
}

// toAggregate reconstitutes a Deployment from a row. If job_params cannot be
// deserialized (data corruption — it is written from a valid command) the
// task/schedule identity is recovered from the dedicated columns so the
// dispatcher can still fail the deployment with a routable announcement; the
// aggregate's IsDeployable() will report false.
func (r *deploymentsRepository) toAggregate(row *deploymentRow) *model.Deployment {
	var cmd command.DeployTask
	if err := json.Unmarshal(row.JobParams, &cmd); err != nil {
		r.logger.Error("deployment job_params unparseable — recovering identity from columns",
			"deployment_id", row.ID, "error", err)
		cmd = command.DeployTask{TaskID: row.TaskID.String(), ScheduleID: row.ScheduleID.String()}
	}
	return model.Reconstitute(
		row.ID, row.MessageProcessingID, cmd, model.Status(row.Status),
		row.RetryCount, row.MaxRetries, row.NextAttemptAt, row.CreatedAt,
		row.DeployedAt, row.ErrorMessage,
	)
}

// commandIDs parses the command's task/schedule identity into the UUID column
// values. The command holds them as strings (its wire form); the table stores
// them as typed UUID columns for indexing and identity recovery.
func commandIDs(cmd command.DeployTask) (uuid.UUID, uuid.UUID, error) {
	taskID, err := uuid.Parse(cmd.TaskID)
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("parse task_id %q: %w", cmd.TaskID, err)
	}
	scheduleID, err := uuid.Parse(cmd.ScheduleID)
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("parse schedule_id %q: %w", cmd.ScheduleID, err)
	}
	return taskID, scheduleID, nil
}
