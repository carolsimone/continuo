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

var _ repository.DeploymentRepository = (*deploymentsRepository)(nil)

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
	Mode                string     `db:"mode"`
	ReleaseID           *string    `db:"release_id"`
	NodeID              *string    `db:"node_id"`
	Outcome             *string    `db:"outcome"`
	DBTLogURI           *string    `db:"dbt_log_uri"`
	RunResultsURI       *string    `db:"run_results_uri"`
	FailedContainer     *string    `db:"failed_container"`
	OutcomeAt           *time.Time `db:"outcome_at"`
}

func (r *deploymentsRepository) Add(ctx context.Context, d *model.Deployment) error {
	var (
		jobParams          []byte
		taskID, scheduleID uuid.UUID
		releaseID, nodeID  *string
		err                error
	)
	if d.Mode() == model.ModeValidation || d.Mode() == model.ModeSeedBuild || d.Mode() == model.ModeCompile {
		vcmd := d.ValidationCommand()
		if jobParams, err = json.Marshal(vcmd); err != nil {
			return fmt.Errorf("marshal validation deploy command: %w", err)
		}
		// task_id/schedule_id are NOT NULL but validation rows have no real
		// task/schedule identity; derive stable synthetic UUIDs from (release_id,
		// node_id) so re-adds map to the same row.
		taskID, scheduleID = model.ValidationSyntheticIDs(vcmd.ReleaseID, vcmd.NodeID)
		rid, nid := vcmd.ReleaseID, vcmd.NodeID
		releaseID, nodeID = &rid, &nid
	} else {
		cmd := d.Command()
		if jobParams, err = json.Marshal(cmd); err != nil {
			return fmt.Errorf("marshal deploy command: %w", err)
		}
		if taskID, scheduleID, err = commandIDs(cmd); err != nil {
			return err
		}
	}

	const query = `
		INSERT INTO executor_deployments (
			id, message_processing_id, task_id, schedule_id, job_params,
			status, retry_count, max_retries, next_attempt_at, created_at,
			mode, release_id, node_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`
	if _, err := r.exec.ExecContext(ctx, query,
		d.ID(), d.MessageProcessingID(), taskID, scheduleID, jobParams,
		string(d.Status()), d.RetryCount(), d.MaxRetries(), d.NextAttemptAt(), d.CreatedAt(),
		string(d.Mode()), releaseID, nodeID,
	); err != nil {
		return fmt.Errorf("insert executor_deployments row: %w", err)
	}
	return nil
}

func (r *deploymentsRepository) GetDueBatch(ctx context.Context, limit int) ([]*model.Deployment, error) {
	const query = `
		SELECT id, message_processing_id, task_id, schedule_id, job_params,
		       status, retry_count, max_retries, next_attempt_at,
		       created_at, deployed_at, error_message,
		       mode, release_id, node_id, outcome, dbt_log_uri, outcome_at, run_results_uri, failed_container
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
		SET status = $2, retry_count = $3, next_attempt_at = $4, deployed_at = $5, error_message = $6,
		    outcome = $7, dbt_log_uri = $8, outcome_at = $9, run_results_uri = $10, failed_container = $11
		WHERE id = $1`
	res, err := r.exec.ExecContext(ctx, query,
		d.ID(), string(d.Status()), d.RetryCount(), d.NextAttemptAt(), d.DeployedAt(), d.ErrorMessage(),
		nullableStr(d.Outcome()), nullableStr(d.DBTLogURI()), d.OutcomeAt(), nullableStr(d.DBTRunResultsURI()),
		nullableStr(d.FailedContainer()),
	)
	if err != nil {
		return fmt.Errorf("save deployment %s: %w", d.ID(), err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("deployment %s not found for save", d.ID())
	}
	return nil
}

// validationSelectColumns is the column list reconstituted into a Deployment by
// the validation getters. Mirrors the order deploymentRow's db tags expect.
const validationSelectColumns = `
	id, message_processing_id, task_id, schedule_id, job_params,
	status, retry_count, max_retries, next_attempt_at,
	created_at, deployed_at, error_message,
	mode, release_id, node_id, outcome, dbt_log_uri, outcome_at, run_results_uri, failed_container`

func (r *deploymentsRepository) GetByReleaseNode(ctx context.Context, releaseID, nodeID string, mode model.Mode) (*model.Deployment, error) {
	const query = `
		SELECT` + validationSelectColumns + `
		FROM executor_deployments
		WHERE mode = $3 AND release_id = $1 AND node_id = $2`
	var row deploymentRow
	if err := r.exec.QueryRowContext(ctx, query, releaseID, nodeID, string(mode)).Scan(
		&row.ID, &row.MessageProcessingID, &row.TaskID, &row.ScheduleID, &row.JobParams,
		&row.Status, &row.RetryCount, &row.MaxRetries, &row.NextAttemptAt,
		&row.CreatedAt, &row.DeployedAt, &row.ErrorMessage,
		&row.Mode, &row.ReleaseID, &row.NodeID, &row.Outcome, &row.DBTLogURI, &row.OutcomeAt, &row.RunResultsURI,
		&row.FailedContainer,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("get validation deployment for release %s node %s: %w", releaseID, nodeID, err)
	}
	return r.toAggregate(&row), nil
}

func (r *deploymentsRepository) PendingValidationCount(ctx context.Context, releaseID string, mode model.Mode) (int, error) {
	const query = `
		SELECT COUNT(*)
		FROM executor_deployments
		WHERE mode = $2 AND release_id = $1
		  AND status IN ('pending','blocked','deployed') AND outcome IS NULL`
	var n int
	if err := r.exec.QueryRowContext(ctx, query, releaseID, string(mode)).Scan(&n); err != nil {
		return 0, fmt.Errorf("count pending validations for release %s: %w", releaseID, err)
	}
	return n, nil
}

func (r *deploymentsRepository) ListValidationResults(ctx context.Context, releaseID string, mode model.Mode) ([]*model.Deployment, error) {
	const query = `
		SELECT` + validationSelectColumns + `
		FROM executor_deployments
		WHERE mode = $2 AND release_id = $1 AND outcome IS NOT NULL
		ORDER BY outcome_at ASC`
	var rows []*deploymentRow
	if err := r.exec.SelectContext(ctx, &rows, query, releaseID, string(mode)); err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("list validation results for release %s: %w", releaseID, err)
	}
	out := make([]*model.Deployment, len(rows))
	for i, row := range rows {
		out[i] = r.toAggregate(row)
	}
	return out, nil
}

func (r *deploymentsRepository) ListValidationByRelease(ctx context.Context, releaseID string, mode model.Mode) ([]*model.Deployment, error) {
	const query = `
		SELECT` + validationSelectColumns + `
		FROM executor_deployments
		WHERE mode = $2 AND release_id = $1
		ORDER BY created_at ASC`
	var rows []*deploymentRow
	if err := r.exec.SelectContext(ctx, &rows, query, releaseID, string(mode)); err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("list validation deployments for release %s: %w", releaseID, err)
	}
	out := make([]*model.Deployment, len(rows))
	for i, row := range rows {
		out[i] = r.toAggregate(row)
	}
	return out, nil
}

// toAggregate reconstitutes a Deployment from a row. If job_params cannot be
// deserialized (data corruption — it is written from a valid command) the
// task/schedule identity is recovered from the dedicated columns so the
// dispatcher can still fail the deployment with a routable announcement; the
// aggregate's IsDeployable() will report false.
func (r *deploymentsRepository) toAggregate(row *deploymentRow) *model.Deployment {
	if row.Mode == string(model.ModeValidation) {
		var vcmd command.ValidationDeployTask
		if err := json.Unmarshal(row.JobParams, &vcmd); err != nil {
			r.logger.Error("validation deployment job_params unparseable — recovering identity from columns",
				"deployment_id", row.ID, "error", err)
			vcmd = command.ValidationDeployTask{
				ReleaseID: derefStr(row.ReleaseID),
				NodeID:    derefStr(row.NodeID),
			}
		}
		return model.ReconstituteValidation(
			row.ID, row.MessageProcessingID, vcmd, model.Status(row.Status),
			row.RetryCount, row.MaxRetries, row.NextAttemptAt, row.CreatedAt,
			row.DeployedAt, row.ErrorMessage,
			derefStr(row.Outcome), derefStr(row.DBTLogURI), derefStr(row.RunResultsURI), derefStr(row.FailedContainer), row.OutcomeAt,
		)
	}

	if row.Mode == string(model.ModeSeedBuild) {
		var vcmd command.ValidationDeployTask
		if err := json.Unmarshal(row.JobParams, &vcmd); err != nil {
			r.logger.Error("seed_build deployment job_params unparseable — recovering identity from columns",
				"deployment_id", row.ID, "error", err)
			vcmd = command.ValidationDeployTask{
				ReleaseID: derefStr(row.ReleaseID),
				NodeID:    derefStr(row.NodeID),
			}
		}
		return model.ReconstituteSeedBuild(
			row.ID, row.MessageProcessingID, vcmd, model.Status(row.Status),
			row.RetryCount, row.MaxRetries, row.NextAttemptAt, row.CreatedAt,
			row.DeployedAt, row.ErrorMessage,
			derefStr(row.Outcome), derefStr(row.DBTLogURI), derefStr(row.RunResultsURI), derefStr(row.FailedContainer), row.OutcomeAt,
		)
	}

	if row.Mode == string(model.ModeCompile) {
		var vcmd command.ValidationDeployTask
		if err := json.Unmarshal(row.JobParams, &vcmd); err != nil {
			r.logger.Error("compile deployment job_params unparseable — recovering identity from columns",
				"deployment_id", row.ID, "error", err)
			vcmd = command.ValidationDeployTask{
				ReleaseID: derefStr(row.ReleaseID),
				NodeID:    derefStr(row.NodeID),
			}
		}
		return model.ReconstituteCompile(
			row.ID, row.MessageProcessingID, vcmd, model.Status(row.Status),
			row.RetryCount, row.MaxRetries, row.NextAttemptAt, row.CreatedAt,
			row.DeployedAt, row.ErrorMessage,
			derefStr(row.Outcome), derefStr(row.DBTLogURI), derefStr(row.RunResultsURI), derefStr(row.FailedContainer), row.OutcomeAt,
		)
	}

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

// nullableStr maps an empty string to NULL so columns guarded by an
// IN (...) OR IS NULL check accept the "not yet set" state.
func nullableStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// derefStr returns the pointed-to string, or "" when the pointer is nil.
func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
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
