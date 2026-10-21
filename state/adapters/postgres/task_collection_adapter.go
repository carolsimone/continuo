package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// TaskCollectionAdapter satisfies run.TaskCollection by composing the existing
// TaskTrackerRepository methods and the active transaction. Construction is via
// NewTaskCollectionAdapter inside the UoW; never directly by handlers.
type TaskCollectionAdapter struct {
	repo TaskTrackerRepository
	tx   *sqlx.Tx
}

// NewTaskCollectionAdapter constructs the adapter bound to the given tx.
func NewTaskCollectionAdapter(repo TaskTrackerRepository, tx *sqlx.Tx) *TaskCollectionAdapter {
	return &TaskCollectionAdapter{repo: repo, tx: tx}
}

func (a *TaskCollectionAdapter) LoadAttemptState(ctx context.Context, taskID uuid.UUID) (run.TaskAttemptState, bool, error) {
	row, found, err := a.repo.LoadAttemptStateTx(ctx, a.tx, taskID)
	if err != nil || !found {
		return run.TaskAttemptState{}, false, err
	}
	status, err := run.ParseTaskStatus(row.Status)
	if err != nil {
		return run.TaskAttemptState{}, true, err
	}
	return run.TaskAttemptState{
		Status:     status,
		RetryCount: row.Attempt,
		MaxRetries: row.MaxRetries,
		NodeKey:    topology.NodeKey(row.NodeKey),
	}, true, nil
}

func (a *TaskCollectionAdapter) HasFailed(ctx context.Context, runID uuid.UUID) (bool, error) {
	return a.repo.HasFailedTaskTx(ctx, a.tx, runID)
}

func (a *TaskCollectionAdapter) HasRetryableFailed(ctx context.Context, runID uuid.UUID) (bool, error) {
	return a.repo.HasRetryableFailedTaskTx(ctx, a.tx, runID)
}

func (a *TaskCollectionAdapter) HasNonSucceeded(ctx context.Context, runID uuid.UUID) (bool, error) {
	return a.repo.HasNonSucceededTask(ctx, runID)
}

func (a *TaskCollectionAdapter) GetByNode(ctx context.Context, runID uuid.UUID, node run.NodeID) (run.Task, error) {
	t, err := a.repo.GetByScheduleAndNode(ctx, runID, node.ServiceName, node.SchemaName, node.TableName)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return run.Task{}, run.ErrTaskNotFound
		}
		return run.Task{}, err
	}
	return taskFromRow(t), nil
}

// ListRunTasks reads every task of a run through the unit of work's
// transaction: a derived run plans while its creating transaction holds a
// connection, and a read on a second one would wait for it under a small pool.
func (a *TaskCollectionAdapter) ListRunTasks(ctx context.Context, runID uuid.UUID) ([]run.Task, error) {
	rows, err := a.repo.ListByRunTx(ctx, a.tx, runID)
	if err != nil {
		return nil, err
	}
	out := make([]run.Task, 0, len(rows))
	for _, t := range rows {
		out = append(out, taskFromRow(t))
	}
	return out, nil
}

// RunReleasesOf reads the release of each task's run through the unit of
// work's transaction, which a rebase plans in.
func (a *TaskCollectionAdapter) RunReleasesOf(ctx context.Context, taskIDs []uuid.UUID) (map[uuid.UUID]string, error) {
	return a.repo.RunReleasesTx(ctx, a.tx, taskIDs)
}

// GetByID reads one task by id. A task's node and image never change once its
// row is written, so the read uses the autocommit db.
func (a *TaskCollectionAdapter) GetByID(ctx context.Context, taskID uuid.UUID) (run.Task, error) {
	t, err := a.repo.GetByID(ctx, taskID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return run.Task{}, run.ErrTaskNotFound
		}
		return run.Task{}, err
	}
	return taskFromRow(t), nil
}

// taskFromRow maps a task_tracker row onto the domain task.
func taskFromRow(t *TaskTracker) run.Task {
	return run.Task{
		TaskID:              t.TaskID,
		ScheduleID:          t.ScheduleID,
		CreatedAt:           t.CreatedAt,
		ServiceName:         t.ServiceName,
		SchemaName:          t.SchemaName,
		TableName:           t.TableName,
		JobName:             t.JobName,
		Status:              t.Status,
		RetryCount:          t.Attempt,
		MaxRetries:          t.MaxRetries,
		CancelledAt:         t.CancelledAt,
		CancelledBy:         t.CancelledBy,
		ImageTag:            t.ImageTag,
		InheritedFromTaskID: t.InheritedFromTaskID,
		Operation:           operationFromColumn(t.Operation),
		NodeKey:             topology.NodeKey(stringOrEmpty(t.NodeKey)),
		DispatchedAt:        t.DispatchedAt,
	}
}

func (a *TaskCollectionAdapter) SetStatusAndAttempt(ctx context.Context, taskID uuid.UUID, status run.TaskStatus, retryCount int32) (int, error) {
	n, err := a.repo.SetStatusAndAttemptTx(ctx, a.tx, taskID, string(status), retryCount)
	return int(n), err
}

func (a *TaskCollectionAdapter) BulkCreate(ctx context.Context, tasks []run.Task) error {
	rows := make([]*TaskTracker, 0, len(tasks))
	for _, t := range tasks {
		rows = append(rows, &TaskTracker{
			TaskID:              t.TaskID,
			ScheduleID:          t.ScheduleID,
			CreatedAt:           t.CreatedAt,
			ServiceName:         t.ServiceName,
			SchemaName:          t.SchemaName,
			TableName:           t.TableName,
			JobName:             t.JobName,
			Status:              t.Status,
			Attempt:             t.RetryCount,
			MaxRetries:          t.MaxRetries,
			CancelledAt:         t.CancelledAt,
			CancelledBy:         t.CancelledBy,
			ImageTag:            t.ImageTag,
			InheritedFromTaskID: t.InheritedFromTaskID,
			Operation:           operationWithDefault(string(t.Operation)),
			NodeKey:             optionalString(string(t.NodeKey)),
			DispatchedAt:        t.DispatchedAt,
		})
	}
	return a.repo.BulkCreateTx(ctx, a.tx, rows)
}

func (a *TaskCollectionAdapter) MarkDispatched(ctx context.Context, taskIDs []uuid.UUID, at time.Time) ([]uuid.UUID, error) {
	return a.repo.MarkDispatchedTx(ctx, a.tx, taskIDs, at)
}

func (a *TaskCollectionAdapter) MarkSkipped(ctx context.Context, taskIDs []uuid.UUID) (int, error) {
	n, err := a.repo.MarkSkippedTx(ctx, a.tx, taskIDs)
	return int(n), err
}

func (a *TaskCollectionAdapter) BulkCancel(ctx context.Context, runID uuid.UUID, cancelledBy string, cancelledAt time.Time) (int, error) {
	n, err := a.repo.BulkCancelByScheduleIDTx(ctx, a.tx, runID, cancelledBy, cancelledAt)
	return int(n), err
}

// Compile-time interface assertion.
var _ run.TaskCollection = (*TaskCollectionAdapter)(nil)
