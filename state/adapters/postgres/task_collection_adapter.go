package postgres

import (
	"context"
	"errors"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
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

func (a *TaskCollectionAdapter) GetStatus(ctx context.Context, taskID uuid.UUID) (run.TaskStatus, bool, error) {
	s, err := a.repo.GetStatusTx(ctx, a.tx, taskID)
	if err != nil {
		return "", false, err
	}
	if s == "" {
		return "", false, nil
	}
	parsed, err := run.ParseTaskStatus(s)
	if err != nil {
		return "", true, err
	}
	return parsed, true, nil
}

func (a *TaskCollectionAdapter) GetStatusAndAttempt(ctx context.Context, taskID uuid.UUID) (run.TaskStatus, int32, bool, error) {
	s, retryCount, err := a.repo.GetStatusAndAttemptTx(ctx, a.tx, taskID)
	if err != nil {
		return "", 0, false, err
	}
	if s == "" {
		return "", 0, false, nil
	}
	parsed, err := run.ParseTaskStatus(s)
	if err != nil {
		return "", retryCount, true, err
	}
	return parsed, retryCount, true, nil
}

func (a *TaskCollectionAdapter) Exists(ctx context.Context, taskID uuid.UUID) (bool, error) {
	return a.repo.ExistsTx(ctx, a.tx, taskID)
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
	return run.Task{
		TaskID:              t.TaskID,
		ScheduleID:          t.ScheduleID,
		CreatedAt:           t.CreatedAt,
		ServiceName:         t.ServiceName,
		SchemaName:          t.SchemaName,
		TableName:           t.TableName,
		JobName:             t.JobName,
		Status:              t.Status,
		RetryCount:          t.RetryCount,
		MaxRetries:          t.MaxRetries,
		CancelledAt:         t.CancelledAt,
		CancelledBy:         t.CancelledBy,
		ManifestVersion:     t.ManifestVersion,
		ImageTag:            t.ImageTag,
		InheritedFromTaskID: t.InheritedFromTaskID,
	}, nil
}

func (a *TaskCollectionAdapter) UpdateStatusIfChanged(ctx context.Context, taskID uuid.UUID, status run.TaskStatus, retryCount int32) (int, error) {
	n, err := a.repo.UpdateStatusIfChangedTx(ctx, a.tx, taskID, string(status), retryCount)
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
			RetryCount:          t.RetryCount,
			MaxRetries:          t.MaxRetries,
			CancelledAt:         t.CancelledAt,
			CancelledBy:         t.CancelledBy,
			ManifestVersion:     t.ManifestVersion,
			ImageTag:            t.ImageTag,
			InheritedFromTaskID: t.InheritedFromTaskID,
		})
	}
	return a.repo.BulkCreateTx(ctx, a.tx, rows)
}

func (a *TaskCollectionAdapter) BulkCancel(ctx context.Context, runID uuid.UUID, cancelledBy string) (int, error) {
	n, err := a.repo.BulkCancelByScheduleIDTx(ctx, a.tx, runID, cancelledBy)
	return int(n), err
}

func (a *TaskCollectionAdapter) Update(ctx context.Context, t run.Task) error {
	return a.repo.UpdateTx(ctx, a.tx, &TaskTracker{
		TaskID:              t.TaskID,
		ScheduleID:          t.ScheduleID,
		CreatedAt:           t.CreatedAt,
		ServiceName:         t.ServiceName,
		SchemaName:          t.SchemaName,
		TableName:           t.TableName,
		JobName:             t.JobName,
		Status:              t.Status,
		RetryCount:          t.RetryCount,
		MaxRetries:          t.MaxRetries,
		CancelledAt:         t.CancelledAt,
		CancelledBy:         t.CancelledBy,
		ManifestVersion:     t.ManifestVersion,
		ImageTag:            t.ImageTag,
		InheritedFromTaskID: t.InheritedFromTaskID,
	})
}

// Compile-time interface assertion.
var _ run.TaskCollection = (*TaskCollectionAdapter)(nil)
