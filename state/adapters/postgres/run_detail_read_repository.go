package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/projection"
	"github.com/carolsimone/continuo/state/domain/repository"
)

var (
	_ repository.RunDetailReader  = (*RunRepository)(nil)
	_ repository.LastRunReader    = (*RunRepository)(nil)
	_ repository.TaskDetailReader = (*TaskReadRepository)(nil)
)

// GetRunDetail returns the tenant's run runID for display, or
// repository.ErrRunNotFound when the tenant has no such run.
func (r *RunRepository) GetRunDetail(ctx context.Context, tenantID string, runID uuid.UUID) (projection.RunDetail, error) {
	row, err := r.GetByID(ctx, tenantID, runID)
	if errors.Is(err, ErrNotFound) {
		return projection.RunDetail{}, repository.ErrRunNotFound
	}
	if err != nil {
		return projection.RunDetail{}, err
	}
	return runDetailOf(row), nil
}

func runDetailOf(row *RunRow) projection.RunDetail {
	d := projection.RunDetail{
		RunID:          row.ScheduleID,
		ScheduleName:   row.ScheduleName,
		Kind:           row.Kind,
		Operation:      row.Operation,
		Status:         row.Status,
		InitiatedBy:    row.InitiatedBy,
		CreatedAt:      row.CreatedAt,
		StartedAt:      row.StartedAt,
		CompletedAt:    row.CompletedAt,
		LastProgressAt: row.LastProgressAt,
		CancelledAt:    row.CancelledAt,
	}
	if row.FailureReason != nil {
		d.FailureReason = *row.FailureReason
	}
	if row.ReleaseID != nil {
		d.ReleaseID = *row.ReleaseID
	}
	if row.PromotionSeq != nil {
		d.PromotionSeq = *row.PromotionSeq
	}
	if row.CancelledBy != nil {
		d.CancelledBy = *row.CancelledBy
	}
	if row.CancellationReason != nil {
		d.CancellationReason = *row.CancellationReason
	}
	return d
}

// TaskReadRepository reads the tasks of a tenant's runs for display.
type TaskReadRepository struct {
	tasks *TaskCollection
}

// NewTaskReadRepository returns a TaskReadRepository over tasks, which it
// binds to the tenant of each read.
func NewTaskReadRepository(tasks *TaskCollection) *TaskReadRepository {
	return &TaskReadRepository{tasks: tasks}
}

// ListRunTasks returns one page of the tasks of the tenant's run runID, newest
// first, only those in status when it is not nil, and how many tasks match.
// Each task carries the run holding the node lease it waits for.
func (r *TaskReadRepository) ListRunTasks(ctx context.Context, tenantID string, runID uuid.UUID, status *run.TaskStatus, limit, offset int) ([]projection.TaskDetail, int, error) {
	rows, total, err := r.tasks.ForTenant(tenantID).ListByRunID(ctx, runID, status, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	out := make([]projection.TaskDetail, 0, len(rows))
	for _, row := range rows {
		out = append(out, taskDetailOf(row))
	}
	return out, total, nil
}

func taskDetailOf(row *TaskRow) projection.TaskDetail {
	d := projection.TaskDetail{
		TaskID:         row.TaskID,
		RunID:          row.ScheduleID,
		CreatedAt:      row.CreatedAt,
		ServiceName:    row.ServiceName,
		SchemaName:     row.SchemaName,
		TableName:      row.TableName,
		JobName:        row.JobName,
		Status:         row.Status,
		Attempt:        row.Attempt,
		MaxRetries:     row.MaxRetries,
		CancelledAt:    row.CancelledAt,
		WaitingSince:   row.LeaseWaitSince,
		WaitingOnRunID: row.WaitingOnRunID,
	}
	if row.CancelledBy != nil {
		d.CancelledBy = *row.CancelledBy
	}
	if row.WaitReason != nil {
		d.WaitReason = run.WaitReason(*row.WaitReason)
	}
	return d
}
