package runreads

import (
	"context"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/projection"
	"github.com/carolsimone/continuo/state/domain/repository"
)

// DefaultTaskPageSize is the task-list page size when the caller sets none.
const DefaultTaskPageSize = 50

// RunDetails reads one run and the tasks of a run for display, each of the
// tenant it is given.
type RunDetails struct {
	runs  repository.RunDetailReader
	tasks repository.TaskDetailReader
}

// NewRunDetails returns a RunDetails over runs and tasks.
func NewRunDetails(runs repository.RunDetailReader, tasks repository.TaskDetailReader) *RunDetails {
	return &RunDetails{runs: runs, tasks: tasks}
}

// GetRun returns tenantID's run runID, or repository.ErrRunNotFound when the
// tenant has no such run.
func (d *RunDetails) GetRun(ctx context.Context, tenantID string, runID uuid.UUID) (projection.RunDetail, error) {
	return d.runs.GetRunDetail(ctx, tenantID, runID)
}

// ListTasks returns one page of the tasks of tenantID's run, newest first,
// only those in status when it is not nil, and how many tasks match. A page
// size below 1 becomes DefaultTaskPageSize and has no upper bound; an offset
// below 0 becomes 0. A run the tenant does not have has no tasks.
func (d *RunDetails) ListTasks(ctx context.Context, tenantID string, runID uuid.UUID, status *run.TaskStatus, pageSize, pageOffset int) ([]projection.TaskDetail, int, error) {
	if pageSize < 1 {
		pageSize = DefaultTaskPageSize
	}
	pageOffset = max(pageOffset, 0)
	return d.tasks.ListRunTasks(ctx, tenantID, runID, status, pageSize, pageOffset)
}
