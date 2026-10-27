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
// TaskRepository methods and the active transaction. Construction is via
// NewTaskCollectionAdapter inside the UoW; never directly by handlers.
type TaskCollectionAdapter struct {
	repo TaskRepository
	tx   *sqlx.Tx
	// tenantID is the tenant whose runs the tasks belong to: the node write
	// leases the adapter takes are this tenant's.
	tenantID string
}

// NewTaskCollectionAdapter constructs the adapter bound to the given tx, for
// the runs of tenantID.
func NewTaskCollectionAdapter(repo TaskRepository, tx *sqlx.Tx, tenantID string) *TaskCollectionAdapter {
	return &TaskCollectionAdapter{repo: repo, tx: tx, tenantID: tenantID}
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
		RunID:        row.RunID,
		Status:       status,
		Attempt:      row.Attempt,
		MaxRetries:   row.MaxRetries,
		NodeKey:      topology.NodeKey(row.NodeKey),
		JobName:      row.JobName,
		ServiceName:  row.ServiceName,
		SchemaName:   row.SchemaName,
		TableName:    row.TableName,
		DispatchedAt: row.DispatchedAt,
		WaitReason:   run.WaitReason(row.WaitReason),
	}, true, nil
}

// LoadAttempt, SaveAttempt and RequestStops run through the unit of work's
// transaction: the attempt row is locked after the task row and written in
// the same commit.
func (a *TaskCollectionAdapter) LoadAttempt(ctx context.Context, taskID uuid.UUID, attempt int32) (run.Attempt, bool, error) {
	return loadAttempt(ctx, a.tx, taskID, attempt)
}

func (a *TaskCollectionAdapter) SaveAttempt(ctx context.Context, at run.Attempt) error {
	return saveAttempt(ctx, a.tx, at)
}

func (a *TaskCollectionAdapter) AddAttempts(ctx context.Context, attempts []run.Attempt) error {
	return addAttempts(ctx, a.tx, attempts)
}

func (a *TaskCollectionAdapter) RequestStops(ctx context.Context, runID uuid.UUID) ([]run.StopTarget, error) {
	return requestStops(ctx, a.tx, runID)
}

func (a *TaskCollectionAdapter) HasFailed(ctx context.Context, runID uuid.UUID) (bool, error) {
	return a.repo.HasFailedTaskTx(ctx, a.tx, runID)
}

func (a *TaskCollectionAdapter) HasInFlight(ctx context.Context, runID uuid.UUID) (bool, error) {
	return a.repo.HasInFlightTaskTx(ctx, a.tx, runID)
}

func (a *TaskCollectionAdapter) HasNonSucceeded(ctx context.Context, runID uuid.UUID) (bool, error) {
	return a.repo.HasNonSucceededTask(ctx, runID)
}

func (a *TaskCollectionAdapter) GetByNode(ctx context.Context, runID uuid.UUID, node run.NodeID) (run.Task, error) {
	t, err := a.repo.GetByRunAndNode(ctx, runID, node.ServiceName, node.SchemaName, node.TableName)
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

// taskFromRow maps a task row onto the domain task.
func taskFromRow(t *TaskRow) run.Task {
	return run.Task{
		TaskID:              t.TaskID,
		ScheduleID:          t.ScheduleID,
		CreatedAt:           t.CreatedAt,
		ServiceName:         t.ServiceName,
		SchemaName:          t.SchemaName,
		TableName:           t.TableName,
		JobName:             t.JobName,
		Status:              t.Status,
		Attempt:             t.Attempt,
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

func (a *TaskCollectionAdapter) SetStatusAndAttempt(ctx context.Context, taskID uuid.UUID, status run.TaskStatus, attempt int32) (int, error) {
	n, err := a.repo.SetStatusAndAttemptTx(ctx, a.tx, taskID, string(status), attempt)
	return int(n), err
}

func (a *TaskCollectionAdapter) BulkCreate(ctx context.Context, tasks []run.Task) error {
	rows := make([]*TaskRow, 0, len(tasks))
	for _, t := range tasks {
		rows = append(rows, &TaskRow{
			TaskID:              t.TaskID,
			ScheduleID:          t.ScheduleID,
			CreatedAt:           t.CreatedAt,
			ServiceName:         t.ServiceName,
			SchemaName:          t.SchemaName,
			TableName:           t.TableName,
			JobName:             t.JobName,
			Status:              t.Status,
			Attempt:             t.Attempt,
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

func (a *TaskCollectionAdapter) SkipPending(ctx context.Context, runID uuid.UUID) (int, error) {
	n, err := a.repo.SkipPendingTx(ctx, a.tx, runID)
	return int(n), err
}

func (a *TaskCollectionAdapter) BeginStop(ctx context.Context, runID uuid.UUID, cancelledBy string, cancelledAt time.Time) (run.StopCounts, error) {
	return a.repo.BeginStopTx(ctx, a.tx, runID, cancelledBy, cancelledAt)
}

// TakeLeases, WaitForLeases, ReleaseLease, TakeLeaseForWaiter and
// GrantableWaits run through the unit of work's transaction, so a lease is
// taken or released, and a wait begins, in the commit that dispatches, makes
// wait or ends the task, and the waits a grant serves are read under the lock
// of the run it grants.
func (a *TaskCollectionAdapter) TakeLeases(ctx context.Context, claims []run.LeaseClaim) ([]uuid.UUID, error) {
	return takeLeases(ctx, a.tx, a.tenantID, claims)
}

func (a *TaskCollectionAdapter) WaitForLeases(ctx context.Context, taskIDs []uuid.UUID) error {
	return waitForLeases(ctx, a.tx, taskIDs)
}

func (a *TaskCollectionAdapter) ReleaseLease(ctx context.Context, taskID uuid.UUID) error {
	return releaseLeases(ctx, a.tx, []uuid.UUID{taskID})
}

func (a *TaskCollectionAdapter) TakeLeaseForWaiter(ctx context.Context, claim run.LeaseClaim) (bool, error) {
	return takeLeaseForWaiter(ctx, a.tx, a.tenantID, claim)
}

func (a *TaskCollectionAdapter) GrantableWaits(ctx context.Context, runID uuid.UUID) ([]run.LeaseWait, error) {
	return grantableWaitsOf(ctx, a.tx, a.tenantID, runID)
}

// Compile-time interface assertion.
var _ run.TaskCollection = (*TaskCollectionAdapter)(nil)
