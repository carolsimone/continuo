package run_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
)

func TestNewPendingRun_RecordsRunStarted(t *testing.T) {
	now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)
	meta := map[string]run.ServiceMetadata{
		"orders": {ManifestVersion: "v1", ImageTag: "abc"},
	}

	r, evt, err := run.NewPendingRun("daily_orders", run.KindCron, nil, meta, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.ScheduleName() != "daily_orders" {
		t.Fatalf("schedule_name: got %q want %q", r.ScheduleName(), "daily_orders")
	}
	if r.Status() != run.SchedulerStatusPending {
		t.Fatalf("status: got %q want pending", r.Status())
	}
	if r.Kind() != run.KindCron {
		t.Fatalf("kind: got %q want cron", r.Kind())
	}
	if r.ScheduleID() == uuid.Nil {
		t.Fatalf("schedule_id should be generated")
	}
	started, ok := evt.(run.RunStarted)
	if !ok {
		t.Fatalf("expected RunStarted, got %T", evt)
	}
	if started.ID != r.ScheduleID() || started.Name != "daily_orders" {
		t.Fatalf("event identity mismatch: %+v", started)
	}
	if len(started.ServiceMetadata) != 1 || started.ServiceMetadata["orders"].ImageTag != "abc" {
		t.Fatalf("service metadata not carried: %+v", started.ServiceMetadata)
	}
}

func TestNewPendingRun_RejectsEmptyName(t *testing.T) {
	_, _, err := run.NewPendingRun("", run.KindCron, nil, nil, time.Now())
	if err != run.ErrScheduleNameRequired {
		t.Fatalf("err: got %v want %v", err, run.ErrScheduleNameRequired)
	}
}

func TestNewPendingRun_RejectsInvalidKind(t *testing.T) {
	_, _, err := run.NewPendingRun("x", run.Kind("bogus"), nil, nil, time.Now())
	if err != run.ErrInvalidKind {
		t.Fatalf("err: got %v want %v", err, run.ErrInvalidKind)
	}
}

// fakeTaskCollection is the in-memory test double used by every aggregate
// unit test in this file. Tests set fields per scenario.
type fakeTaskCollection struct {
	bulkCreated   []run.Task
	bulkCancelled map[uuid.UUID]string
	statuses      map[uuid.UUID]run.TaskStatus
	hasFailed     bool
	hasRetryable  bool
	hasNonSucc    bool
	byNode        map[run.NodeID]run.Task
	updateApplied map[uuid.UUID]int
}

func newFakeTaskCollection() *fakeTaskCollection {
	return &fakeTaskCollection{
		bulkCancelled: map[uuid.UUID]string{},
		statuses:      map[uuid.UUID]run.TaskStatus{},
		byNode:        map[run.NodeID]run.Task{},
		updateApplied: map[uuid.UUID]int{},
	}
}

func (f *fakeTaskCollection) GetStatus(_ context.Context, id uuid.UUID) (run.TaskStatus, bool, error) {
	s, ok := f.statuses[id]
	return s, ok, nil
}
func (f *fakeTaskCollection) Exists(_ context.Context, id uuid.UUID) (bool, error) {
	_, ok := f.statuses[id]
	return ok, nil
}
func (f *fakeTaskCollection) HasFailed(_ context.Context, _ uuid.UUID) (bool, error) {
	return f.hasFailed, nil
}
func (f *fakeTaskCollection) HasRetryableFailed(_ context.Context, _ uuid.UUID) (bool, error) {
	return f.hasRetryable, nil
}
func (f *fakeTaskCollection) HasNonSucceeded(_ context.Context, _ uuid.UUID) (bool, error) {
	return f.hasNonSucc, nil
}
func (f *fakeTaskCollection) GetByNode(_ context.Context, _ uuid.UUID, n run.NodeID) (run.Task, error) {
	t, ok := f.byNode[n]
	if !ok {
		return run.Task{}, run.ErrTaskNotFound
	}
	return t, nil
}
func (f *fakeTaskCollection) UpdateStatusIfChanged(_ context.Context, id uuid.UUID, s run.TaskStatus, _ int32) (int, error) {
	prev := f.statuses[id]
	if prev == s {
		f.updateApplied[id] = 0
		return 0, nil
	}
	f.statuses[id] = s
	f.updateApplied[id] = 1
	return 1, nil
}
func (f *fakeTaskCollection) BulkCreate(_ context.Context, tasks []run.Task) error {
	f.bulkCreated = append(f.bulkCreated, tasks...)
	for _, t := range tasks {
		f.statuses[t.TaskID] = t.Status
	}
	return nil
}
func (f *fakeTaskCollection) BulkCancel(_ context.Context, runID uuid.UUID, by string) (int, error) {
	f.bulkCancelled[runID] = by
	return 1, nil
}
func (f *fakeTaskCollection) Update(_ context.Context, t run.Task) error {
	f.statuses[t.TaskID] = t.Status
	return nil
}

// freshPendingRun returns a Run in PENDING + init_status=in_progress with
// zero counters — the state on entry to AcceptDispatch.
func freshPendingRun(t *testing.T) *run.Run {
	t.Helper()
	r, _, err := run.NewPendingRun("daily", run.KindCron, nil, nil, time.Now())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	return r
}

func TestAcceptDispatch_TransitionsToRunningWhenAnyTaskPending(t *testing.T) {
	ctx := context.Background()
	r := freshPendingRun(t)
	tc := newFakeTaskCollection()
	now := time.Now()

	projection := []run.DispatchedTask{
		{TaskID: uuid.New(), ServiceName: "s", SchemaName: "p", TableName: "a", Status: run.TaskStatusPending, MaxRetries: 3},
		{TaskID: uuid.New(), ServiceName: "s", SchemaName: "p", TableName: "b", Status: run.TaskStatusSucceeded, MaxRetries: 3},
	}
	events, err := r.AcceptDispatch(ctx, tc, projection, now)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if r.Status() != run.SchedulerStatusRunning {
		t.Fatalf("status: got %s want running", r.Status())
	}
	if r.InitializationStatus() != run.InitStatusCompleted {
		t.Fatalf("init: got %s want completed", r.InitializationStatus())
	}
	if r.TerminalTaskCount() != 1 {
		t.Fatalf("terminal_task_count: got %d want 1 (seeded from inherited SUCCEEDED)", r.TerminalTaskCount())
	}
	if !r.TotalTaskCount().Valid || r.TotalTaskCount().Int32 != 2 {
		t.Fatalf("total_task_count: got %v want 2", r.TotalTaskCount())
	}
	if len(tc.bulkCreated) != 2 {
		t.Fatalf("BulkCreate called with %d tasks, want 2", len(tc.bulkCreated))
	}
	if len(events) != 0 {
		t.Fatalf("expected no events on RUNNING transition, got %d", len(events))
	}
}

func TestAcceptDispatch_AutoRollupsAllSucceeded(t *testing.T) {
	ctx := context.Background()
	r := freshPendingRun(t)
	tc := newFakeTaskCollection()
	now := time.Now()

	projection := []run.DispatchedTask{
		{TaskID: uuid.New(), ServiceName: "s", SchemaName: "p", TableName: "a", Status: run.TaskStatusSucceeded, MaxRetries: 3},
		{TaskID: uuid.New(), ServiceName: "s", SchemaName: "p", TableName: "b", Status: run.TaskStatusSucceeded, MaxRetries: 3},
	}
	events, err := r.AcceptDispatch(ctx, tc, projection, now)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if r.Status() != run.SchedulerStatusSucceeded {
		t.Fatalf("status: got %s want succeeded (auto-rollup)", r.Status())
	}
	if r.CompletedAt() == nil {
		t.Fatalf("completed_at should be set on auto-rollup")
	}
	if r.TerminalTaskCount() != 2 {
		t.Fatalf("terminal_task_count: got %d want 2", r.TerminalTaskCount())
	}
	if len(events) != 1 {
		t.Fatalf("expected one RunFinalized event, got %d", len(events))
	}
	fin, ok := events[0].(run.RunFinalized)
	if !ok || fin.Outcome != run.SchedulerStatusSucceeded {
		t.Fatalf("expected RunFinalized{Outcome=succeeded}, got %+v", events[0])
	}
}

func TestAcceptDispatch_AutoRollupsFailedWhenAnyTerminalNonSucceeded(t *testing.T) {
	ctx := context.Background()
	r := freshPendingRun(t)
	tc := newFakeTaskCollection()
	now := time.Now()

	projection := []run.DispatchedTask{
		{TaskID: uuid.New(), ServiceName: "s", SchemaName: "p", TableName: "a", Status: run.TaskStatusSucceeded, MaxRetries: 3},
		{TaskID: uuid.New(), ServiceName: "s", SchemaName: "p", TableName: "b", Status: run.TaskStatusFailed, MaxRetries: 3},
	}
	events, err := r.AcceptDispatch(ctx, tc, projection, now)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if r.Status() != run.SchedulerStatusFailed {
		t.Fatalf("status: got %s want failed", r.Status())
	}
	fin, ok := events[0].(run.RunFinalized)
	if !ok || fin.Outcome != run.SchedulerStatusFailed {
		t.Fatalf("expected RunFinalized{Outcome=failed}, got %+v", events[0])
	}
}

func TestAcceptDispatch_NoOpWhenAlreadyTerminal(t *testing.T) {
	ctx := context.Background()
	r := freshPendingRun(t)
	tc := newFakeTaskCollection()
	// Force terminal via the cancel path before exercising AcceptDispatch.
	_, _ = r.Cancel(ctx, tc, "tester", "drift", time.Now())

	events, err := r.AcceptDispatch(ctx, tc, []run.DispatchedTask{
		{TaskID: uuid.New(), ServiceName: "s", SchemaName: "p", TableName: "a", Status: run.TaskStatusPending},
	}, time.Now())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(events) != 0 || len(tc.bulkCreated) != 0 {
		t.Fatalf("expected no-op on terminal run; events=%d bulk=%d", len(events), len(tc.bulkCreated))
	}
}
