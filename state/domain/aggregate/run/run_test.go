package run_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
)

func TestNewPendingRun_RecordsRunStarted(t *testing.T) {
	now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)
	meta := map[string]run.ServiceMetadata{
		"orders": {ManifestVersion: "v1", ImageTag: "abc"},
	}

	r, evt, err := run.NewPendingRun("daily_orders", run.KindCron, nil, identity.SystemUserID, meta, now)
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

func TestNewRun_StampsInitiatorOnAggregateAndEvent(t *testing.T) {
	now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)
	src := uuid.New()

	t.Run("pending run", func(t *testing.T) {
		r, evt, err := run.NewPendingRun("daily", run.KindTrigger, nil, "okta|alice", nil, now)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if r.InitiatedBy() != "okta|alice" {
			t.Fatalf("aggregate InitiatedBy: got %q want okta|alice", r.InitiatedBy())
		}
		if started := evt.(run.RunStarted); started.InitiatedBy != "okta|alice" {
			t.Fatalf("RunStarted.InitiatedBy: got %q want okta|alice", started.InitiatedBy)
		}
	})

	t.Run("derived rerun", func(t *testing.T) {
		r, evt, err := run.NewDerivedRun("daily", run.KindRerun, src, "okta|bob", now)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if r.InitiatedBy() != "okta|bob" {
			t.Fatalf("aggregate InitiatedBy: got %q want okta|bob", r.InitiatedBy())
		}
		if rr := evt.(run.RerunRequested); rr.InitiatedBy != "okta|bob" {
			t.Fatalf("RerunRequested.InitiatedBy: got %q want okta|bob", rr.InitiatedBy)
		}
	})

	t.Run("single node run", func(t *testing.T) {
		target := run.NodeID{ServiceName: "svc", SchemaName: "sch", TableName: "tbl"}
		r, evt, err := run.NewSingleNodeRun("single-node-run-abcd1234", target, run.MetadataSourceLatest, nil, "okta|carol", now)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if r.InitiatedBy() != "okta|carol" {
			t.Fatalf("aggregate InitiatedBy: got %q want okta|carol", r.InitiatedBy())
		}
		if sn := evt.(run.SingleNodeRunRequested); sn.InitiatedBy != "okta|carol" {
			t.Fatalf("SingleNodeRunRequested.InitiatedBy: got %q want okta|carol", sn.InitiatedBy)
		}
	})
}

func TestNewPendingRun_RejectsEmptyName(t *testing.T) {
	_, _, err := run.NewPendingRun("", run.KindCron, nil, identity.SystemUserID, nil, time.Now())
	if err != run.ErrScheduleNameRequired {
		t.Fatalf("err: got %v want %v", err, run.ErrScheduleNameRequired)
	}
}

func TestNewPendingRun_RejectsInvalidKind(t *testing.T) {
	_, _, err := run.NewPendingRun("x", run.Kind("bogus"), nil, identity.SystemUserID, nil, time.Now())
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
	attempts      map[uuid.UUID]int32
	hasFailed     bool
	hasRetryable  bool
	hasNonSucc    bool
	byNode        map[run.NodeID]run.Task
	updateApplied map[uuid.UUID]int
	loadErr       error
}

func newFakeTaskCollection() *fakeTaskCollection {
	return &fakeTaskCollection{
		bulkCancelled: map[uuid.UUID]string{},
		statuses:      map[uuid.UUID]run.TaskStatus{},
		attempts:      map[uuid.UUID]int32{},
		byNode:        map[run.NodeID]run.Task{},
		updateApplied: map[uuid.UUID]int{},
	}
}

func (f *fakeTaskCollection) GetStatus(_ context.Context, id uuid.UUID) (run.TaskStatus, bool, error) {
	s, ok := f.statuses[id]
	return s, ok, nil
}
func (f *fakeTaskCollection) LoadStatusAndAttempt(_ context.Context, id uuid.UUID) (run.TaskStatus, int32, bool, error) {
	if f.loadErr != nil {
		return "", 0, false, f.loadErr
	}
	s, ok := f.statuses[id]
	return s, f.attempts[id], ok, nil
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
func (f *fakeTaskCollection) SetStatusAndAttempt(_ context.Context, id uuid.UUID, s run.TaskStatus, retryCount int32) (int, error) {
	if f.statuses[id] == run.TaskStatusCancelled {
		return 0, nil
	}
	f.statuses[id] = s
	f.attempts[id] = retryCount
	f.updateApplied[id] = 1
	return 1, nil
}
func (f *fakeTaskCollection) BulkCreate(_ context.Context, tasks []run.Task) error {
	f.bulkCreated = append(f.bulkCreated, tasks...)
	for _, t := range tasks {
		f.statuses[t.TaskID] = t.Status
		f.attempts[t.TaskID] = int32(t.RetryCount) //nolint:gosec // G115: test fixture; RetryCount is a small literal set by the test table, never attacker-controlled input.
	}
	return nil
}
func (f *fakeTaskCollection) BulkCancel(_ context.Context, runID uuid.UUID, by string, _ time.Time) (int, error) {
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
	r, _, err := run.NewPendingRun("daily", run.KindCron, nil, identity.SystemUserID, nil, time.Now())
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
	if r.TotalTaskCount() == nil || *r.TotalTaskCount() != 2 {
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

// runningRunWithProjection puts a Run through AcceptDispatch with all
// pending tasks so it is RUNNING with init=completed, total=N, terminal=0.
func runningRunWithProjection(t *testing.T, tc *fakeTaskCollection, taskIDs ...uuid.UUID) *run.Run {
	t.Helper()
	r := freshPendingRun(t)
	projection := make([]run.DispatchedTask, 0, len(taskIDs))
	for i, id := range taskIDs {
		projection = append(projection, run.DispatchedTask{
			TaskID:      id,
			ServiceName: "s", SchemaName: "p", TableName: fmt.Sprintf("t%d", i),
			Status: run.TaskStatusPending, MaxRetries: 3,
		})
	}
	if _, err := r.AcceptDispatch(context.Background(), tc, projection, time.Now()); err != nil {
		t.Fatalf("setup: %v", err)
	}
	return r
}

func TestRecordTaskStatus_FinalizesWhenAllSucceeded(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	id1, id2 := uuid.New(), uuid.New()
	r := runningRunWithProjection(t, tc, id1, id2)

	// First task → SUCCEEDED, not yet finalized.
	events, err := r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusSucceeded, 0, time.Now())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(events) != 0 || r.IsTerminal() {
		t.Fatalf("premature finalize: events=%d terminal=%v", len(events), r.IsTerminal())
	}
	if r.TerminalTaskCount() != 1 {
		t.Fatalf("terminal_task_count: got %d want 1", r.TerminalTaskCount())
	}

	// Second task → SUCCEEDED, finalize.
	tc.hasFailed = false
	tc.hasRetryable = false
	events, err = r.RecordTaskStatus(ctx, tc, id2, run.TaskStatusSucceeded, 0, time.Now())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !r.IsTerminal() || r.Status() != run.SchedulerStatusSucceeded {
		t.Fatalf("expected SUCCEEDED terminal, got %s", r.Status())
	}
	fin, ok := events[0].(run.RunFinalized)
	if !ok || fin.Outcome != run.SchedulerStatusSucceeded {
		t.Fatalf("expected RunFinalized succeeded, got %+v", events[0])
	}
}

func TestRecordTaskStatus_FinalizesAsFailedWhenAnyFailed(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	id1, id2 := uuid.New(), uuid.New()
	r := runningRunWithProjection(t, tc, id1, id2)

	_, _ = r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusFailed, 0, time.Now())
	tc.hasFailed = true
	tc.hasRetryable = false
	events, err := r.RecordTaskStatus(ctx, tc, id2, run.TaskStatusSucceeded, 0, time.Now())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if r.Status() != run.SchedulerStatusFailed {
		t.Fatalf("status: got %s want failed", r.Status())
	}
	fin, ok := events[0].(run.RunFinalized)
	if !ok || fin.Outcome != run.SchedulerStatusFailed {
		t.Fatalf("expected RunFinalized failed, got %+v", events[0])
	}
}

func TestRecordTaskStatus_DefersFinalizeWhenRetryablePending(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	id1, id2 := uuid.New(), uuid.New()
	r := runningRunWithProjection(t, tc, id1, id2)

	_, _ = r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusFailed, 0, time.Now())
	tc.hasFailed = true
	tc.hasRetryable = true // a retry will come
	events, err := r.RecordTaskStatus(ctx, tc, id2, run.TaskStatusFailed, 0, time.Now())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected finalize deferred, got %d events", len(events))
	}
	if r.IsTerminal() {
		t.Fatalf("status: got %s want still running", r.Status())
	}
	if r.TerminalTaskCount() != 2 {
		t.Fatalf("terminal_task_count: got %d want 2", r.TerminalTaskCount())
	}
}

func TestRecordTaskStatus_DecrementsOnRetry(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	id1 := uuid.New()
	r := runningRunWithProjection(t, tc, id1, uuid.New())

	_, _ = r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusFailed, 0, time.Now())
	if r.TerminalTaskCount() != 1 {
		t.Fatalf("pre-retry terminal: got %d want 1", r.TerminalTaskCount())
	}
	// k8s-controller emits RUNNING on retry; FAILED→RUNNING must un-fill.
	tc.statuses[id1] = run.TaskStatusFailed
	_, err := r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusRunning, 1, time.Now())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if r.TerminalTaskCount() != 0 {
		t.Fatalf("post-retry terminal: got %d want 0", r.TerminalTaskCount())
	}
}

// TestRecordTaskStatus_IgnoresStaleRunningAfterSucceeded covers the cross-producer
// reorder where the original attempt's RUNNING (executor) is processed after that
// attempt's SUCCEEDED (k8s-controller). Both carry the same attempt number, so the
// late RUNNING must be a no-op: no un-fill, no status regression, no finalize.
func TestRecordTaskStatus_IgnoresStaleRunningAfterSucceeded(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	id1, id2 := uuid.New(), uuid.New()
	r := runningRunWithProjection(t, tc, id1, id2)

	if _, err := r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusSucceeded, 0, time.Now()); err != nil {
		t.Fatalf("succeeded: %v", err)
	}
	if r.TerminalTaskCount() != 1 {
		t.Fatalf("after SUCCEEDED terminal: got %d want 1", r.TerminalTaskCount())
	}

	// Stale RUNNING for the same attempt (retry_count 0 <= terminal attempt 0).
	events, err := r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusRunning, 0, time.Now())
	if err != nil {
		t.Fatalf("stale running: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("stale RUNNING must emit no events, got %d", len(events))
	}
	if r.TerminalTaskCount() != 1 {
		t.Fatalf("terminal must stay filled: got %d want 1", r.TerminalTaskCount())
	}
	if r.IsTerminal() {
		t.Fatalf("run must not finalize on a stale RUNNING")
	}
	if got := tc.statuses[id1]; got != run.TaskStatusSucceeded {
		t.Fatalf("status must not regress: got %s want succeeded", got)
	}
}

// TestRecordTaskStatus_PropagatesLoadError verifies that a transient failure of
// the FOR-UPDATE status read is surfaced as an error rather than swallowed.
// A swallowed read error used to fall through to the "newer attempt — adopt it"
// branch, which would overwrite the already-recorded terminal with a stale
// delivery and corrupt finalization arithmetic. The aggregate must instead make
// no state change so the consumer redelivers.
func TestRecordTaskStatus_PropagatesLoadError(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	id1, id2 := uuid.New(), uuid.New()
	r := runningRunWithProjection(t, tc, id1, id2)

	// Record a real terminal for id1 first.
	if _, err := r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusSucceeded, 0, time.Now()); err != nil {
		t.Fatalf("succeeded: %v", err)
	}
	if r.TerminalTaskCount() != 1 {
		t.Fatalf("after SUCCEEDED terminal: got %d want 1", r.TerminalTaskCount())
	}

	// Now the FOR-UPDATE read fails transiently while a stale RUNNING for the
	// same attempt arrives.
	tc.loadErr = errors.New("read replica unavailable")
	events, err := r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusRunning, 0, time.Now())
	if err == nil {
		t.Fatalf("expected the load error to propagate, got nil")
	}
	if len(events) != 0 {
		t.Fatalf("no events expected on a propagated read error, got %d", len(events))
	}

	// No state change: the terminal slot stays filled, the recorded status is
	// untouched, and no write was applied for id1 by this call.
	tc.loadErr = nil
	if r.TerminalTaskCount() != 1 {
		t.Fatalf("terminal_task_count must be unchanged: got %d want 1", r.TerminalTaskCount())
	}
	if got := tc.statuses[id1]; got != run.TaskStatusSucceeded {
		t.Fatalf("status must not regress: got %s want succeeded", got)
	}
	if r.IsTerminal() {
		t.Fatalf("run must not have finalized off a failed read")
	}
}

// TestRecordTaskStatus_IgnoresStaleRunningAfterFailed is the FAILED counterpart:
// a RUNNING re-delivered for the attempt that already FAILED (same attempt number)
// must not un-fill the slot.
func TestRecordTaskStatus_IgnoresStaleRunningAfterFailed(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	id1, id2 := uuid.New(), uuid.New()
	r := runningRunWithProjection(t, tc, id1, id2)

	if _, err := r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusFailed, 0, time.Now()); err != nil {
		t.Fatalf("failed: %v", err)
	}
	if r.TerminalTaskCount() != 1 {
		t.Fatalf("after FAILED terminal: got %d want 1", r.TerminalTaskCount())
	}

	events, err := r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusRunning, 0, time.Now())
	if err != nil {
		t.Fatalf("stale running: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("stale RUNNING must emit no events, got %d", len(events))
	}
	if r.TerminalTaskCount() != 1 {
		t.Fatalf("terminal must stay filled: got %d want 1", r.TerminalTaskCount())
	}
	if got := tc.statuses[id1]; got != run.TaskStatusFailed {
		t.Fatalf("status must not regress: got %s want failed", got)
	}
}

// TestRecordTaskStatus_HonorsGenuineRetryRunning asserts a RUNNING whose attempt
// is strictly newer than the recorded terminal (a real k8s retry) un-fills the
// slot, as before.
func TestRecordTaskStatus_HonorsGenuineRetryRunning(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	id1 := uuid.New()
	r := runningRunWithProjection(t, tc, id1, uuid.New())

	// Attempt 0 fails (terminal carries the attempt that ran).
	if _, err := r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusFailed, 0, time.Now()); err != nil {
		t.Fatalf("failed: %v", err)
	}
	if r.TerminalTaskCount() != 1 {
		t.Fatalf("after FAILED terminal: got %d want 1", r.TerminalTaskCount())
	}

	// Genuine retry deploys as attempt 1 → strictly newer → un-fill.
	if _, err := r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusRunning, 1, time.Now()); err != nil {
		t.Fatalf("retry running: %v", err)
	}
	if r.TerminalTaskCount() != 0 {
		t.Fatalf("genuine retry must un-fill: got %d want 0", r.TerminalTaskCount())
	}
	if got := tc.statuses[id1]; got != run.TaskStatusRunning {
		t.Fatalf("status must advance to running: got %s", got)
	}
	if got := tc.attempts[id1]; got != 1 {
		t.Fatalf("stored attempt must advance to 1: got %d", got)
	}
}

// TestRecordTaskStatus_ReorderedDoubleTerminalThenLateRunning is the P1 multi-attempt
// reorder. state processes the original attempt's terminal FAILED(k), then the retry
// attempt's terminal FAILED(k+1), then the retry's delayed RUNNING(k+1). The newer
// terminal must advance the stored attempt without double-filling, so the late
// RUNNING(k+1) is recognized as the already-terminated attempt and ignored — the slot
// stays filled.
func TestRecordTaskStatus_ReorderedDoubleTerminalThenLateRunning(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	id1, id2 := uuid.New(), uuid.New()
	r := runningRunWithProjection(t, tc, id1, id2)

	// Original attempt fails (carries the attempt that ran = 0).
	if _, err := r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusFailed, 0, time.Now()); err != nil {
		t.Fatalf("failed(0): %v", err)
	}
	if r.TerminalTaskCount() != 1 {
		t.Fatalf("after FAILED(0): got %d want 1", r.TerminalTaskCount())
	}

	// Retry attempt's terminal arrives before its RUNNING. Newer terminal:
	// advance attempt to 1, no double-count.
	if _, err := r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusFailed, 1, time.Now()); err != nil {
		t.Fatalf("failed(1): %v", err)
	}
	if r.TerminalTaskCount() != 1 {
		t.Fatalf("newer terminal must not double-count: got %d want 1", r.TerminalTaskCount())
	}
	if got := tc.attempts[id1]; got != 1 {
		t.Fatalf("stored attempt must advance to 1: got %d", got)
	}

	// The retry's delayed RUNNING for the now-terminated attempt — must be ignored.
	if _, err := r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusRunning, 1, time.Now()); err != nil {
		t.Fatalf("late running(1): %v", err)
	}
	if r.TerminalTaskCount() != 1 {
		t.Fatalf("late RUNNING(1) must not un-fill: got %d want 1", r.TerminalTaskCount())
	}
	if got := tc.statuses[id1]; got != run.TaskStatusFailed {
		t.Fatalf("status must stay failed: got %s", got)
	}
}

// TestRecordTaskStatus_IgnoresStaleOlderTerminal covers a terminal from a superseded
// attempt arriving after a newer attempt is already RUNNING: FAILED(k) must not mark
// the task terminal or regress the attempt while RUNNING(k+1) is the current state.
func TestRecordTaskStatus_IgnoresStaleOlderTerminal(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	id1, id2 := uuid.New(), uuid.New()
	r := runningRunWithProjection(t, tc, id1, id2)

	// Original attempt fails, retry starts running (attempt 1).
	if _, err := r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusFailed, 0, time.Now()); err != nil {
		t.Fatalf("failed(0): %v", err)
	}
	if _, err := r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusRunning, 1, time.Now()); err != nil {
		t.Fatalf("running(1): %v", err)
	}
	if r.TerminalTaskCount() != 0 {
		t.Fatalf("after retry running: got %d want 0", r.TerminalTaskCount())
	}

	// A stale terminal from the original attempt arrives late — ignore it.
	if _, err := r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusFailed, 0, time.Now()); err != nil {
		t.Fatalf("stale failed(0): %v", err)
	}
	if r.TerminalTaskCount() != 0 {
		t.Fatalf("stale older terminal must not fill: got %d want 0", r.TerminalTaskCount())
	}
	if got := tc.statuses[id1]; got != run.TaskStatusRunning {
		t.Fatalf("status must stay running: got %s", got)
	}
	if got := tc.attempts[id1]; got != 1 {
		t.Fatalf("attempt must stay 1: got %d", got)
	}
}

// TestRecordTaskStatus_NewerPermanentTerminalFinalizes verifies that when an earlier
// retryable FAILED deferred finalization, the newer attempt's permanent FAILED — which
// does not change terminal_task_count — still re-checks and finalizes the run.
func TestRecordTaskStatus_NewerPermanentTerminalFinalizes(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	id1 := uuid.New()
	r := runningRunWithProjection(t, tc, id1)

	// Attempt 0 fails but is retryable → finalize deferred.
	tc.hasFailed = true
	tc.hasRetryable = true
	events, err := r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusFailed, 0, time.Now())
	if err != nil {
		t.Fatalf("failed(0): %v", err)
	}
	if len(events) != 0 || r.IsTerminal() {
		t.Fatalf("retryable failure must defer finalize: events=%d terminal=%v", len(events), r.IsTerminal())
	}

	// Retry (attempt 1) fails permanently — no more retries. Slot count is
	// unchanged, but the run must now finalize as FAILED.
	tc.hasRetryable = false
	events, err = r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusFailed, 1, time.Now())
	if err != nil {
		t.Fatalf("failed(1): %v", err)
	}
	if !r.IsTerminal() || r.Status() != run.SchedulerStatusFailed {
		t.Fatalf("permanent failure of last attempt must finalize FAILED, got %s", r.Status())
	}
	if len(events) != 1 {
		t.Fatalf("expected one RunFinalized, got %d", len(events))
	}
}

// TestRecordTaskStatus_RetrySucceedsAfterFailReorder covers FAILED(k) then a reordered
// SUCCEEDED(k+1) (retry succeeded): the slot stays filled exactly once and the run
// finalizes SUCCEEDED.
func TestRecordTaskStatus_RetrySucceedsAfterFailReorder(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	id1 := uuid.New()
	r := runningRunWithProjection(t, tc, id1)

	// Attempt 0 fails but is retryable → finalize deferred.
	tc.hasFailed = true
	tc.hasRetryable = true
	if _, err := r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusFailed, 0, time.Now()); err != nil {
		t.Fatalf("failed(0): %v", err)
	}
	if r.TerminalTaskCount() != 1 || r.IsTerminal() {
		t.Fatalf("after retryable FAILED(0): count=%d terminal=%v want 1/false", r.TerminalTaskCount(), r.IsTerminal())
	}

	// Retry succeeds. Newer terminal, same slot — no double-count — and the
	// task is no longer failed, so the run finalizes SUCCEEDED.
	tc.hasFailed = false
	tc.hasRetryable = false
	events, err := r.RecordTaskStatus(ctx, tc, id1, run.TaskStatusSucceeded, 1, time.Now())
	if err != nil {
		t.Fatalf("succeeded(1): %v", err)
	}
	if r.TerminalTaskCount() != 1 {
		t.Fatalf("retry success must not double-count: got %d want 1", r.TerminalTaskCount())
	}
	if !r.IsTerminal() || r.Status() != run.SchedulerStatusSucceeded {
		t.Fatalf("expected SUCCEEDED finalize, got %s", r.Status())
	}
	fin, ok := events[0].(run.RunFinalized)
	if !ok || fin.Outcome != run.SchedulerStatusSucceeded {
		t.Fatalf("expected RunFinalized succeeded, got %+v", events[0])
	}
}

func TestRecordTaskStatus_NoOpWhenSchedulerTerminal(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	r := runningRunWithProjection(t, tc, uuid.New())
	_, _ = r.Cancel(ctx, tc, "tester", "drift", time.Now())

	events, err := r.RecordTaskStatus(ctx, tc, uuid.New(), run.TaskStatusSucceeded, 0, time.Now())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no-op, got %d events", len(events))
	}
}

func TestRecordTaskStatus_TransientWhenTaskRowMissing(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	r := runningRunWithProjection(t, tc, uuid.New())

	_, err := r.RecordTaskStatus(ctx, tc, uuid.New() /* not in tc.statuses */, run.TaskStatusSucceeded, 0, time.Now())
	if err != run.ErrTaskRowNotProjected {
		t.Fatalf("err: got %v want ErrTaskRowNotProjected", err)
	}
}

func TestCanBeRerunSource_AcceptsFailedTerminalWithWork(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	r := runningRunWithProjection(t, tc, uuid.New())
	_, _ = r.MarkDispatchFailed("orchestrator empty", time.Now())
	tc.hasNonSucc = true

	if err := r.CanBeRerunSource(ctx, tc); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestCanBeRerunSource_RejectsNotTerminal(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	r := runningRunWithProjection(t, tc, uuid.New())
	tc.hasNonSucc = true

	if err := r.CanBeRerunSource(ctx, tc); err != run.ErrSourceMustBeTerminallyFailedOrCancelled {
		t.Fatalf("err: got %v want ErrSourceMustBeTerminallyFailedOrCancelled", err)
	}
}

func TestCanBeRerunSource_RejectsWhenAllSucceeded(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	r := runningRunWithProjection(t, tc, uuid.New())
	_, _ = r.MarkDispatchFailed("any reason", time.Now())
	tc.hasNonSucc = false

	if err := r.CanBeRerunSource(ctx, tc); err != run.ErrNothingToRerun {
		t.Fatalf("err: got %v want ErrNothingToRerun", err)
	}
}

func TestHasTaskAt_ReturnsTrueWhenPresent(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	r := freshPendingRun(t)
	node := run.NodeID{ServiceName: "s", SchemaName: "p", TableName: "a"}
	tc.byNode[node] = run.Task{TaskID: uuid.New(), Status: run.TaskStatusSucceeded}

	ok, err := r.HasTaskAt(ctx, tc, node)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok {
		t.Fatalf("expected true")
	}
}

func TestHasTaskAt_ReturnsFalseWhenMissing(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	r := freshPendingRun(t)
	ok, err := r.HasTaskAt(ctx, tc, run.NodeID{ServiceName: "x"})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok {
		t.Fatalf("expected false")
	}
}

// TestComputeJobName_RejectsUnsanitizableIdentity pins the failure condition
// that AcceptDispatch wraps as run.ErrInvalidDispatchedTask: pkg/domain.
// ComputeJobName returns an error when a projected task's identity sanitizes to
// an empty job name. AcceptDispatch calls ComputeJobName directly and wraps any
// error in ErrInvalidDispatchedTask; the application handler maps that sentinel
// to pkg/events.ErrPermanent so the consumer binding ACKs-and-drops the poison
// message instead of NACK-retrying it forever.
//
// Every constructible Run has a non-empty schedule_id whose String() always
// contributes a non-empty suffix, so the empty-name branch can only be reached
// at the ComputeJobName boundary itself; this test exercises that boundary.
func TestComputeJobName_RejectsUnsanitizableIdentity(t *testing.T) {
	if _, err := domain.ComputeJobName("-", "-", "-", ""); err == nil {
		t.Fatal("ComputeJobName must reject an identity that sanitizes to empty")
	}
}

func TestRun_Cancel_FinalizesAndEmitsBothEvents(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	r := runningRunWithProjection(t, tc, uuid.New())
	now := time.Now()

	events, err := r.Cancel(ctx, tc, "tester", "drift", now)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	var sawCancelled, sawFinalized bool
	for _, e := range events {
		switch ev := e.(type) {
		case run.RunCancelled:
			sawCancelled = true
			if ev.By != "tester" || ev.CancellationReason != "drift" {
				t.Fatalf("RunCancelled fields: By=%q CancellationReason=%q, want By=%q CancellationReason=%q",
					ev.By, ev.CancellationReason, "tester", "drift")
			}
		case run.RunFinalized:
			sawFinalized = true
			if ev.Outcome != run.SchedulerStatusCancelled {
				t.Fatalf("RunFinalized.Outcome = %q, want %q", ev.Outcome, run.SchedulerStatusCancelled)
			}
		}
	}
	if !sawCancelled || !sawFinalized {
		t.Fatalf("want both RunCancelled and RunFinalized; cancelled=%v finalized=%v", sawCancelled, sawFinalized)
	}
	if r.CompletedAt() == nil {
		t.Fatal("CompletedAt must be set on cancel (cancel is terminal)")
	}
	if !r.CompletedAt().Equal(now) {
		t.Fatalf("CompletedAt = %v, want %v", *r.CompletedAt(), now)
	}
	if r.CancelledAt() == nil {
		t.Fatal("CancelledAt must remain set on cancel")
	}
}

func TestRun_Cancel_AlreadyTerminal_NoEvents(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	r := runningRunWithProjection(t, tc, uuid.New())
	if _, err := r.Cancel(ctx, tc, "tester", "drift", time.Now()); err != nil {
		t.Fatalf("first Cancel: %v", err)
	}
	events, err := r.Cancel(ctx, tc, "tester", "again", time.Now())
	if err != run.ErrAlreadyTerminal {
		t.Fatalf("second Cancel err = %v, want ErrAlreadyTerminal", err)
	}
	if len(events) != 0 {
		t.Fatalf("want no events on re-cancel, got %d", len(events))
	}
}
