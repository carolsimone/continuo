package run_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
)

func TestNewPendingRun_CreatesAPendingRun(t *testing.T) {
	now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)
	meta := map[string]run.ServiceMetadata{
		"orders": {ImageTag: "abc"},
	}

	r, err := run.NewPendingRun("daily_orders", run.KindCron, nil, identity.SystemUserID, meta, model.OperationTest, run.ReleaseStamp{}, now)
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
	if r.Operation() != model.OperationTest {
		t.Fatalf("operation: got %q want test", r.Operation())
	}
	if got := r.ServiceMetadata(); len(got) != 1 || got["orders"].ImageTag != "abc" {
		t.Fatalf("service metadata not kept: %+v", got)
	}
	if !r.Changes().IsCreated() {
		t.Fatal("a new run must be marked created")
	}
}

func TestNewRun_StampsTheInitiator(t *testing.T) {
	now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)

	pending, err := run.NewPendingRun("daily", run.KindTrigger, nil, "okta|alice", nil, model.OperationRun, run.ReleaseStamp{}, now)
	if err != nil || pending.InitiatedBy() != "okta|alice" {
		t.Fatalf("pending run InitiatedBy = %q, err = %v; want okta|alice", pending.InitiatedBy(), err)
	}
	derived, err := run.NewDerivedRun("daily", run.KindRerun, uuid.New(), run.ReleaseStamp{}, "okta|bob", model.OperationRun, now)
	if err != nil || derived.InitiatedBy() != "okta|bob" {
		t.Fatalf("derived run InitiatedBy = %q, err = %v; want okta|bob", derived.InitiatedBy(), err)
	}
	single, err := run.NewSingleNodeRun("single-node-run-abcd1234", model.OperationTest, nil, "okta|carol", run.ReleaseStamp{}, now)
	if err != nil || single.InitiatedBy() != "okta|carol" || single.Operation() != model.OperationTest {
		t.Fatalf("single-node run InitiatedBy = %q, Operation = %q, err = %v; want okta|carol, test", single.InitiatedBy(), single.Operation(), err)
	}
}

func TestNewPendingRun_RejectsEmptyName(t *testing.T) {
	_, err := run.NewPendingRun("", run.KindCron, nil, identity.SystemUserID, nil, model.OperationRun, run.ReleaseStamp{}, time.Now())
	if err != run.ErrScheduleNameRequired {
		t.Fatalf("err: got %v want %v", err, run.ErrScheduleNameRequired)
	}
}

func TestNewPendingRun_RejectsInvalidKind(t *testing.T) {
	_, err := run.NewPendingRun("x", run.Kind("bogus"), nil, identity.SystemUserID, nil, model.OperationRun, run.ReleaseStamp{}, time.Now())
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
	maxRetries    map[uuid.UUID]int32
	keys          map[uuid.UUID]topology.NodeKey
	// dispatched holds the tasks stamped dispatched, by BulkCreate or
	// MarkDispatched; skipped the tasks MarkSkipped changed, in call order.
	dispatched []uuid.UUID
	skipped    []uuid.UUID
	// derived makes HasFailed and HasRetryableFailed read the stored
	// statuses, attempts and budgets instead of hasFailed and hasRetryable.
	derived bool
}

func newFakeTaskCollection() *fakeTaskCollection {
	return &fakeTaskCollection{
		bulkCancelled: map[uuid.UUID]string{},
		statuses:      map[uuid.UUID]run.TaskStatus{},
		attempts:      map[uuid.UUID]int32{},
		byNode:        map[run.NodeID]run.Task{},
		updateApplied: map[uuid.UUID]int{},
		maxRetries:    map[uuid.UUID]int32{},
		keys:          map[uuid.UUID]topology.NodeKey{},
	}
}

func (f *fakeTaskCollection) GetStatus(_ context.Context, id uuid.UUID) (run.TaskStatus, bool, error) {
	s, ok := f.statuses[id]
	return s, ok, nil
}
func (f *fakeTaskCollection) LoadAttemptState(_ context.Context, id uuid.UUID) (run.TaskAttemptState, bool, error) {
	if f.loadErr != nil {
		return run.TaskAttemptState{}, false, f.loadErr
	}
	s, ok := f.statuses[id]
	return run.TaskAttemptState{Status: s, RetryCount: f.attempts[id], MaxRetries: f.maxRetries[id], NodeKey: f.keys[id]}, ok, nil
}
func (f *fakeTaskCollection) Exists(_ context.Context, id uuid.UUID) (bool, error) {
	_, ok := f.statuses[id]
	return ok, nil
}
func (f *fakeTaskCollection) HasFailed(_ context.Context, _ uuid.UUID) (bool, error) {
	if !f.derived {
		return f.hasFailed, nil
	}
	for _, s := range f.statuses {
		if s == run.TaskStatusFailed || s == run.TaskStatusCancelled {
			return true, nil
		}
	}
	return false, nil
}
func (f *fakeTaskCollection) HasRetryableFailed(_ context.Context, _ uuid.UUID) (bool, error) {
	if !f.derived {
		return f.hasRetryable, nil
	}
	for id, s := range f.statuses {
		if s == run.TaskStatusFailed && f.attempts[id] < f.maxRetries[id] {
			return true, nil
		}
	}
	return false, nil
}
func (f *fakeTaskCollection) HasNonSucceeded(_ context.Context, _ uuid.UUID) (bool, error) {
	return f.hasNonSucc, nil
}
func (f *fakeTaskCollection) GetByID(_ context.Context, id uuid.UUID) (run.Task, error) {
	for _, t := range f.byNode {
		if t.TaskID == id {
			return t, nil
		}
	}
	return run.Task{}, run.ErrTaskNotFound
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
		f.attempts[t.TaskID] = int32(t.RetryCount)   //nolint:gosec // G115: test fixture; RetryCount is a small literal set by the test table, never attacker-controlled input.
		f.maxRetries[t.TaskID] = int32(t.MaxRetries) //nolint:gosec // G115: test fixture; MaxRetries is a small literal set by the test, never attacker-controlled input.
		f.keys[t.TaskID] = t.NodeKey
		if t.DispatchedAt != nil {
			f.dispatched = append(f.dispatched, t.TaskID)
		}
	}
	return nil
}
func (f *fakeTaskCollection) MarkDispatched(_ context.Context, ids []uuid.UUID, _ time.Time) ([]uuid.UUID, error) {
	var stamped []uuid.UUID
	for _, id := range ids {
		if f.statuses[id] == run.TaskStatusPending && !slices.Contains(f.dispatched, id) {
			f.dispatched = append(f.dispatched, id)
			stamped = append(stamped, id)
		}
	}
	return stamped, nil
}
func (f *fakeTaskCollection) ListRunTasks(_ context.Context, _ uuid.UUID) ([]run.Task, error) {
	return f.bulkCreated, nil
}
func (f *fakeTaskCollection) MarkSkipped(_ context.Context, ids []uuid.UUID) (int, error) {
	n := 0
	for _, id := range ids {
		if f.statuses[id] == run.TaskStatusPending && !slices.Contains(f.dispatched, id) {
			f.statuses[id] = run.TaskStatusSkipped
			f.skipped = append(f.skipped, id)
			n++
		}
	}
	return n, nil
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
// zero counters — the state on entry to Start.
func freshPendingRun(t *testing.T) *run.Run {
	t.Helper()
	r, err := run.NewPendingRun("daily", run.KindCron, nil, identity.SystemUserID, nil, model.OperationRun, run.ReleaseStamp{}, time.Now())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	return r
}

// runningRunWithProjection returns a RUNNING run whose tasks state did not
// plan, holding one pending task per id (stored in tc) with init=completed,
// total=N and terminal=0.
func runningRunWithProjection(t *testing.T, tc *fakeTaskCollection, taskIDs ...uuid.UUID) *run.Run {
	t.Helper()
	return dispatchedRun(t, tc, time.Now(), taskIDs...)
}

func TestRecordTaskStatus_FinalizesWhenAllSucceeded(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	id1, id2 := uuid.New(), uuid.New()
	r := runningRunWithProjection(t, tc, id1, id2)

	// First task → SUCCEEDED, not yet finalized.
	events, applied, err := r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusSucceeded, 0, time.Now())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(events) != 0 || r.IsTerminal() {
		t.Fatalf("premature finalize: events=%d terminal=%v", len(events), r.IsTerminal())
	}
	if !applied {
		t.Fatal("an accepted success that leaves the run open must report it was applied")
	}
	if r.TerminalTaskCount() != 1 {
		t.Fatalf("terminal_task_count: got %d want 1", r.TerminalTaskCount())
	}

	// Second task → SUCCEEDED, finalize.
	tc.hasFailed = false
	tc.hasRetryable = false
	events, applied, err = r.RecordTaskStatus(ctx, tc, nil, id2, run.TaskStatusSucceeded, 0, time.Now())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !r.IsTerminal() || r.Status() != run.SchedulerStatusSucceeded {
		t.Fatalf("expected SUCCEEDED terminal, got %s", r.Status())
	}
	if !applied {
		t.Fatal("a success that finalizes the run must report it was applied")
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

	_, _, _ = r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusFailed, 0, time.Now())
	tc.hasFailed = true
	tc.hasRetryable = false
	events, _, err := r.RecordTaskStatus(ctx, tc, nil, id2, run.TaskStatusSucceeded, 0, time.Now())
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

	_, _, _ = r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusFailed, 0, time.Now())
	tc.hasFailed = true
	tc.hasRetryable = true // a retry will come
	events, _, err := r.RecordTaskStatus(ctx, tc, nil, id2, run.TaskStatusFailed, 0, time.Now())
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

	_, _, _ = r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusFailed, 0, time.Now())
	if r.TerminalTaskCount() != 1 {
		t.Fatalf("pre-retry terminal: got %d want 1", r.TerminalTaskCount())
	}
	// execution-controller emits RUNNING on retry; FAILED→RUNNING must un-fill.
	tc.statuses[id1] = run.TaskStatusFailed
	_, _, err := r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusRunning, 1, time.Now())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if r.TerminalTaskCount() != 0 {
		t.Fatalf("post-retry terminal: got %d want 0", r.TerminalTaskCount())
	}
}

// TestRecordTaskStatus_IgnoresStaleRunningAfterSucceeded covers the redelivery
// case where the original attempt's RUNNING is processed after that attempt's
// SUCCEEDED (both written by execution-controller's job-status handler). Both
// carry the same attempt number, so the late RUNNING must be a no-op: no
// un-fill, no status regression, no finalize.
func TestRecordTaskStatus_IgnoresStaleRunningAfterSucceeded(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	id1, id2 := uuid.New(), uuid.New()
	r := runningRunWithProjection(t, tc, id1, id2)

	if _, _, err := r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusSucceeded, 0, time.Now()); err != nil {
		t.Fatalf("succeeded: %v", err)
	}
	if r.TerminalTaskCount() != 1 {
		t.Fatalf("after SUCCEEDED terminal: got %d want 1", r.TerminalTaskCount())
	}

	// Stale RUNNING for the same attempt (retry_count 0 <= terminal attempt 0).
	events, _, err := r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusRunning, 0, time.Now())
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
	if _, _, err := r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusSucceeded, 0, time.Now()); err != nil {
		t.Fatalf("succeeded: %v", err)
	}
	if r.TerminalTaskCount() != 1 {
		t.Fatalf("after SUCCEEDED terminal: got %d want 1", r.TerminalTaskCount())
	}

	// Now the FOR-UPDATE read fails transiently while a stale RUNNING for the
	// same attempt arrives.
	tc.loadErr = errors.New("read replica unavailable")
	events, _, err := r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusRunning, 0, time.Now())
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

	if _, _, err := r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusFailed, 0, time.Now()); err != nil {
		t.Fatalf("failed: %v", err)
	}
	if r.TerminalTaskCount() != 1 {
		t.Fatalf("after FAILED terminal: got %d want 1", r.TerminalTaskCount())
	}

	events, _, err := r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusRunning, 0, time.Now())
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
	if _, _, err := r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusFailed, 0, time.Now()); err != nil {
		t.Fatalf("failed: %v", err)
	}
	if r.TerminalTaskCount() != 1 {
		t.Fatalf("after FAILED terminal: got %d want 1", r.TerminalTaskCount())
	}

	// Genuine retry deploys as attempt 1 → strictly newer → un-fill.
	if _, _, err := r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusRunning, 1, time.Now()); err != nil {
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
	if _, _, err := r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusFailed, 0, time.Now()); err != nil {
		t.Fatalf("failed(0): %v", err)
	}
	if r.TerminalTaskCount() != 1 {
		t.Fatalf("after FAILED(0): got %d want 1", r.TerminalTaskCount())
	}

	// Retry attempt's terminal arrives before its RUNNING. Newer terminal:
	// advance attempt to 1, no double-count.
	if _, _, err := r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusFailed, 1, time.Now()); err != nil {
		t.Fatalf("failed(1): %v", err)
	}
	if r.TerminalTaskCount() != 1 {
		t.Fatalf("newer terminal must not double-count: got %d want 1", r.TerminalTaskCount())
	}
	if got := tc.attempts[id1]; got != 1 {
		t.Fatalf("stored attempt must advance to 1: got %d", got)
	}

	// The retry's delayed RUNNING for the now-terminated attempt — must be ignored.
	if _, _, err := r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusRunning, 1, time.Now()); err != nil {
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
	if _, _, err := r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusFailed, 0, time.Now()); err != nil {
		t.Fatalf("failed(0): %v", err)
	}
	if _, _, err := r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusRunning, 1, time.Now()); err != nil {
		t.Fatalf("running(1): %v", err)
	}
	if r.TerminalTaskCount() != 0 {
		t.Fatalf("after retry running: got %d want 0", r.TerminalTaskCount())
	}

	// A stale terminal from the original attempt arrives late — ignore it.
	if _, _, err := r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusFailed, 0, time.Now()); err != nil {
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
	events, _, err := r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusFailed, 0, time.Now())
	if err != nil {
		t.Fatalf("failed(0): %v", err)
	}
	if len(events) != 0 || r.IsTerminal() {
		t.Fatalf("retryable failure must defer finalize: events=%d terminal=%v", len(events), r.IsTerminal())
	}

	// Retry (attempt 1) fails permanently — no more retries. Slot count is
	// unchanged, but the run must now finalize as FAILED.
	tc.hasRetryable = false
	events, _, err = r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusFailed, 1, time.Now())
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
	if _, _, err := r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusFailed, 0, time.Now()); err != nil {
		t.Fatalf("failed(0): %v", err)
	}
	if r.TerminalTaskCount() != 1 || r.IsTerminal() {
		t.Fatalf("after retryable FAILED(0): count=%d terminal=%v want 1/false", r.TerminalTaskCount(), r.IsTerminal())
	}

	// Retry succeeds. Newer terminal, same slot — no double-count — and the
	// task is no longer failed, so the run finalizes SUCCEEDED.
	tc.hasFailed = false
	tc.hasRetryable = false
	events, _, err := r.RecordTaskStatus(ctx, tc, nil, id1, run.TaskStatusSucceeded, 1, time.Now())
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

	events, applied, err := r.RecordTaskStatus(ctx, tc, nil, uuid.New(), run.TaskStatusSucceeded, 0, time.Now())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no-op, got %d events", len(events))
	}
	if applied {
		t.Fatal("a success delivered to a terminal run must not report it was applied")
	}
}

func TestRecordTaskStatus_TransientWhenTaskRowMissing(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	r := runningRunWithProjection(t, tc, uuid.New())

	_, _, err := r.RecordTaskStatus(ctx, tc, nil, uuid.New() /* not in tc.statuses */, run.TaskStatusSucceeded, 0, time.Now())
	if err != run.ErrTaskRowNotProjected {
		t.Fatalf("err: got %v want ErrTaskRowNotProjected", err)
	}
}

func TestMarkDispatchTerminal_BenignFinalizesSkipped(t *testing.T) {
	tc := newFakeTaskCollection()
	r := runningRunWithProjection(t, tc, uuid.New())
	evts, err := r.MarkDispatchTerminal(true, "no_tests", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if r.Status() != run.SchedulerStatusSkipped {
		t.Fatalf("status=%q, want skipped", r.Status())
	}
	fin, ok := evts[0].(run.RunFinalized)
	if !ok || fin.Outcome != run.SchedulerStatusSkipped {
		t.Fatalf("expected RunFinalized{Outcome=skipped}, got %+v", evts[0])
	}
}

func TestMarkDispatchTerminal_NonBenignFinalizesFailed(t *testing.T) {
	tc := newFakeTaskCollection()
	r := runningRunWithProjection(t, tc, uuid.New())
	evts, err := r.MarkDispatchTerminal(false, "empty_projection", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if r.Status() != run.SchedulerStatusFailed {
		t.Fatalf("status=%q, want failed", r.Status())
	}
	fin, ok := evts[0].(run.RunFinalized)
	if !ok || fin.Outcome != run.SchedulerStatusFailed {
		t.Fatalf("expected RunFinalized{Outcome=failed}, got %+v", evts[0])
	}
}

func TestCanBeRerunSource_AcceptsFailedTerminalWithWork(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	r := runningRunWithProjection(t, tc, uuid.New())
	_, _ = r.MarkDispatchTerminal(false, "orchestrator empty", time.Now())
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
	_, _ = r.MarkDispatchTerminal(false, "any reason", time.Now())
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
// Start refuses a plan for: pkg/domain.ComputeJobName returns an error when a
// task's identity and run-id suffix both sanitize to nothing.
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

// dispatchedRun returns a RUNNING run with one pending task per id, dispatched
// at dispatchedAt, with a clean change set.
func dispatchedRun(t *testing.T, tc *fakeTaskCollection, dispatchedAt time.Time, ids ...uuid.UUID) *run.Run {
	t.Helper()
	total := int32(len(ids)) //nolint:gosec // G115: test fixture; a handful of ids.
	r := run.HydrateRun(uuid.New(), "daily", run.SchedulerStatusRunning, run.InitStatusCompleted, run.KindCron, nil,
		identity.SystemUserID, model.OperationRun, dispatchedAt, &dispatchedAt, nil, &dispatchedAt, nil, nil, nil,
		&total, 0, map[string]run.ServiceMetadata{})
	tasks := make([]run.Task, 0, len(ids))
	for i, id := range ids {
		tasks = append(tasks, run.Task{
			TaskID: id, ScheduleID: r.ScheduleID(), ServiceName: "s", SchemaName: "p", TableName: fmt.Sprintf("t%d", i),
			Status: run.TaskStatusPending, MaxRetries: 3,
		})
	}
	if err := tc.BulkCreate(context.Background(), tasks); err != nil {
		t.Fatalf("setup: %v", err)
	}
	return r
}

func TestNewPendingRun_LeavesProgressClockUnset(t *testing.T) {
	r := freshPendingRun(t)
	if r.LastHeartbeatAt() != nil {
		t.Fatalf("LastHeartbeatAt = %v, want nil before the run is dispatched", *r.LastHeartbeatAt())
	}
	if r.Changes().IsHeartbeatDirty() {
		t.Fatal("a new run must not mark the progress clock dirty")
	}
}

// TestRecordTaskStatus_ReportsOnlyAnAppliedSuccess pins the boolean result: it
// is true exactly when the call persisted a terminal SUCCEEDED, whether or not
// the run finalized, and false for every ignored delivery and every other
// status.
func TestRecordTaskStatus_ReportsOnlyAnAppliedSuccess(t *testing.T) {
	cases := []struct {
		name    string
		prepare func(t *testing.T, r *run.Run, tc *fakeTaskCollection, id, other uuid.UUID)
		status  run.TaskStatus
		attempt int32
		want    bool
	}{
		{name: "a success that leaves the run open", status: run.TaskStatusSucceeded, want: true},
		{
			name: "a success that finalizes the run",
			prepare: func(t *testing.T, r *run.Run, tc *fakeTaskCollection, _, other uuid.UUID) {
				if _, applied, err := r.RecordTaskStatus(context.Background(), tc, nil, other, run.TaskStatusSucceeded, 0, time.Now()); err != nil || !applied {
					t.Fatalf("the other task: applied=%v err=%v", applied, err)
				}
			},
			status: run.TaskStatusSucceeded, want: true,
		},
		{
			name: "a retry that succeeds after a failed attempt",
			prepare: func(_ *testing.T, _ *run.Run, tc *fakeTaskCollection, id, _ uuid.UUID) {
				tc.statuses[id] = run.TaskStatusFailed
			},
			status: run.TaskStatusSucceeded, attempt: 1, want: true,
		},
		{
			name: "a success delivered to a run that is already terminal",
			prepare: func(t *testing.T, r *run.Run, tc *fakeTaskCollection, _, _ uuid.UUID) {
				if _, err := r.Cancel(context.Background(), tc, "tester", "drift", time.Now()); err != nil {
					t.Fatalf("cancel: %v", err)
				}
			},
			status: run.TaskStatusSucceeded,
		},
		{
			name: "a replay of a success the task already recorded",
			prepare: func(t *testing.T, r *run.Run, tc *fakeTaskCollection, id, _ uuid.UUID) {
				if _, applied, err := r.RecordTaskStatus(context.Background(), tc, nil, id, run.TaskStatusSucceeded, 0, time.Now()); err != nil || !applied {
					t.Fatalf("first delivery: applied=%v err=%v", applied, err)
				}
			},
			status: run.TaskStatusSucceeded,
		},
		{
			name: "a success of a superseded attempt",
			prepare: func(_ *testing.T, _ *run.Run, tc *fakeTaskCollection, id, _ uuid.UUID) {
				tc.attempts[id] = 2
			},
			status: run.TaskStatusSucceeded, attempt: 1,
		},
		{
			name: "a success the task row refuses",
			prepare: func(_ *testing.T, _ *run.Run, tc *fakeTaskCollection, id, _ uuid.UUID) {
				tc.statuses[id] = run.TaskStatusCancelled
			},
			status: run.TaskStatusSucceeded, attempt: 1,
		},
		{name: "a failure", status: run.TaskStatusFailed},
		{name: "a skip", status: run.TaskStatusSkipped},
		{name: "a task that starts running", status: run.TaskStatusRunning},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tc := newFakeTaskCollection()
			id, other := uuid.New(), uuid.New()
			r := runningRunWithProjection(t, tc, id, other)
			if c.prepare != nil {
				c.prepare(t, r, tc, id, other)
			}

			_, applied, err := r.RecordTaskStatus(context.Background(), tc, nil, id, c.status, c.attempt, time.Now())
			if err != nil {
				t.Fatalf("RecordTaskStatus: %v", err)
			}
			if applied != c.want {
				t.Fatalf("succeededApplied = %v, want %v", applied, c.want)
			}
		})
	}
}

func TestRecordTaskStatus_StampsProgressWhenTheWriteApplies(t *testing.T) {
	cases := []struct {
		name    string
		prepare func(tc *fakeTaskCollection, id uuid.UUID)
		status  run.TaskStatus
		attempt int32
	}{
		{name: "same attempt pending to running", status: run.TaskStatusRunning},
		{name: "first terminal of the attempt", status: run.TaskStatusSucceeded},
		{name: "cascade skip", status: run.TaskStatusSkipped},
		{
			name: "newer attempt adopted",
			prepare: func(tc *fakeTaskCollection, id uuid.UUID) {
				tc.statuses[id] = run.TaskStatusFailed
			},
			status:  run.TaskStatusRunning,
			attempt: 1,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tc := newFakeTaskCollection()
			id := uuid.New()
			dispatchedAt := time.Now()
			r := dispatchedRun(t, tc, dispatchedAt, id, uuid.New())
			if c.prepare != nil {
				c.prepare(tc, id)
			}
			later := dispatchedAt.Add(time.Hour)

			if _, _, err := r.RecordTaskStatus(context.Background(), tc, nil, id, c.status, c.attempt, later); err != nil {
				t.Fatalf("RecordTaskStatus: %v", err)
			}
			if r.LastHeartbeatAt() == nil || !r.LastHeartbeatAt().Equal(later) {
				t.Fatalf("LastHeartbeatAt = %v, want %v", r.LastHeartbeatAt(), later)
			}
			if !r.Changes().IsHeartbeatDirty() {
				t.Fatal("an applied task write must mark the progress clock dirty")
			}
		})
	}
}

func TestRecordTaskStatus_NoProgressWithoutAnAppliedWrite(t *testing.T) {
	cases := []struct {
		name    string
		prepare func(t *testing.T, r *run.Run, tc *fakeTaskCollection, id uuid.UUID, at time.Time)
		status  run.TaskStatus
		attempt int32
	}{
		{
			name: "replayed terminal",
			prepare: func(t *testing.T, r *run.Run, tc *fakeTaskCollection, id uuid.UUID, at time.Time) {
				if _, _, err := r.RecordTaskStatus(context.Background(), tc, nil, id, run.TaskStatusSucceeded, 0, at); err != nil {
					t.Fatalf("first terminal: %v", err)
				}
			},
			status: run.TaskStatusSucceeded,
		},
		{name: "unchanged status", status: run.TaskStatusPending},
		{
			name: "stale older attempt",
			prepare: func(_ *testing.T, _ *run.Run, tc *fakeTaskCollection, id uuid.UUID, _ time.Time) {
				tc.attempts[id] = 2
			},
			status:  run.TaskStatusRunning,
			attempt: 1,
		},
		{
			name: "cancelled task row",
			prepare: func(_ *testing.T, _ *run.Run, tc *fakeTaskCollection, id uuid.UUID, _ time.Time) {
				tc.statuses[id] = run.TaskStatusCancelled
			},
			status:  run.TaskStatusRunning,
			attempt: 1,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tc := newFakeTaskCollection()
			id := uuid.New()
			dispatchedAt := time.Now()
			r := dispatchedRun(t, tc, dispatchedAt, id, uuid.New())
			if c.prepare != nil {
				c.prepare(t, r, tc, id, dispatchedAt.Add(time.Minute))
			}
			r.ResetChanges()
			before := *r.LastHeartbeatAt()

			if _, _, err := r.RecordTaskStatus(context.Background(), tc, nil, id, c.status, c.attempt, dispatchedAt.Add(time.Hour)); err != nil {
				t.Fatalf("RecordTaskStatus: %v", err)
			}
			if r.Changes().IsHeartbeatDirty() {
				t.Fatal("a write that applied nothing must not mark the progress clock dirty")
			}
			if !r.LastHeartbeatAt().Equal(before) {
				t.Fatalf("LastHeartbeatAt = %v, want it unchanged at %v", *r.LastHeartbeatAt(), before)
			}
		})
	}
}

func TestRun_Cancel_SkippedRun_ErrAlreadyTerminal(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	r := runningRunWithProjection(t, tc, uuid.New())
	if _, err := r.MarkDispatchTerminal(true, "no_tests", time.Now()); err != nil {
		t.Fatalf("MarkDispatchTerminal: %v", err)
	}
	events, err := r.Cancel(ctx, tc, "tester", "late", time.Now())
	if err != run.ErrAlreadyTerminal {
		t.Fatalf("Cancel on a skipped run: err = %v, want ErrAlreadyTerminal", err)
	}
	if len(events) != 0 || len(tc.bulkCancelled) != 0 {
		t.Fatalf("want no events and no task cancels; events=%d bulkCancelled=%d", len(events), len(tc.bulkCancelled))
	}
	if r.Status() != run.SchedulerStatusSkipped {
		t.Fatalf("status = %q, want skipped", r.Status())
	}
}

// A run holding a cancelled task finalizes failed once the rest succeed.
func TestRecordTaskStatus_ACancelledTaskFinalizesTheRunFailed(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	pending, cancelled := uuid.New(), uuid.New()
	r := dispatchedRun(t, tc, time.Now(), pending, cancelled)
	if _, _, err := r.RecordTaskStatus(ctx, tc, nil, cancelled, run.TaskStatusCancelled, 0, time.Now()); err != nil {
		t.Fatalf("RecordTaskStatus(cancelled): %v", err)
	}

	// TaskCollection.HasFailed answers true for a run holding a cancelled task.
	tc.hasFailed = true
	events, _, err := r.RecordTaskStatus(ctx, tc, nil, pending, run.TaskStatusSucceeded, 0, time.Now())
	if err != nil {
		t.Fatalf("RecordTaskStatus: %v", err)
	}
	if r.Status() != run.SchedulerStatusFailed {
		t.Fatalf("status = %q, want failed", r.Status())
	}
	if fin, ok := events[0].(run.RunFinalized); !ok || fin.Outcome != run.SchedulerStatusFailed {
		t.Fatalf("events[0] = %+v, want RunFinalized{Outcome: failed}", events[0])
	}
}

// A promote-seed run carries the id it is given and the release it builds.
func TestNewPromotedSeedsRun_CarriesTheRelease(t *testing.T) {
	id := uuid.New()
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

	r, err := run.NewPromotedSeedsRun(id, "promote-seed-abc12345", run.ReleaseStamp{ReleaseID: "rel-1", PromotionSeq: 7}, now)
	if err != nil {
		t.Fatalf("NewPromotedSeedsRun: %v", err)
	}
	if r.ScheduleID() != id || r.Release() != (run.ReleaseStamp{ReleaseID: "rel-1", PromotionSeq: 7}) {
		t.Fatalf("id/release = %s/%+v, want %s/rel-1 7", r.ScheduleID(), r.Release(), id)
	}
	if r.Kind() != run.KindPromoteSeed || r.Status() != run.SchedulerStatusRunning {
		t.Fatalf("kind/status = %q/%q, want promote_seed/running", r.Kind(), r.Status())
	}
}

// A run rebuilt from its row gets its release back only through
// HydrateReleaseID; a run that names none reports "".
func TestHydrateReleaseID_RestoresTheStoredRelease(t *testing.T) {
	r := run.HydrateRun(uuid.New(), "promote-seed-abc12345", run.SchedulerStatusRunning, run.InitStatusCompleted,
		run.KindPromoteSeed, nil, identity.SystemUserID, model.OperationRun, time.Now(),
		nil, nil, nil, nil, nil, nil, nil, 0, map[string]run.ServiceMetadata{})
	if r.ReleaseID() != "" {
		t.Fatalf("ReleaseID() = %q before hydration, want empty", r.ReleaseID())
	}
	r.HydrateRelease(run.ReleaseStamp{ReleaseID: "rel-9"})
	if r.ReleaseID() != "rel-9" {
		t.Fatalf("ReleaseID() = %q, want rel-9", r.ReleaseID())
	}
	if r.Changes().IsCreated() || r.Changes().IsStatusDirty() {
		t.Fatal("hydrating the release id must not mark the run dirty")
	}
}

// A rerun replays its source's tasks at the source's images, so the caller
// gives it the source's release stamp and the run records it as is.
func TestNewDerivedRun_RerunCarriesItsSourceRelease(t *testing.T) {
	rerun, err := run.NewDerivedRun("promote-seed-abc12345", run.KindRerun, uuid.New(), run.ReleaseStamp{ReleaseID: "rel-1", PromotionSeq: 3}, "okta|alice", model.OperationRun, time.Now())
	if err != nil {
		t.Fatalf("NewDerivedRun(rerun): %v", err)
	}
	if rerun.ReleaseID() != "rel-1" || rerun.Release().PromotionSeq != 3 {
		t.Fatalf("rerun Release() = %+v, want rel-1 / 3", rerun.Release())
	}
}
