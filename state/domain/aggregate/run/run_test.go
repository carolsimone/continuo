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

// taskDefaults are the task settings the aggregate tests plan with: the
// chart's defaults.
var taskDefaults = run.TaskDefaults{MaxRetries: 2, Timeout: 24 * time.Hour}

// fakeTaskCollection is the in-memory test double used by every aggregate
// unit test in this file. Tests set fields per scenario.
type fakeTaskCollection struct {
	bulkCreated   []run.Task
	bulkCancelled map[uuid.UUID]string
	statuses      map[uuid.UUID]run.TaskStatus
	attempts      map[uuid.UUID]int32
	hasFailed     bool
	hasNonSucc    bool
	byNode        map[run.NodeID]run.Task
	updateApplied map[uuid.UUID]int
	loadErr       error
	maxRetries    map[uuid.UUID]int32
	keys          map[uuid.UUID]topology.NodeKey
	// attemptRows holds the stored attempts; cancelledRuns the runs
	// CancelOpenAttempts was called for; lockOrder the LoadAttemptState
	// ("task <id>") and LoadAttempt ("attempt <id>/<n>") calls in call order.
	attemptRows   map[attemptKey]run.Attempt
	cancelledRuns []uuid.UUID
	lockOrder     []string
	// saves holds every attempt SaveAttempt was given, in call order; added
	// every attempt AddAttempts inserted.
	saves []run.Attempt
	added []run.Attempt
	// created holds each task BulkCreate stored, with the dispatch time
	// BulkCreate or MarkDispatched stamped on it.
	created map[uuid.UUID]run.Task
	// dispatched holds the tasks stamped dispatched, by BulkCreate or
	// MarkDispatched; skipped the tasks MarkSkipped changed, in call order.
	dispatched []uuid.UUID
	skipped    []uuid.UUID
	// derived makes HasFailed read the stored statuses instead of
	// hasFailed.
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
		attemptRows:   map[attemptKey]run.Attempt{},
		created:       map[uuid.UUID]run.Task{},
	}
}

// attemptKey identifies one stored attempt.
type attemptKey struct {
	task    uuid.UUID
	attempt int32
}

func (f *fakeTaskCollection) LoadAttempt(_ context.Context, id uuid.UUID, attempt int32) (run.Attempt, bool, error) {
	f.lockOrder = append(f.lockOrder, fmt.Sprintf("attempt %s/%d", id, attempt))
	a, ok := f.attemptRows[attemptKey{id, attempt}]
	return a, ok, nil
}
func (f *fakeTaskCollection) SaveAttempt(_ context.Context, a run.Attempt) error {
	f.attemptRows[attemptKey{a.TaskID, a.Number}] = a
	f.saves = append(f.saves, a)
	return nil
}

// AddAttempts stores the attempts as the real adapter inserts them; a second
// attempt of one number is an error, as the primary key makes it.
func (f *fakeTaskCollection) AddAttempts(_ context.Context, attempts []run.Attempt) error {
	for _, a := range attempts {
		k := attemptKey{a.TaskID, a.Number}
		if _, ok := f.attemptRows[k]; ok {
			return fmt.Errorf("attempt %d of task %s already stored", a.Number, a.TaskID)
		}
		f.attemptRows[k] = a
		f.added = append(f.added, a)
	}
	return nil
}

func (f *fakeTaskCollection) HasInFlight(_ context.Context, _ uuid.UUID) (bool, error) {
	for _, s := range f.statuses {
		if s == run.TaskStatusDispatched || s == run.TaskStatusQueued || s == run.TaskStatusRunning {
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeTaskCollection) SkipPending(_ context.Context, _ uuid.UUID) (int, error) {
	n := 0
	for id, s := range f.statuses {
		if s == run.TaskStatusPending && !slices.Contains(f.dispatched, id) {
			f.statuses[id] = run.TaskStatusSkipped
			f.skipped = append(f.skipped, id)
			n++
		}
	}
	return n, nil
}

// CancelOpenAttempts cancels every open attempt the fake holds: a test builds
// one run, so every stored attempt belongs to it.
func (f *fakeTaskCollection) CancelOpenAttempts(_ context.Context, runID uuid.UUID) (int, error) {
	f.cancelledRuns = append(f.cancelledRuns, runID)
	n := 0
	for k, a := range f.attemptRows {
		if a.Status.IsFinal() {
			continue
		}
		a.Status = run.AttemptCancelled
		f.attemptRows[k] = a
		n++
	}
	return n, nil
}

func (f *fakeTaskCollection) GetStatus(_ context.Context, id uuid.UUID) (run.TaskStatus, bool, error) {
	s, ok := f.statuses[id]
	return s, ok, nil
}
func (f *fakeTaskCollection) LoadAttemptState(_ context.Context, id uuid.UUID) (run.TaskAttemptState, bool, error) {
	f.lockOrder = append(f.lockOrder, "task "+id.String())
	if f.loadErr != nil {
		return run.TaskAttemptState{}, false, f.loadErr
	}
	s, ok := f.statuses[id]
	c := f.created[id]
	return run.TaskAttemptState{
		Status: s, Attempt: f.attempts[id], MaxRetries: f.maxRetries[id], NodeKey: f.keys[id],
		JobName: c.JobName, ServiceName: c.ServiceName, SchemaName: c.SchemaName, TableName: c.TableName,
		DispatchedAt: c.DispatchedAt,
	}, ok, nil
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
func (f *fakeTaskCollection) SetStatusAndAttempt(_ context.Context, id uuid.UUID, s run.TaskStatus, attempt int32) (int, error) {
	if f.statuses[id] == run.TaskStatusCancelled {
		return 0, nil
	}
	f.statuses[id] = s
	f.attempts[id] = attempt
	f.updateApplied[id] = 1
	return 1, nil
}
func (f *fakeTaskCollection) BulkCreate(_ context.Context, tasks []run.Task) error {
	f.bulkCreated = append(f.bulkCreated, tasks...)
	for _, t := range tasks {
		f.statuses[t.TaskID] = t.Status
		f.attempts[t.TaskID] = int32(t.Attempt)      //nolint:gosec // G115: test fixture; Attempt is a small literal set by the test table, never attacker-controlled input.
		f.maxRetries[t.TaskID] = int32(t.MaxRetries) //nolint:gosec // G115: test fixture; MaxRetries is a small literal set by the test, never attacker-controlled input.
		f.keys[t.TaskID] = t.NodeKey
		f.created[t.TaskID] = t
		if t.DispatchedAt != nil {
			f.dispatched = append(f.dispatched, t.TaskID)
		}
	}
	return nil
}
func (f *fakeTaskCollection) MarkDispatched(_ context.Context, ids []uuid.UUID, at time.Time) ([]uuid.UUID, error) {
	var stamped []uuid.UUID
	for _, id := range ids {
		if f.statuses[id] == run.TaskStatusPending && !slices.Contains(f.dispatched, id) {
			f.dispatched = append(f.dispatched, id)
			f.statuses[id] = run.TaskStatusDispatched
			if c, ok := f.created[id]; ok {
				stampedAt := at
				c.DispatchedAt = &stampedAt
				f.created[id] = c
			}
			stamped = append(stamped, id)
		}
	}
	return stamped, nil
}
func (f *fakeTaskCollection) ListRunTasks(_ context.Context, _ uuid.UUID) ([]run.Task, error) {
	return f.bulkCreated, nil
}
func (f *fakeTaskCollection) RunReleasesOf(_ context.Context, _ []uuid.UUID) (map[uuid.UUID]string, error) {
	panic("RunReleasesOf not used by the aggregate")
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
func TestMarkDispatchTerminal_BenignFinalizesSkipped(t *testing.T) {
	tc := newFakeTaskCollection()
	r := runningRunWithProjection(t, tc, uuid.New())
	evts, err := r.MarkDispatchTerminal(true, "", "no_tests", time.Now())
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
	evts, err := r.MarkDispatchTerminal(false, run.RunFailureEmptySelection, "empty_projection", time.Now())
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

// A run that fails without a task failing records one of the declared failure
// reasons: an empty or unknown one would be refused when the run is stored, so
// MarkDispatchTerminal refuses it before it changes the run.
func TestMarkDispatchTerminal_RefusesAFailureWithoutADeclaredReason(t *testing.T) {
	for name, reason := range map[string]run.RunFailureReason{
		"empty":   "",
		"unknown": "not_a_reason",
	} {
		t.Run(name, func(t *testing.T) {
			tc := newFakeTaskCollection()
			r := runningRunWithProjection(t, tc, uuid.New())

			evts, err := r.MarkDispatchTerminal(false, reason, "no reason", time.Now())

			if !errors.Is(err, run.ErrInvalidRunFailureReason) {
				t.Fatalf("err=%v, want ErrInvalidRunFailureReason", err)
			}
			if len(evts) != 0 {
				t.Fatalf("events=%v, want none", evts)
			}
			if r.Status() != run.SchedulerStatusRunning {
				t.Fatalf("status=%q, want the run left running", r.Status())
			}
			if r.FailureReason() != nil {
				t.Fatalf("failure reason=%q, want none", *r.FailureReason())
			}
		})
	}
}

func TestCanBeRerunSource_AcceptsFailedTerminalWithWork(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	r := runningRunWithProjection(t, tc, uuid.New())
	_, _ = r.MarkDispatchTerminal(false, run.RunFailureEmptySelection, "orchestrator empty", time.Now())
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
	_, _ = r.MarkDispatchTerminal(false, run.RunFailureEmptySelection, "any reason", time.Now())
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
	if _, err := domain.ComputeJobName("-", "-", "-", "", 0); err == nil {
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

// Cancelling a run cancels its open attempts in the same call; an attempt
// that has ended keeps its status.
func TestCancel_CancelsTheRunsOpenAttempts(t *testing.T) {
	ctx := context.Background()
	r, tc, g := startedFailureRun(t)
	id1, id2 := taskOf(r, "ftable_a"), taskOf(r, "ftable_b")
	report(t, r, tc, g, "ftable_a", 0, run.AttemptRunning, "")
	report(t, r, tc, g, "ftable_b", 0, run.AttemptSucceeded, "")

	if _, err := r.Cancel(ctx, tc, "tester", "drift", time.Now()); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	if len(tc.cancelledRuns) != 1 || tc.cancelledRuns[0] != r.ScheduleID() {
		t.Fatalf("CancelOpenAttempts calls = %v, want [%s]", tc.cancelledRuns, r.ScheduleID())
	}
	if got := tc.attemptRows[attemptKey{id1, 0}].Status; got != run.AttemptCancelled {
		t.Fatalf("running attempt status = %q, want %q", got, run.AttemptCancelled)
	}
	if got := tc.attemptRows[attemptKey{id2, 0}].Status; got != run.AttemptSucceeded {
		t.Fatalf("ended attempt status = %q, want %q", got, run.AttemptSucceeded)
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
func TestRun_Cancel_SkippedRun_ErrAlreadyTerminal(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	r := runningRunWithProjection(t, tc, uuid.New())
	if _, err := r.MarkDispatchTerminal(true, "", "no_tests", time.Now()); err != nil {
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
