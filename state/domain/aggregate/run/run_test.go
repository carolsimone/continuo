package run_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain"
	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPendingRun_CreatesAPendingRun(t *testing.T) {
	now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)

	r, err := run.NewPendingRun(pkgevents.DefaultTenantID, uuid.New(), "daily_orders", run.KindCron, nil, identity.SystemUserID, model.OperationTest, run.ReleaseStamp{}, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.ScheduleName() != "daily_orders" {
		t.Fatalf("schedule_name: got %q want %q", r.ScheduleName(), "daily_orders")
	}
	if r.Status() != run.RunStatusPending {
		t.Fatalf("status: got %q want pending", r.Status())
	}
	if r.Kind() != run.KindCron {
		t.Fatalf("kind: got %q want cron", r.Kind())
	}
	if r.RunID() == uuid.Nil {
		t.Fatalf("schedule_id should be generated")
	}
	if r.Operation() != model.OperationTest {
		t.Fatalf("operation: got %q want test", r.Operation())
	}
	if !r.Changes().IsCreated() {
		t.Fatal("a new run must be marked created")
	}
}

func TestNewRun_StampsTheInitiator(t *testing.T) {
	now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)

	pending, err := run.NewPendingRun(pkgevents.DefaultTenantID, uuid.New(), "daily", run.KindTrigger, nil, "okta|alice", model.OperationRun, run.ReleaseStamp{}, now)
	if err != nil || pending.InitiatedBy() != "okta|alice" {
		t.Fatalf("pending run InitiatedBy = %q, err = %v; want okta|alice", pending.InitiatedBy(), err)
	}
	derived, err := run.NewDerivedRun(pkgevents.DefaultTenantID, uuid.New(), "daily", run.KindRerun, uuid.New(), run.ReleaseStamp{}, "okta|bob", model.OperationRun, now)
	if err != nil || derived.InitiatedBy() != "okta|bob" {
		t.Fatalf("derived run InitiatedBy = %q, err = %v; want okta|bob", derived.InitiatedBy(), err)
	}
	single, err := run.NewSingleNodeRun(pkgevents.DefaultTenantID, uuid.New(), "single-node-run-abcd1234", model.OperationTest, nil, "okta|carol", run.ReleaseStamp{}, now)
	if err != nil || single.InitiatedBy() != "okta|carol" || single.Operation() != model.OperationTest {
		t.Fatalf("single-node run InitiatedBy = %q, Operation = %q, err = %v; want okta|carol, test", single.InitiatedBy(), single.Operation(), err)
	}
}

func TestNewPendingRun_KeepsItsTenant(t *testing.T) {
	r, err := run.NewPendingRun("acme", uuid.New(), "daily", run.KindCron, nil, identity.SystemUserID, model.OperationRun, run.ReleaseStamp{}, time.Now())
	require.NoError(t, err)
	assert.Equal(t, "acme", r.TenantID())
}

func TestNewPendingRun_RefusesAnEmptyTenant(t *testing.T) {
	_, err := run.NewPendingRun("", uuid.New(), "daily", run.KindCron, nil, identity.SystemUserID, model.OperationRun, run.ReleaseStamp{}, time.Now())
	assert.ErrorIs(t, err, run.ErrNoTenant)
}

func TestNewDerivedRun_RefusesAnEmptyTenant(t *testing.T) {
	_, err := run.NewDerivedRun("", uuid.New(), "daily", run.KindRerun, uuid.New(), run.ReleaseStamp{}, "alice", model.OperationRun, time.Now())
	assert.ErrorIs(t, err, run.ErrNoTenant)
}

func TestNewSingleNodeRun_RefusesAnEmptyTenant(t *testing.T) {
	_, err := run.NewSingleNodeRun("", uuid.New(), "single-node-run-ab12cd34", model.OperationRun, nil, "alice", run.ReleaseStamp{}, time.Now())
	assert.ErrorIs(t, err, run.ErrNoTenant)
}

// Every run takes its id from its caller: the id it was given, never a nil
// one.
func TestNewRuns_TakeTheirCallersRunID(t *testing.T) {
	id := uuid.New()
	pending, err := run.NewPendingRun("acme", id, "daily", run.KindTrigger, nil, "alice", model.OperationRun, run.ReleaseStamp{}, time.Now())
	require.NoError(t, err)
	assert.Equal(t, id, pending.RunID())
	derived, err := run.NewDerivedRun("acme", id, "daily", run.KindRerun, uuid.New(), run.ReleaseStamp{}, "alice", model.OperationRun, time.Now())
	require.NoError(t, err)
	assert.Equal(t, id, derived.RunID())
	single, err := run.NewSingleNodeRun("acme", id, "single-node-run-ab12cd34", model.OperationRun, nil, "alice", run.ReleaseStamp{}, time.Now())
	require.NoError(t, err)
	assert.Equal(t, id, single.RunID())

	_, err = run.NewPendingRun("acme", uuid.Nil, "daily", run.KindTrigger, nil, "alice", model.OperationRun, run.ReleaseStamp{}, time.Now())
	assert.ErrorIs(t, err, run.ErrRunIDRequired)
	_, err = run.NewDerivedRun("acme", uuid.Nil, "daily", run.KindRerun, uuid.New(), run.ReleaseStamp{}, "alice", model.OperationRun, time.Now())
	assert.ErrorIs(t, err, run.ErrRunIDRequired)
	_, err = run.NewSingleNodeRun("acme", uuid.Nil, "single-node-run-ab12cd34", model.OperationRun, nil, "alice", run.ReleaseStamp{}, time.Now())
	assert.ErrorIs(t, err, run.ErrRunIDRequired)
}

func TestNewSingleNodeRun_KeepsItsTenant(t *testing.T) {
	r, err := run.NewSingleNodeRun("acme", uuid.New(), "single-node-run-ab12cd34", model.OperationRun, nil, "alice", run.ReleaseStamp{}, time.Now())
	require.NoError(t, err)
	assert.Equal(t, "acme", r.TenantID())
}

// A rerun or rebase belongs to the tenant of the run it derives from.
func TestNewDerivedRun_CarriesTheTenantOfItsSource(t *testing.T) {
	source := run.HydrateRun("acme", uuid.New(), "daily", run.RunStatusFailed, run.KindCron, nil, "system", model.OperationRun,
		time.Now(), nil, nil, nil, nil, nil, nil, nil, 0)
	for _, kind := range []run.Kind{run.KindRerun, run.KindRebase} {
		derived, err := run.NewDerivedRun(source.TenantID(), uuid.New(), source.ScheduleName(), kind, source.RunID(), source.Release(), "alice", source.Operation(), time.Now())
		require.NoError(t, err)
		assert.Equal(t, "acme", derived.TenantID(), string(kind))
	}
}

func TestNewPromotedSeedsRun_RefusesAnEmptyTenant(t *testing.T) {
	_, err := run.NewPromotedSeedsRun("", uuid.New(), "promote-seed-ab12cd34", run.ReleaseStamp{ReleaseID: "rel-1"}, time.Now())
	assert.ErrorIs(t, err, run.ErrNoTenant)
}

func TestHydrateRun_KeepsItsTenant(t *testing.T) {
	r := run.HydrateRun("acme", uuid.New(), "daily", run.RunStatusRunning, run.KindCron, nil, "system", model.OperationRun,
		time.Now(), nil, nil, nil, nil, nil, nil, nil, 0)
	assert.Equal(t, "acme", r.TenantID())
}

func TestNewPendingRun_RejectsEmptyName(t *testing.T) {
	_, err := run.NewPendingRun(pkgevents.DefaultTenantID, uuid.New(), "", run.KindCron, nil, identity.SystemUserID, model.OperationRun, run.ReleaseStamp{}, time.Now())
	if err != run.ErrScheduleNameRequired {
		t.Fatalf("err: got %v want %v", err, run.ErrScheduleNameRequired)
	}
}

// A schedule name may be up to 50 characters long, counted in characters
// rather than bytes. A longer name is ErrScheduleNameTooLong, with a message
// that names the limit and the name's length.
func TestNewPendingRun_RefusesANameLongerThanFiftyCharacters(t *testing.T) {
	fifty, fiftyOne := strings.Repeat("a", 50), strings.Repeat("a", 51)
	accented, accentedTooLong := strings.Repeat("é", 50), strings.Repeat("é", 51)
	require.Len(t, accented, 100, "each é is two bytes")

	for _, name := range []string{fifty, accented} {
		r, err := run.NewPendingRun(pkgevents.DefaultTenantID, uuid.New(), name, run.KindTrigger, nil, "alice", model.OperationRun, run.ReleaseStamp{}, time.Now())
		require.NoError(t, err, "a name of 50 characters (%d bytes) is a run's", len(name))
		assert.Equal(t, name, r.ScheduleName())
	}

	for _, name := range []string{fiftyOne, accentedTooLong} {
		_, err := run.NewPendingRun(pkgevents.DefaultTenantID, uuid.New(), name, run.KindTrigger, nil, "alice", model.OperationRun, run.ReleaseStamp{}, time.Now())
		require.ErrorIs(t, err, run.ErrScheduleNameTooLong, "a name of 51 characters (%d bytes) is refused", len(name))
		assert.Equal(t, "schedule_name must be at most 50 characters long, not 51", err.Error())
	}
}

// Every run constructor holds the schedule name to the same limit.
func TestRunConstructors_RefuseANameLongerThanARunCarries(t *testing.T) {
	tooLong := strings.Repeat("a", run.MaxScheduleNameChars+1)
	stamp := run.ReleaseStamp{ReleaseID: "rel-1", PromotionSeq: 1}
	constructors := map[string]func(name string) error{
		"NewDerivedRun": func(name string) error {
			_, err := run.NewDerivedRun("acme", uuid.New(), name, run.KindRerun, uuid.New(), stamp, "alice", model.OperationRun, time.Now())
			return err
		},
		"NewSingleNodeRun": func(name string) error {
			_, err := run.NewSingleNodeRun("acme", uuid.New(), name, model.OperationRun, nil, "alice", stamp, time.Now())
			return err
		},
		"NewPromotedSeedsRun": func(name string) error {
			_, err := run.NewPromotedSeedsRun("acme", uuid.New(), name, stamp, time.Now())
			return err
		},
	}
	for constructor, build := range constructors {
		require.NoError(t, build(tooLong[:run.MaxScheduleNameChars]), constructor)
		assert.ErrorIs(t, build(tooLong), run.ErrScheduleNameTooLong, constructor)
		assert.ErrorIs(t, build(""), run.ErrScheduleNameRequired, constructor)
	}
}

func TestNewPendingRun_RejectsInvalidKind(t *testing.T) {
	_, err := run.NewPendingRun(pkgevents.DefaultTenantID, uuid.New(), "x", run.Kind("bogus"), nil, identity.SystemUserID, model.OperationRun, run.ReleaseStamp{}, time.Now())
	if err != run.ErrInvalidKind {
		t.Fatalf("err: got %v want %v", err, run.ErrInvalidKind)
	}
}

// A cron run is stamped with the slot of its schedule it is fired for; a run
// of any other kind is refused one and keeps none.
func TestSetScheduledFor_OnlyACronRunHasASlot(t *testing.T) {
	slot := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	cronRun, err := run.NewPendingRun(pkgevents.DefaultTenantID, uuid.New(), "daily", run.KindCron, nil, identity.SystemUserID, model.OperationRun, run.ReleaseStamp{}, slot)
	require.NoError(t, err)
	assert.Nil(t, cronRun.ScheduledFor(), "a run has no slot until it is stamped")

	require.NoError(t, cronRun.SetScheduledFor(slot))
	require.NotNil(t, cronRun.ScheduledFor())
	assert.True(t, slot.Equal(*cronRun.ScheduledFor()))

	for _, kind := range []run.Kind{run.KindTrigger, run.KindSingleNodeRun, run.KindPromoteSeed} {
		other, err := run.NewPendingRun(pkgevents.DefaultTenantID, uuid.New(), "daily", kind, nil, "okta|alice", model.OperationRun, run.ReleaseStamp{}, slot)
		require.NoError(t, err)
		require.ErrorIs(t, other.SetScheduledFor(slot), run.ErrNotACronRun, "kind %s", kind)
		assert.Nil(t, other.ScheduledFor(), "kind %s keeps no slot", kind)
	}
}

// taskDefaults are the task settings the aggregate tests plan with: the
// chart's defaults.
var taskDefaults = run.TaskDefaults{MaxRetries: 2, Timeout: 24 * time.Hour}

// fakeTaskCollection is the in-memory test double used by every aggregate
// unit test in this file. Tests set fields per scenario.
type fakeTaskCollection struct {
	bulkCreated []run.Task
	// stopsBegun holds the runs BeginStop was called for, cancelledBy who
	// each pending task BeginStop cancelled was cancelled by, and
	// stopRequests the runs RequestStops was called for.
	stopsBegun   []uuid.UUID
	cancelledBy  map[uuid.UUID]string
	stopRequests []uuid.UUID
	// stopRequested holds the attempts RequestStops asked to stop;
	// unansweredStops makes RequestStops ask none.
	stopRequested   map[attemptKey]bool
	unansweredStops bool
	statuses        map[uuid.UUID]run.TaskStatus
	attempts        map[uuid.UUID]int32
	hasFailed       bool
	hasNonSucc      bool
	byNode          map[run.NodeID]run.Task
	updateApplied   map[uuid.UUID]int
	loadErr         error
	maxRetries      map[uuid.UUID]int32
	keys            map[uuid.UUID]topology.NodeKey
	// attemptRows holds the stored attempts; lockOrder the LoadAttemptState
	// ("task <id>") and LoadAttempt ("attempt <id>/<n>") calls in call order.
	attemptRows map[attemptKey]run.Attempt
	lockOrder   []string
	// saves holds every attempt SaveAttempt was given, in call order; added
	// every attempt AddAttempts inserted.
	saves []run.Attempt
	added []run.Attempt
	// created holds each task BulkCreate stored, with the status and dispatch
	// time MarkDispatched stamped on it.
	created map[uuid.UUID]run.Task
	// dispatched holds the tasks stamped dispatched, by BulkCreate or
	// MarkDispatched; skipped the tasks MarkSkipped changed, in call order.
	dispatched []uuid.UUID
	skipped    []uuid.UUID
	// derived makes HasFailed read the stored statuses instead of
	// hasFailed.
	derived bool
	// leases holds the task that holds each node's write lease; waits the
	// tasks waiting for one. leaseTakes holds the claims of each TakeLeases
	// call, waiterTakes the claim of each TakeLeaseForWaiter call and
	// releases the task of each ReleaseLease call, in call order. leaseWrites
	// holds every TakeLeases ("take <node keys>") and ReleaseLease ("release
	// <node key>") call in one list, in call order.
	leases      map[topology.NodeKey]uuid.UUID
	waits       map[uuid.UUID]run.WaitReason
	leaseTakes  [][]run.LeaseClaim
	waiterTakes []run.LeaseClaim
	releases    []uuid.UUID
	leaseWrites []string
	// waitSince holds when each waiting task began to wait, and
	// grantableReads the runs GrantableWaits was called for. nextInLine, when
	// set, is what GrantableWaits returns, as a read taken before another
	// transaction took the node would.
	waitSince      map[uuid.UUID]time.Time
	grantableReads []uuid.UUID
	nextInLine     []run.LeaseWait
}

func newFakeTaskCollection() *fakeTaskCollection {
	return &fakeTaskCollection{
		cancelledBy:   map[uuid.UUID]string{},
		stopRequested: map[attemptKey]bool{},
		statuses:      map[uuid.UUID]run.TaskStatus{},
		attempts:      map[uuid.UUID]int32{},
		byNode:        map[run.NodeID]run.Task{},
		updateApplied: map[uuid.UUID]int{},
		maxRetries:    map[uuid.UUID]int32{},
		keys:          map[uuid.UUID]topology.NodeKey{},
		attemptRows:   map[attemptKey]run.Attempt{},
		created:       map[uuid.UUID]run.Task{},
		leases:        map[topology.NodeKey]uuid.UUID{},
		waits:         map[uuid.UUID]run.WaitReason{},
		waitSince:     map[uuid.UUID]time.Time{},
	}
}

// GrantableWaits returns the pending tasks of runID that wait for a node no
// task holds, in node-key order; a test builds one run, so each such task is
// its node's oldest waiter. With nextInLine set it returns that.
func (f *fakeTaskCollection) GrantableWaits(_ context.Context, runID uuid.UUID) ([]run.LeaseWait, error) {
	f.grantableReads = append(f.grantableReads, runID)
	if f.nextInLine != nil {
		return f.nextInLine, nil
	}
	var out []run.LeaseWait
	for id, w := range f.waits {
		if _, held := f.leases[f.keys[id]]; held || w != run.WaitReasonNodeLease ||
			f.statuses[id] != run.TaskStatusPending || f.created[id].ScheduleID != runID {
			continue
		}
		out = append(out, run.LeaseWait{TaskID: id, NodeKey: f.keys[id], WaitingSince: f.waitSince[id]})
	}
	slices.SortFunc(out, func(a, b run.LeaseWait) int { return strings.Compare(string(a.NodeKey), string(b.NodeKey)) })
	return out, nil
}

// TakeLeases takes, in node-key order, each claimed node no task holds and no
// other pending task waits for, and returns the claiming tasks that hold their
// node's lease afterwards, as the real adapter does.
func (f *fakeTaskCollection) TakeLeases(_ context.Context, claims []run.LeaseClaim) ([]uuid.UUID, error) {
	f.leaseTakes = append(f.leaseTakes, slices.Clone(claims))
	keys := make([]string, 0, len(claims))
	for _, c := range claims {
		keys = append(keys, string(c.NodeKey))
	}
	f.leaseWrites = append(f.leaseWrites, "take "+strings.Join(keys, ","))
	ordered := slices.Clone(claims)
	slices.SortFunc(ordered, func(a, b run.LeaseClaim) int { return strings.Compare(string(a.NodeKey), string(b.NodeKey)) })
	var taken []uuid.UUID
	for _, c := range ordered {
		holder, held := f.leases[c.NodeKey]
		switch {
		case held && holder == c.TaskID:
			taken = append(taken, c.TaskID)
		case !held && !f.othersWaitFor(c.NodeKey, c.TaskID):
			f.leases[c.NodeKey] = c.TaskID
			taken = append(taken, c.TaskID)
		}
	}
	return taken, nil
}

// othersWaitFor reports whether a pending task other than self waits for the
// lease on key.
func (f *fakeTaskCollection) othersWaitFor(key topology.NodeKey, self uuid.UUID) bool {
	for id, w := range f.waits {
		if id != self && w == run.WaitReasonNodeLease && f.keys[id] == key && f.statuses[id] == run.TaskStatusPending {
			return true
		}
	}
	return false
}

// WaitForLeases marks each task waiting for its node's lease.
func (f *fakeTaskCollection) WaitForLeases(_ context.Context, ids []uuid.UUID) error {
	for _, id := range ids {
		f.waits[id] = run.WaitReasonNodeLease
	}
	return nil
}

// ReleaseLease deletes the lease the task holds, if any.
func (f *fakeTaskCollection) ReleaseLease(_ context.Context, id uuid.UUID) error {
	f.releases = append(f.releases, id)
	f.leaseWrites = append(f.leaseWrites, "release "+string(f.keys[id]))
	for k, holder := range f.leases {
		if holder == id {
			delete(f.leases, k)
		}
	}
	return nil
}

// TakeLeaseForWaiter takes the claimed node for its task when no task holds
// it, without looking at the node's other waiters.
func (f *fakeTaskCollection) TakeLeaseForWaiter(_ context.Context, c run.LeaseClaim) (bool, error) {
	f.waiterTakes = append(f.waiterTakes, c)
	holder, held := f.leases[c.NodeKey]
	if held {
		return holder == c.TaskID, nil
	}
	f.leases[c.NodeKey] = c.TaskID
	return true, nil
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
		if s.IsInFlight() {
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
			delete(f.waits, id)
			f.skipped = append(f.skipped, id)
			n++
		}
	}
	return n, nil
}

// BeginStop makes the dispatched, queued and running tasks the fake holds
// whose current attempt is open stopping, and cancels the pending ones and
// those with no open current attempt, ending their waits and releasing their
// leases: a test builds one run, so every stored task belongs to it.
func (f *fakeTaskCollection) BeginStop(_ context.Context, runID uuid.UUID, by string, _ time.Time) (run.StopCounts, error) {
	f.stopsBegun = append(f.stopsBegun, runID)
	var counts run.StopCounts
	for id, s := range f.statuses {
		switch s {
		case run.TaskStatusPending, run.TaskStatusDispatched, run.TaskStatusQueued, run.TaskStatusRunning:
			if a, ok := f.attemptRows[attemptKey{id, f.attempts[id]}]; s != run.TaskStatusPending && ok && !a.Status.IsFinal() {
				f.statuses[id] = run.TaskStatusStopping
				counts.Stopping++
				continue
			}
			f.statuses[id] = run.TaskStatusCancelled
			delete(f.waits, id)
			for k, holder := range f.leases {
				if holder == id {
					delete(f.leases, k)
				}
			}
			f.cancelledBy[id] = by
			counts.Cancelled++
		}
	}
	return counts, nil
}

// RequestStops asks the current attempt of each stopping task to stop when it
// is open, and returns those attempts; with unansweredStops it asks none.
func (f *fakeTaskCollection) RequestStops(_ context.Context, runID uuid.UUID) ([]run.StopTarget, error) {
	f.stopRequests = append(f.stopRequests, runID)
	if f.unansweredStops {
		return nil, nil
	}
	var targets []run.StopTarget
	for id, s := range f.statuses {
		if s != run.TaskStatusStopping {
			continue
		}
		k := attemptKey{id, f.attempts[id]}
		a, ok := f.attemptRows[k]
		if !ok || a.Status.IsFinal() {
			continue
		}
		f.stopRequested[k] = true
		targets = append(targets, run.StopTarget{TaskID: id, Attempt: k.attempt, JobName: a.JobName})
	}
	return targets, nil
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
		RunID:  c.ScheduleID,
		Status: s, Attempt: f.attempts[id], MaxRetries: f.maxRetries[id], NodeKey: f.keys[id],
		JobName: c.JobName, ServiceName: c.ServiceName, SchemaName: c.SchemaName, TableName: c.TableName,
		DispatchedAt: c.DispatchedAt, WaitReason: f.waits[id],
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
			delete(f.waits, id)
			if c, ok := f.created[id]; ok {
				stampedAt := at
				c.DispatchedAt = &stampedAt
				c.Status = run.TaskStatusDispatched
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
			delete(f.waits, id)
			f.skipped = append(f.skipped, id)
			n++
		}
	}
	return n, nil
}
func (f *fakeTaskCollection) Update(_ context.Context, t run.Task) error {
	f.statuses[t.TaskID] = t.Status
	return nil
}

// freshPendingRun returns a Run in PENDING + init_status=in_progress with
// zero counters — the state on entry to Start.
func freshPendingRun(t *testing.T) *run.Run {
	t.Helper()
	r, err := run.NewPendingRun(pkgevents.DefaultTenantID, uuid.New(), "daily", run.KindCron, nil, identity.SystemUserID, model.OperationRun, run.ReleaseStamp{}, time.Now())
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
	if r.Status() != run.RunStatusSkipped {
		t.Fatalf("status=%q, want skipped", r.Status())
	}
	fin, ok := evts[0].(run.RunFinalized)
	if !ok || fin.Outcome != run.RunStatusSkipped {
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
	if r.Status() != run.RunStatusFailed {
		t.Fatalf("status=%q, want failed", r.Status())
	}
	fin, ok := evts[0].(run.RunFinalized)
	if !ok || fin.Outcome != run.RunStatusFailed {
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
			if r.Status() != run.RunStatusRunning {
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

// dispatchedRun returns a RUNNING run with one pending task per id, dispatched
// at dispatchedAt, with a clean change set.
func dispatchedRun(t *testing.T, tc *fakeTaskCollection, dispatchedAt time.Time, ids ...uuid.UUID) *run.Run {
	t.Helper()
	total := int32(len(ids)) //nolint:gosec // G115: test fixture; a handful of ids.
	r := run.HydrateRun(pkgevents.DefaultTenantID, uuid.New(), "daily", run.RunStatusRunning, run.KindCron, nil,
		identity.SystemUserID, model.OperationRun, dispatchedAt, &dispatchedAt, nil, &dispatchedAt, nil, nil, nil,
		&total, 0)
	tasks := make([]run.Task, 0, len(ids))
	for i, id := range ids {
		tasks = append(tasks, run.Task{
			TaskID: id, ScheduleID: r.RunID(), ServiceName: "s", SchemaName: "p", TableName: fmt.Sprintf("t%d", i),
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
	if r.LastProgressAt() != nil {
		t.Fatalf("LastProgressAt = %v, want nil before the run is dispatched", *r.LastProgressAt())
	}
	if r.Changes().IsHeartbeatDirty() {
		t.Fatal("a new run must not mark the progress clock dirty")
	}
}

// A promote-seed run carries the id it is given and the release it builds.
func TestNewPromotedSeedsRun_CarriesTheRelease(t *testing.T) {
	id := uuid.New()
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

	r, err := run.NewPromotedSeedsRun(pkgevents.DefaultTenantID, id, "promote-seed-abc12345", run.ReleaseStamp{ReleaseID: "rel-1", PromotionSeq: 7}, now)
	if err != nil {
		t.Fatalf("NewPromotedSeedsRun: %v", err)
	}
	if r.RunID() != id || r.Release() != (run.ReleaseStamp{ReleaseID: "rel-1", PromotionSeq: 7}) {
		t.Fatalf("id/release = %s/%+v, want %s/rel-1 7", r.RunID(), r.Release(), id)
	}
	if r.Kind() != run.KindPromoteSeed || r.Status() != run.RunStatusRunning {
		t.Fatalf("kind/status = %q/%q, want promote_seed/running", r.Kind(), r.Status())
	}
}

// A run rebuilt from its row gets its release back only through
// HydrateReleaseID; a run that names none reports "".
func TestHydrateReleaseID_RestoresTheStoredRelease(t *testing.T) {
	r := run.HydrateRun(pkgevents.DefaultTenantID, uuid.New(), "promote-seed-abc12345", run.RunStatusRunning, run.KindPromoteSeed, nil, identity.SystemUserID, model.OperationRun, time.Now(),
		nil, nil, nil, nil, nil, nil, nil, 0)
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
	rerun, err := run.NewDerivedRun(pkgevents.DefaultTenantID, uuid.New(), "promote-seed-abc12345", run.KindRerun, uuid.New(), run.ReleaseStamp{ReleaseID: "rel-1", PromotionSeq: 3}, "okta|alice", model.OperationRun, time.Now())
	if err != nil {
		t.Fatalf("NewDerivedRun(rerun): %v", err)
	}
	if rerun.ReleaseID() != "rel-1" || rerun.Release().PromotionSeq != 3 {
		t.Fatalf("rerun Release() = %+v, want rel-1 / 3", rerun.Release())
	}
}

// A cancel of a run with an attempt in flight stops it: the task in flight is
// stopping and its attempt asked to stop, the pending tasks are cancelled and
// count terminal, and the run is cancelling, not ended, until the attempt
// answers. Only RunCancelled is recorded, naming no attempt.
func TestCancel_MovesTasksInFlightToStoppingAndTheRunToCancelling(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	report(t, r, tc, g, "ftable_a", 0, run.AttemptRunning, "")
	report(t, r, tc, g, "ftable_b", 0, run.AttemptSucceeded, "")
	cancelAt := reportAt.Add(time.Minute)

	events, err := r.Cancel(context.Background(), tc, "alice", "bad data", cancelAt)

	require.NoError(t, err)
	assert.Equal(t, []run.DomainEvent{run.RunCancelled{
		ID: r.RunID(), TenantID: r.TenantID(), Name: "e2e-schedule-failure", By: "alice", CancellationReason: "bad data",
	}}, events)
	assert.Equal(t, run.RunStatusCancelling, r.Status())
	assert.True(t, r.IsActive())
	assert.False(t, r.IsTerminal())
	assert.Nil(t, r.CompletedAt(), "a cancelling run has not ended")
	require.NotNil(t, r.CancelledAt())
	assert.Equal(t, cancelAt, *r.CancelledAt())
	assert.Equal(t, "alice", *r.CancelledBy())
	assert.Equal(t, "bad data", *r.CancellationReason())
	assert.Equal(t, cancelAt, *r.LastProgressAt(), "the cancel moves the run's tasks")
	assert.True(t, r.Changes().IsCancelDirty())

	assert.Equal(t, run.TaskStatusStopping, tc.statuses[taskOf(r, "ftable_a")])
	assert.Equal(t, run.TaskStatusSucceeded, tc.statuses[taskOf(r, "ftable_b")], "an ended task keeps its status")
	for _, table := range []string{"ftable_c", "ftable_d", "ftable_e", "ftable_f", "ftable_g", "ftable_h"} {
		assert.Equal(t, run.TaskStatusCancelled, tc.statuses[taskOf(r, table)], table)
		assert.Equal(t, "alice", tc.cancelledBy[taskOf(r, table)], table)
	}
	assert.Equal(t, map[attemptKey]bool{{taskOf(r, "ftable_a"), 0}: true}, tc.stopRequested)
	assert.Equal(t, int32(7), r.TerminalTaskCount(), "ftable_b and the six cancelled tasks")
}

// A cancel of a run with nothing in flight ends it at once: its pending tasks
// are cancelled and count terminal, no stop is requested, and the run is
// cancelled with completed_at at the cancel.
func TestCancel_ARunWithNothingInFlightIsCancelledAtOnce(t *testing.T) {
	tc := newFakeTaskCollection()
	r := runningRunWithProjection(t, tc, uuid.New(), uuid.New())
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)

	events, err := r.Cancel(context.Background(), tc, "alice", "bad data", now)

	require.NoError(t, err)
	assert.Equal(t, []run.DomainEvent{
		run.RunCancelled{ID: r.RunID(), TenantID: r.TenantID(), Name: "daily", By: "alice", CancellationReason: "bad data"},
		run.RunFinalized{ID: r.RunID(), Name: "daily", Outcome: run.RunStatusCancelled},
	}, events)
	assert.Equal(t, run.RunStatusCancelled, r.Status())
	require.NotNil(t, r.CompletedAt())
	assert.Equal(t, now, *r.CompletedAt())
	assert.Equal(t, now, *r.CancelledAt())
	assert.Equal(t, int32(2), r.TerminalTaskCount())
	assert.Empty(t, tc.stopRequests, "nothing is in flight")
	for _, task := range tc.bulkCreated {
		assert.Equal(t, run.TaskStatusCancelled, tc.statuses[task.TaskID])
	}
}

// A task in flight whose current attempt has ended has no attempt to answer a
// stop: the cancel cancels it with the pending tasks, and a run with nothing
// else in flight ends cancelled at once.
func TestCancel_ATaskInFlightWithNoOpenAttemptIsCancelled(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	report(t, r, tc, g, "ftable_a", 0, run.AttemptRunning, "")
	report(t, r, tc, g, "ftable_b", 0, run.AttemptRunning, "")
	delete(tc.attemptRows, attemptKey{taskOf(r, "ftable_b"), 0})
	cancelAt := reportAt.Add(time.Minute)

	_, err := r.Cancel(context.Background(), tc, "alice", "bad data", cancelAt)

	require.NoError(t, err)
	assert.Equal(t, run.RunStatusCancelling, r.Status(), "ftable_a's attempt is open")
	assert.Equal(t, run.TaskStatusStopping, tc.statuses[taskOf(r, "ftable_a")])
	assert.Equal(t, run.TaskStatusCancelled, tc.statuses[taskOf(r, "ftable_b")], "no attempt of it is open")
	assert.Equal(t, map[attemptKey]bool{{taskOf(r, "ftable_a"), 0}: true}, tc.stopRequested)
	assert.Equal(t, int32(7), r.TerminalTaskCount(), "ftable_b and the six pending tasks")
}

// A cancel that makes a task stopping without asking an attempt of it to stop
// would leave a run no report ever ends: it fails, so its transaction rolls
// back, and the run is left as it was.
func TestCancel_AStoppingTaskWithNoStopRequestedFailsTheCancel(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	report(t, r, tc, g, "ftable_a", 0, run.AttemptRunning, "")
	tc.unansweredStops = true

	events, err := r.Cancel(context.Background(), tc, "alice", "bad data", reportAt.Add(time.Minute))

	require.ErrorIs(t, err, run.ErrStopUnanswerable)
	assert.Empty(t, events)
	assert.Equal(t, run.RunStatusRunning, r.Status())
	assert.Nil(t, r.CancelledAt())
	assert.False(t, r.Changes().IsCancelDirty())
}

// A second cancel of a cancelling run is refused before it touches a task:
// the first cancel's stops stand.
func TestCancel_ACancellingRunIsRefusedAsAlreadyCancelling(t *testing.T) {
	r, tc, _ := startedFailureRun(t)
	_, err := r.Cancel(context.Background(), tc, "alice", "bad data", reportAt)
	require.NoError(t, err)

	events, err := r.Cancel(context.Background(), tc, "bob", "again", reportAt.Add(time.Minute))

	require.ErrorIs(t, err, run.ErrAlreadyCancelling)
	assert.Empty(t, events)
	assert.Len(t, tc.stopsBegun, 1)
	assert.Equal(t, run.RunStatusCancelling, r.Status())
	assert.Equal(t, "alice", *r.CancelledBy())
	assert.Equal(t, reportAt, *r.CancelledAt())
}

// A run that has ended is never cancelled, and none of its tasks is touched.
func TestCancel_ATerminalRunIsRefused(t *testing.T) {
	for name, end := range map[string]func(*run.Run, *fakeTaskCollection) error{
		"skipped": func(r *run.Run, _ *fakeTaskCollection) error {
			_, err := r.MarkDispatchTerminal(true, "", "no_tests", time.Now())
			return err
		},
		"cancelled": func(r *run.Run, tc *fakeTaskCollection) error {
			_, err := r.Cancel(context.Background(), tc, "alice", "", time.Now())
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			tc := newFakeTaskCollection()
			r := runningRunWithProjection(t, tc, uuid.New())
			require.NoError(t, end(r, tc))
			begun := len(tc.stopsBegun)

			events, err := r.Cancel(context.Background(), tc, "bob", "late", time.Now())

			require.ErrorIs(t, err, run.ErrAlreadyTerminal)
			assert.Empty(t, events)
			assert.Len(t, tc.stopsBegun, begun, "no task is touched")
		})
	}
}
