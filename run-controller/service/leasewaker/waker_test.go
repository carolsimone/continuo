package leasewaker_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/run-controller/domain/repository"
	"github.com/carolsimone/continuo/run-controller/service/leasewaker"
	"github.com/carolsimone/continuo/run-controller/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	now      = time.Date(2026, 10, 24, 10, 6, 40, 0, time.UTC)
	defaults = run.TaskDefaults{MaxRetries: 2, Timeout: 24 * time.Hour}
)

// fakeLeases serves the waiters, repairs and waiter count of a pass; each
// Grantable call records its limit and signals passes when it is set.
type fakeLeases struct {
	grantable []repository.LeaseWaiter
	repaired  []repository.RepairedLease
	waiters   int
	limit     int
	passes    chan struct{}
}

func (f *fakeLeases) Grantable(_ context.Context, limit int) ([]repository.LeaseWaiter, error) {
	f.limit = limit
	if f.passes != nil {
		f.passes <- struct{}{}
	}
	return f.grantable, nil
}
func (f *fakeLeases) RepairEnded(context.Context) ([]repository.RepairedLease, error) {
	return f.repaired, nil
}
func (f *fakeLeases) CountWaiters(context.Context) (int, error) { return f.waiters, nil }

// recordingMetrics records what the waker observes.
type recordingMetrics struct {
	waiters  []int
	granted  []time.Duration
	repaired int
}

func (*recordingMetrics) RunStalled()                       {}
func (*recordingMetrics) RunsCancelling(_, _ int)           {}
func (*recordingMetrics) RunCancelled(time.Duration)        {}
func (m *recordingMetrics) LeaseWaiters(n int)              { m.waiters = append(m.waiters, n) }
func (m *recordingMetrics) LeaseGranted(wait time.Duration) { m.granted = append(m.granted, wait) }
func (m *recordingMetrics) LeaseRepaired()                  { m.repaired++ }
func (m *recordingMetrics) CommandRequest(string, string)   {}
func (m *recordingMetrics) CronFire(string)                 {}
func (m *recordingMetrics) CronFires(string, int)           {}
func (m *recordingMetrics) Schedules(int, int, int)         {}

// runs serves the runs it holds for update, each only under its own tenant,
// fails a run it does not hold, and counts saves.
type runs struct {
	repository.RunRepository
	held  map[uuid.UUID]*run.Run
	saved int
}

func (s *runs) LoadRunForUpdate(_ context.Context, tenantID string, id uuid.UUID) (*run.Run, error) {
	r, ok := s.held[id]
	if !ok || r.TenantID() != tenantID {
		return nil, errors.New("run not found")
	}
	return r, nil
}
func (s *runs) SaveRun(context.Context, *run.Run) error { s.saved++; return nil }

// unreadTasks counts the tasks a grant reads.
type unreadTasks struct {
	run.TaskCollection
	reads int
}

func (t *unreadTasks) LoadAttemptState(context.Context, uuid.UUID) (run.TaskAttemptState, bool, error) {
	t.reads++
	return run.TaskAttemptState{}, false, nil
}

type recordingOutbox struct{ events []run.DomainEvent }

func (o *recordingOutbox) Append(_ context.Context, events []run.DomainEvent, _ uuid.UUID) error {
	o.events = append(o.events, events...)
	return nil
}

type fixedClock struct{}

func (fixedClock) Now() time.Time { return now }

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// runIn returns a run of the default tenant in status that writes its nodes.
func runIn(status run.RunStatus) *run.Run { return runOf(pkgevents.DefaultTenantID, status) }

// A pass runs at once, then on each wake signal without waiting for the
// interval, and Run returns once its context ends.
func TestLeaseWaker_RunsOnANotification(t *testing.T) {
	leases := &fakeLeases{passes: make(chan struct{}, 4)}
	f := &uow.FakeUnitOfWork{LeaseRepo: leases}
	waker := leasewaker.New(func() uow.UnitOfWork { return f }, defaults, &recordingMetrics{}, discard())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		waker.Run(ctx, wake, time.Hour)
		close(done)
	}()
	awaitPass := func(why string) {
		t.Helper()
		select {
		case <-leases.passes:
		case <-time.After(5 * time.Second):
			t.Fatal(why)
		}
	}

	awaitPass("the first pass runs at once")
	select {
	case <-leases.passes:
		t.Fatal("no pass runs before a notification or the interval")
	case <-time.After(50 * time.Millisecond):
	}
	wake <- struct{}{}
	awaitPass("a notification runs a pass before the interval")
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run returns once its context ends")
	}
}

// A waiter whose run stopped running after it was listed, here because it was
// cancelled, is granted nothing: its task is not read, the run is not saved,
// nothing is written to the outbox and no grant is observed. The pass asks for
// at most GrantLimit waiters and still sets the waiter gauge.
func TestLeaseWaker_LeavesAWaiterWhoseRunIsNoLongerRunning(t *testing.T) {
	r := runIn(run.RunStatusCancelling)
	leases := &fakeLeases{waiters: 1, grantable: []repository.LeaseWaiter{
		{TenantID: pkgevents.DefaultTenantID, TaskID: uuid.New(), RunID: r.RunID(), NodeKey: "e2e_schema.table_a", WaitingSince: now.Add(-4 * time.Minute)},
	}}
	held := &runs{held: map[uuid.UUID]*run.Run{r.RunID(): r}}
	tasks := &unreadTasks{}
	outbox := &recordingOutbox{}
	f := &uow.FakeUnitOfWork{LeaseRepo: leases}
	f.SetRunRepo(held)
	f.SetTaskCollection(tasks)
	f.SetOutboxPublisher(outbox)
	f.SetClock(fixedClock{})
	metrics := &recordingMetrics{}

	require.NoError(t, leasewaker.New(func() uow.UnitOfWork { return f }, defaults, metrics, discard()).Pass(context.Background()))

	assert.Equal(t, 200, leasewaker.GrantLimit)
	assert.Equal(t, leasewaker.GrantLimit, leases.limit)
	assert.Zero(t, tasks.reads, "the task of a run that is not running is not read")
	assert.Zero(t, held.saved)
	assert.Empty(t, outbox.events)
	assert.Empty(t, metrics.granted)
	assert.Equal(t, []int{1}, metrics.waiters)
}

// A grant that fails does not keep the pass from the next waiter or from the
// waiter gauge; the pass returns the failure.
func TestLeaseWaker_AFailedGrantDoesNotStopThePass(t *testing.T) {
	gone := uuid.New()
	r := runIn(run.RunStatusCancelling)
	leases := &fakeLeases{waiters: 2, grantable: []repository.LeaseWaiter{
		{TenantID: pkgevents.DefaultTenantID, TaskID: uuid.New(), RunID: gone, NodeKey: "e2e_schema.table_a", WaitingSince: now.Add(-time.Minute)},
		{TenantID: pkgevents.DefaultTenantID, TaskID: uuid.New(), RunID: r.RunID(), NodeKey: "e2e_schema.table_b", WaitingSince: now.Add(-time.Minute)},
	}}
	tasks := &unreadTasks{}
	f := &uow.FakeUnitOfWork{LeaseRepo: leases}
	f.SetRunRepo(&runs{held: map[uuid.UUID]*run.Run{r.RunID(): r}})
	f.SetTaskCollection(tasks)
	f.SetOutboxPublisher(&recordingOutbox{})
	f.SetClock(fixedClock{})
	metrics := &recordingMetrics{}

	err := leasewaker.New(func() uow.UnitOfWork { return f }, defaults, metrics, discard()).Pass(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), gone.String())
	assert.Equal(t, 2, f.BeginCalled, "the second waiter got its transaction")
	assert.Equal(t, 1, f.RollbackCalled)
	assert.Equal(t, []int{2}, metrics.waiters)
}

// Each repaired lease is counted.
func TestLeaseWaker_CountsEachRepairedLease(t *testing.T) {
	leases := &fakeLeases{repaired: []repository.RepairedLease{
		{TenantID: pkgevents.DefaultTenantID, TaskID: uuid.New(), RunID: uuid.New(), NodeKey: "e2e_schema.table_a", TaskStatus: run.TaskStatusSucceeded, AcquiredAt: now.Add(-time.Hour)},
		{TenantID: pkgevents.DefaultTenantID, TaskID: uuid.New(), RunID: uuid.New(), NodeKey: "e2e_schema.table_b", TaskStatus: run.TaskStatusCancelled, AcquiredAt: now.Add(-time.Hour)},
	}}
	metrics := &recordingMetrics{}
	f := &uow.FakeUnitOfWork{LeaseRepo: leases}

	require.NoError(t, leasewaker.New(func() uow.UnitOfWork { return f }, defaults, metrics, discard()).Pass(context.Background()))

	assert.Equal(t, 2, metrics.repaired)
	assert.Equal(t, []int{0}, metrics.waiters)
}

// A pass acts on the waiters of every tenant the scan lists, each under its
// own tenant: the waiter's run is loaded, and its tasks and waits are read,
// with the tenant the waiter names. Here both runs stopped running after the
// scan, so neither is granted.
func TestLeaseWaker_ActsOnEachWaiterUnderItsTenant(t *testing.T) {
	ours, theirs := runOf("default", run.RunStatusCancelling), runOf("acme", run.RunStatusCancelling)
	leases := &fakeLeases{grantable: []repository.LeaseWaiter{
		{TenantID: "default", TaskID: uuid.New(), RunID: ours.RunID(), NodeKey: "e2e_schema.table_a", WaitingSince: now.Add(-time.Minute)},
		{TenantID: "acme", TaskID: uuid.New(), RunID: theirs.RunID(), NodeKey: "e2e_schema.table_a", WaitingSince: now.Add(-time.Minute)},
	}}
	f := &uow.FakeUnitOfWork{LeaseRepo: leases}
	f.SetRunRepo(&runs{held: map[uuid.UUID]*run.Run{ours.RunID(): ours, theirs.RunID(): theirs}})
	f.SetTaskCollection(&unreadTasks{})
	f.SetOutboxPublisher(&recordingOutbox{})
	f.SetClock(fixedClock{})

	require.NoError(t, leasewaker.New(func() uow.UnitOfWork { return f }, defaults, &recordingMetrics{}, discard()).Pass(context.Background()),
		"each waiter's run is found under its own tenant")

	assert.Equal(t, []string{"default", "acme"}, f.TaskCollectionTenants)
	assert.Equal(t, "acme", f.GraphTenant)
}

// runOf returns a run of tenantID in status that writes its nodes.
func runOf(tenantID string, status run.RunStatus) *run.Run {
	started := now.Add(-time.Hour)
	total := int32(1)
	return run.HydrateRun(tenantID, uuid.New(), "single-node-run-91c2ab07", status, run.KindSingleNodeRun, nil, "system",
		model.OperationRun, started, &started, nil, &started, nil, nil, nil, &total, 0)
}
