package cronloop_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/run-controller/adapters/cron"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/run-controller/domain/repository"
	"github.com/carolsimone/continuo/run-controller/domain/schedule"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/carolsimone/continuo/run-controller/service/cronloop"
	"github.com/carolsimone/continuo/run-controller/service/handlers"
	"github.com/carolsimone/continuo/run-controller/service/runstart"
	"github.com/carolsimone/continuo/run-controller/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// paris is the zone the schedules of these tests are read in.
var paris = func() *time.Location {
	loc, err := time.LoadLocation("Europe/Paris")
	if err != nil {
		panic(err)
	}
	return loc
}()

// nov is the instant of hour:minute in Paris on the given day of November 2026,
// a month whose clocks do not change.
func nov(day, hour, minute int) time.Time {
	return time.Date(2026, time.November, day, hour, minute, 0, 0, paris)
}

// daily fires every day at hour:minute in loc.
type daily struct {
	hour, minute int
	loc          *time.Location
}

func (d daily) Next(after time.Time) time.Time {
	local := after.In(d.loc)
	next := time.Date(local.Year(), local.Month(), local.Day(), d.hour, d.minute, 0, 0, d.loc)
	if !next.After(after) {
		next = time.Date(local.Year(), local.Month(), local.Day()+1, d.hour, d.minute, 0, 0, d.loc)
	}
	return next
}

// hourly fires at the start of every hour.
type hourly struct{}

func (hourly) Next(after time.Time) time.Time { return after.Truncate(time.Hour).Add(time.Hour) }

// fixedClock is a clock a test moves by hand.
type fixedClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fixedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fixedClock) set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

func key(tenantID, name string) string { return tenantID + "/" + name }

// scheduleRows is the shared uow.FakeScheduleRepository with two hooks on
// LoadForUpdate: failGet fails the load of one schedule, and a schedule in
// stuck waits for the load's context to end, as a fire waiting on a lock does.
// It counts the loads of each schedule (gets) and keeps the deadline of the
// context each was last loaded with. A schedule is added with an expression of
// its own, so the tests drive it with the daily and hourly expressions above; a
// broken one is added with a cron the parser refuses.
type scheduleRows struct {
	*uow.FakeScheduleRepository
	mu      sync.Mutex
	gets    map[string]int
	failGet map[string]error
	// deadlines holds the deadline of the context each schedule's row was
	// last loaded with.
	deadlines map[string]time.Time
	stuck     map[string]bool
	// loadHook, when set, runs at the start of every load for update: a test
	// uses it to let time pass during a fire.
	loadHook func(tenantID, name string)
}

func newScheduleRows(inTx func() bool) *scheduleRows {
	parser, err := cron.NewParser()
	if err != nil {
		panic(err)
	}
	return &scheduleRows{
		FakeScheduleRepository: uow.NewFakeScheduleRepository(parser, inTx),
		gets:                   map[string]int{}, failGet: map[string]error{},
		deadlines: map[string]time.Time{}, stuck: map[string]bool{},
	}
}

func (r *scheduleRows) deadlineOf(tenantID, name string) (time.Time, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.deadlines[key(tenantID, name)]
	return d, ok
}

func (r *scheduleRows) setStuck(tenantID, name string, stuck bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stuck[key(tenantID, name)] = stuck
}

// getsOf is how many times tenantID's schedule name was loaded for update.
func (r *scheduleRows) getsOf(tenantID, name string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.gets[key(tenantID, name)]
}

// add stores a schedule last edited by alice@example.com at version 3 whose next
// fire is next.
func (r *scheduleRows) add(tenantID, name, cron string, expr schedule.Expression, next time.Time) {
	n := next
	r.Put(uow.StoredSchedule{
		TenantID: tenantID, Name: name, Cron: cron, Timezone: "Europe/Paris", NextFireAt: &n,
		Version: 3, UpdatedBy: "alice@example.com", UpdatedAt: nov(1, 9, 0), Expression: expr,
	})
}

// addBroken stores a schedule whose cron no longer parses and whose next fire
// is next.
func (r *scheduleRows) addBroken(tenantID, name string, next time.Time) {
	n := next
	r.Put(uow.StoredSchedule{
		TenantID: tenantID, Name: name, Cron: "not a cron", Timezone: "Europe/Paris", NextFireAt: &n,
		Version: 3, UpdatedBy: "alice@example.com", UpdatedAt: nov(1, 9, 0),
	})
}

// pause pauses the schedule, which has no next fire then.
func (r *scheduleRows) pause(tenantID, name string) {
	r.Update(tenantID, name, func(row *uow.StoredSchedule) {
		row.Paused = true
		row.NextFireAt = nil
	})
}

func (r *scheduleRows) setNext(tenantID, name string, next time.Time) {
	n := next
	r.Update(tenantID, name, func(row *uow.StoredSchedule) { row.NextFireAt = &n })
}

// edit makes an operator's edit of the schedule, which raises its version.
func (r *scheduleRows) edit(tenantID, name string) {
	r.Update(tenantID, name, func(row *uow.StoredSchedule) { row.Version++ })
}

func (r *scheduleRows) LoadForUpdate(ctx context.Context, tenantID, name string) (*schedule.Schedule, bool, error) {
	if r.loadHook != nil {
		r.loadHook(tenantID, name)
	}
	k := key(tenantID, name)
	r.mu.Lock()
	r.gets[k]++
	if d, ok := ctx.Deadline(); ok {
		r.deadlines[k] = d
	}
	stuck := r.stuck[k]
	failure := r.failGet[k]
	r.mu.Unlock()
	if stuck {
		<-ctx.Done()
		return nil, false, ctx.Err()
	}
	if failure != nil {
		return nil, false, failure
	}
	return r.FakeScheduleRepository.LoadForUpdate(ctx, tenantID, name)
}

// cronRuns stores the runs the loop creates. active names the schedules whose
// run another caller started and that is still active; taken holds the slots
// that already have their run, as uq_runs_scheduled_for does; a schedule in
// raceOnce loses its next insert to a run another caller starts at that
// moment.
type cronRuns struct {
	repository.RunRepository
	mu       sync.Mutex
	saved    []*run.Run
	active   map[string]bool
	taken    map[string]bool
	raceOnce map[string]bool
	created  chan *run.Run
}

func slotKey(tenantID, name string, slot time.Time) string {
	return key(tenantID, name) + "@" + slot.UTC().Format(time.RFC3339)
}

func (r *cronRuns) HasActiveRun(_ context.Context, tenantID, name string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active[key(tenantID, name)] {
		return true, nil
	}
	for _, s := range r.saved {
		if s.TenantID() == tenantID && s.ScheduleName() == name && s.IsActive() {
			return true, nil
		}
	}
	return false, nil
}

func (r *cronRuns) SaveRun(_ context.Context, rn *run.Run) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !rn.Changes().IsCreated() {
		rn.ResetChanges()
		return nil
	}
	k := key(rn.TenantID(), rn.ScheduleName())
	if r.raceOnce[k] {
		delete(r.raceOnce, k)
		r.active[k] = true
		return run.ErrScheduleHasActiveRun
	}
	if slot := rn.ScheduledFor(); slot != nil {
		if r.taken[slotKey(rn.TenantID(), rn.ScheduleName(), *slot)] {
			return run.ErrSlotTaken
		}
		r.taken[slotKey(rn.TenantID(), rn.ScheduleName(), *slot)] = true
	}
	r.saved = append(r.saved, rn)
	rn.ResetChanges()
	if r.created != nil {
		select {
		case r.created <- rn:
		default:
		}
	}
	return nil
}

func (r *cronRuns) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.saved)
}

func (r *cronRuns) of(tenantID, name string) []*run.Run {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*run.Run
	for _, s := range r.saved {
		if s.TenantID() == tenantID && s.ScheduleName() == name {
			out = append(out, s)
		}
	}
	return out
}

// dispatchedTasks stores a new run's tasks; every ready task gets its node's
// lease and is dispatched.
type dispatchedTasks struct {
	run.TaskCollection
	created []run.Task
}

func (t *dispatchedTasks) BulkCreate(_ context.Context, tasks []run.Task) error {
	t.created = append(t.created, tasks...)
	return nil
}
func (t *dispatchedTasks) AddAttempts(context.Context, []run.Attempt) error { return nil }
func (t *dispatchedTasks) TakeLeases(_ context.Context, claims []run.LeaseClaim) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, 0, len(claims))
	for _, c := range claims {
		ids = append(ids, c.TaskID)
	}
	return ids, nil
}
func (t *dispatchedTasks) MarkDispatched(_ context.Context, ids []uuid.UUID, _ time.Time) ([]uuid.UUID, error) {
	return ids, nil
}

// liveGraphs serves the live release's graph of each schedule; a schedule it
// holds no graph for has no node in the release.
type liveGraphs struct {
	uow.LiveReleaseOnly
	graphs map[string]topology.Graph
}

func (l *liveGraphs) ScheduleGraph(_ context.Context, _, _, scheduleName string) (topology.Graph, error) {
	return l.graphs[scheduleName], nil
}

// ordersToRevenue is raw.orders feeding mart.revenue, both dbt models in
// schedule name.
func ordersToRevenue(name string) topology.Graph { return ordersToRevenueOfType(name, "dbt-model") }

// ordersToRevenueOfType is raw.orders feeding mart.revenue, both of nodeType,
// in schedule name.
func ordersToRevenueOfType(name, nodeType string) topology.Graph {
	node := func(schema, table string) topology.Node {
		return topology.Node{
			Key: topology.KeyOf(schema, table), ServiceName: "svc", SchemaName: schema, TableName: table,
			NodeType: nodeType, ContentHash: "h-" + table, ImageTag: "svc:1", ScheduleName: name,
		}
	}
	return topology.NewGraph(
		[]topology.Node{node("raw", "orders"), node("mart", "revenue")},
		[]topology.Edge{{Upstream: "raw.orders", Downstream: "mart.revenue"}},
	)
}

type recordedEvents struct {
	mu     sync.Mutex
	events []run.DomainEvent
}

func (o *recordedEvents) Append(_ context.Context, events []run.DomainEvent, _ uuid.UUID) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, events...)
	return nil
}

func (o *recordedEvents) dispatched() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	var keys []string
	for _, e := range o.events {
		if d, ok := e.(run.TaskDispatched); ok {
			keys = append(keys, string(d.Node.Key))
		}
	}
	return keys
}

// unaskedModels counts the questions a fire asks of the live release's model
// counts: a schedule row is what authorises a fire, so there are none.
type unaskedModels struct {
	repository.LiveScheduleModels
	reads int
}

func (m *unaskedModels) CountOf(context.Context, string, string) (int, error) {
	m.reads++
	return 1, nil
}

// firesCounted records the cron fires counted by outcome, the calls that
// counted them, and the last schedules gauge.
type firesCounted struct {
	mu                            sync.Mutex
	fires                         map[string]int
	calls                         map[string]int
	scheduled, paused, unparsable int
}

func (m *firesCounted) CronFire(outcome string) { m.CronFires(outcome, 1) }

func (m *firesCounted) CronFires(outcome string, n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fires[outcome] += n
	m.calls[outcome]++
}

// callsOf is how many calls counted fires of outcome.
func (m *firesCounted) callsOf(outcome string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[outcome]
}

func (m *firesCounted) Schedules(scheduled, paused, unparsable int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.scheduled, m.paused, m.unparsable = scheduled, paused, unparsable
}

func (m *firesCounted) counted() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]int{}
	for k, v := range m.fires {
		out[k] = v
	}
	return out
}

func (*firesCounted) RunStalled()                   {}
func (*firesCounted) RunsCancelling(int, int)       {}
func (*firesCounted) RunCancelled(time.Duration)    {}
func (*firesCounted) LeaseWaiters(int)              {}
func (*firesCounted) LeaseGranted(time.Duration)    {}
func (*firesCounted) LeaseRepaired()                {}
func (*firesCounted) CommandRequest(string, string) {}

// lockedBuffer is a log sink a loop's goroutine and a test can share.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// world is everything one test's loop reads and writes, behind the fake unit
// of work every pass of the loop uses. Tenant acme's live release is rel-1,
// whose daily and hourly schedules each hold raw.orders feeding mart.revenue,
// and whose legacy schedule holds the same two nodes with a node type the
// node_type vocabulary does not declare.
type world struct {
	clock     *fixedClock
	schedules *scheduleRows
	runs      *cronRuns
	tasks     *dispatchedTasks
	outbox    *recordedEvents
	models    *unaskedModels
	metrics   *firesCounted
	logs      *lockedBuffer
	u         *uow.FakeUnitOfWork
}

const tenant = "acme"

func newWorld(now time.Time) *world {
	u := &uow.FakeUnitOfWork{}
	w := &world{
		clock:     &fixedClock{now: now},
		schedules: newScheduleRows(u.InTransaction),
		runs:      &cronRuns{active: map[string]bool{}, taken: map[string]bool{}, raceOnce: map[string]bool{}},
		tasks:     &dispatchedTasks{},
		outbox:    &recordedEvents{},
		models:    &unaskedModels{},
		metrics:   &firesCounted{fires: map[string]int{}, calls: map[string]int{}},
		logs:      &lockedBuffer{},
		u:         u,
	}
	w.u.ScheduleRepo = w.schedules
	w.u.TopologyRepo = &liveGraphs{
		LiveReleaseOnly: uow.LiveReleaseOnly{Live: topology.Pointer{TenantID: tenant, ReleaseID: "rel-1", PromotionSeq: 4}},
		graphs: map[string]topology.Graph{
			"daily": ordersToRevenue("daily"), "hourly": ordersToRevenue("hourly"),
			"legacy": ordersToRevenueOfType("legacy", "spreadsheet"),
		},
	}
	w.u.SetRunRepo(w.runs)
	w.u.SetTaskCollection(w.tasks)
	w.u.SetOutboxPublisher(w.outbox)
	w.u.LiveModels = w.models
	w.u.SetClock(w.clock)
	return w
}

// loop is a loop over w; with maintenance on it starts no run.
func (w *world) loop(maintenance bool) *cronloop.Loop {
	logger := slog.New(slog.NewTextHandler(w.logs, nil))
	starter := handlers.NewStartRunHandler(runstart.Planner{Tasks: run.TaskDefaults{MaxRetries: 2, Timeout: time.Hour}}, logger)
	return cronloop.New(func() uow.UnitOfWork { return w.u }, starter, w.metrics, maintenance, logger)
}

func (w *world) nextFire(name string) time.Time {
	row, _ := w.schedules.Row(tenant, name)
	if row.NextFireAt == nil {
		return time.Time{}
	}
	return *row.NextFireAt
}

// pass runs one pass of l at the world's now and requires it to succeed.
func (w *world) pass(t *testing.T, ctx context.Context, l *cronloop.Loop) {
	t.Helper()
	require.NoError(t, l.Pass(ctx, w.clock.Now()))
}

// The trace of a fire: the due slot fires once, as one run of kind cron for
// the slot, initiated by the system, of the row's tenant, on the live release,
// with its ready tasks dispatched; the next fire moves to the next slot, and
// the version and authorship the operator left stay. The schedules row
// authorises the fire, so the fire does not ask whether a model carries the
// name.
func TestCronLoop_FiresTheDueSlotOnceAndMovesTheNextFire(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)

	w.pass(t, ctx, w.loop(false))

	runs := w.runs.of(tenant, "daily")
	require.Len(t, runs, 1)
	r := runs[0]
	assert.Equal(t, run.KindCron, r.Kind())
	require.NotNil(t, r.ScheduledFor())
	assert.True(t, slot.Equal(*r.ScheduledFor()), "scheduled for: got %s", r.ScheduledFor())
	assert.Equal(t, identity.SystemUserID, r.InitiatedBy())
	assert.Equal(t, run.RunStatusRunning, r.Status())
	assert.Equal(t, "rel-1", r.ReleaseID())
	assert.Equal(t, []string{"raw.orders"}, w.outbox.dispatched(), "the ready task is dispatched in the fire")
	assert.Zero(t, w.models.reads, "a fire does not ask whether a model carries the name")

	row, _ := w.schedules.Row(tenant, "daily")
	assert.True(t, nov(3, 1, 0).Equal(*row.NextFireAt), "next fire: got %s", row.NextFireAt)
	assert.Equal(t, int64(3), row.Version, "a fire leaves the version")
	assert.Equal(t, "alice@example.com", row.UpdatedBy)
	assert.Equal(t, map[string]int{"started": 1}, w.metrics.counted())
	assert.Contains(t, w.logs.String(), "schedule_name=daily")
	assert.Contains(t, w.logs.String(), "tenant_id="+tenant)
}

// A second pass on the slot, from the same loop or another replica's, finds it
// fired: it makes no second run and writes nothing.
func TestCronLoop_TwoPassesOnOneSlotMakeOneRun(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)

	w.pass(t, ctx, w.loop(false))
	w.pass(t, ctx, w.loop(false))
	w.pass(t, ctx, w.loop(false))

	assert.Len(t, w.runs.of(tenant, "daily"), 1)
	assert.Equal(t, 1, w.schedules.SaveCalls(), "the passes after the fire write nothing")
	assert.Equal(t, map[string]int{"started": 1}, w.metrics.counted())
}

// state is down from 00:55 to 03:10: the loop that starts at 03:10 finds the
// 01:00 slot due and fires it once, late, and counts no missed slot.
func TestCronLoop_ARestartAcrossADueSlotFiresItOnceLate(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(nov(2, 3, 10))
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)

	w.pass(t, ctx, w.loop(false))
	w.pass(t, ctx, w.loop(false))

	runs := w.runs.of(tenant, "daily")
	require.Len(t, runs, 1)
	assert.True(t, slot.Equal(*runs[0].ScheduledFor()), "the late fire is of the slot it was due for")
	assert.True(t, nov(3, 1, 0).Equal(w.nextFire("daily")))
	assert.Equal(t, map[string]int{"started": 1}, w.metrics.counted())
}

// After the same outage an hourly cron fires its 03:00 slot only: the 01:00
// and 02:00 slots are logged and counted as missed.
func TestCronLoop_OnlyTheLatestMissedSlotFires(t *testing.T) {
	ctx := context.Background()
	w := newWorld(nov(2, 3, 10))
	w.schedules.add(tenant, "hourly", "0 * * * *", hourly{}, nov(2, 1, 0))

	w.pass(t, ctx, w.loop(false))

	runs := w.runs.of(tenant, "hourly")
	require.Len(t, runs, 1)
	assert.True(t, nov(2, 3, 0).Equal(*runs[0].ScheduledFor()), "scheduled for: got %s", runs[0].ScheduledFor())
	assert.True(t, nov(2, 4, 0).Equal(w.nextFire("hourly")))
	assert.Equal(t, map[string]int{"started": 1, "missed": 2}, w.metrics.counted(), "missed counts slots")
	assert.Equal(t, 1, w.metrics.callsOf("missed"), "the missed slots of a fire are counted in one call")
	logs := w.logs.String()
	assert.Contains(t, logs, "missed=2")
	assert.Contains(t, logs, "schedule_name=hourly")
}

// In maintenance mode a due slot is consumed with no run, counted and logged,
// and the schedule moves on to its next slot.
func TestCronLoop_MaintenanceConsumesTheSlot(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)

	w.pass(t, ctx, w.loop(true))

	assert.Empty(t, w.runs.of(tenant, "daily"))
	assert.True(t, nov(3, 1, 0).Equal(w.nextFire("daily")), "the slot is consumed")
	assert.Equal(t, map[string]int{"skipped_maintenance": 1}, w.metrics.counted())
	logs := w.logs.String()
	assert.Contains(t, logs, "maintenance")
	assert.Contains(t, logs, "schedule_name=daily")
}

// A slot due while the schedule's previous run is still active is consumed
// with no run; the next slot, once that run has ended, fires.
func TestCronLoop_AnActiveRunConsumesTheSlot(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)
	w.runs.active[key(tenant, "daily")] = true

	w.pass(t, ctx, w.loop(false))

	assert.Empty(t, w.runs.of(tenant, "daily"))
	assert.True(t, nov(3, 1, 0).Equal(w.nextFire("daily")), "the slot is consumed")
	assert.Equal(t, map[string]int{"skipped_active": 1}, w.metrics.counted())

	// The run ends; the next day's slot fires.
	delete(w.runs.active, key(tenant, "daily"))
	w.clock.set(nov(3, 1, 0))
	w.pass(t, ctx, w.loop(false))

	runs := w.runs.of(tenant, "daily")
	require.Len(t, runs, 1)
	assert.True(t, nov(3, 1, 0).Equal(*runs[0].ScheduledFor()))
	assert.Equal(t, map[string]int{"skipped_active": 1, "started": 1}, w.metrics.counted())
}

// A fire of a schedule no model of the live release belongs to records a
// failed run for the slot, with failure reason empty_selection.
func TestCronLoop_AScheduleWithNoModelsRecordsAFailedRun(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 3, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "weekly", "0 3 * * 1", daily{3, 0, paris}, slot)

	w.pass(t, ctx, w.loop(false))

	runs := w.runs.of(tenant, "weekly")
	require.Len(t, runs, 1)
	assert.Equal(t, run.RunStatusFailed, runs[0].Status())
	require.NotNil(t, runs[0].FailureReason())
	assert.Equal(t, run.RunFailureEmptySelection, *runs[0].FailureReason())
	assert.True(t, slot.Equal(*runs[0].ScheduledFor()))
	assert.Empty(t, w.outbox.dispatched())
	assert.False(t, w.nextFire("weekly").IsZero())
	assert.True(t, w.nextFire("weekly").After(slot), "the slot is consumed")
	assert.Equal(t, map[string]int{"empty_selection": 1}, w.metrics.counted())
	assert.Contains(t, w.logs.String(), "failure_reason=empty_selection")
}

// A fire of a schedule whose live graph holds a node of a type the node_type
// vocabulary does not declare records a failed run for the slot with failure
// reason invalid_node_type, and counts and logs that outcome, not
// empty_selection.
func TestCronLoop_AScheduleWithAnInvalidNodeTypeRecordsAFailedRun(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "legacy", "0 1 * * *", daily{1, 0, paris}, slot)

	w.pass(t, ctx, w.loop(false))

	runs := w.runs.of(tenant, "legacy")
	require.Len(t, runs, 1)
	assert.Equal(t, run.RunStatusFailed, runs[0].Status())
	require.NotNil(t, runs[0].FailureReason())
	assert.Equal(t, run.RunFailureInvalidNodeType, *runs[0].FailureReason())
	assert.Empty(t, w.outbox.dispatched())
	assert.True(t, nov(3, 1, 0).Equal(w.nextFire("legacy")), "the slot is consumed")
	assert.Equal(t, map[string]int{"invalid_node_type": 1}, w.metrics.counted())
	logs := w.logs.String()
	assert.Contains(t, logs, "failure_reason=invalid_node_type")
	assert.NotContains(t, logs, "empty_selection")
}

// A slot that already has its run (uq_runs_scheduled_for) gets no second run;
// the slot is consumed, and no fire outcome is counted for it.
func TestCronLoop_ASlotThatHasItsRunIsConsumedWithoutASecondRun(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)
	w.runs.taken[slotKey(tenant, "daily", slot)] = true

	w.pass(t, ctx, w.loop(false))

	assert.Empty(t, w.runs.of(tenant, "daily"))
	assert.Empty(t, w.outbox.dispatched())
	assert.True(t, nov(3, 1, 0).Equal(w.nextFire("daily")), "the slot is consumed")
	assert.Empty(t, w.metrics.counted())
	assert.Contains(t, w.logs.String(), "already has its run")
}

// A run another caller starts between the fire's check and its insert ends the
// fire's transaction: the pass writes nothing and reports no failure, and the
// next pass finds that run active and consumes the slot.
func TestCronLoop_ARunStartedDuringTheFireIsSkippedOnTheNextPass(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)
	w.runs.raceOnce[key(tenant, "daily")] = true

	w.pass(t, ctx, w.loop(false))

	assert.Empty(t, w.runs.of(tenant, "daily"))
	assert.Zero(t, w.schedules.SaveCalls(), "the fire wrote nothing")
	assert.True(t, slot.Equal(w.nextFire("daily")))
	assert.Equal(t, 1, w.u.RollbackCalled)

	w.pass(t, ctx, w.loop(false))

	assert.True(t, nov(3, 1, 0).Equal(w.nextFire("daily")), "the next pass consumes the slot")
	assert.Equal(t, map[string]int{"skipped_active": 1}, w.metrics.counted())
}

// A row whose fire fails does not stop the others, and is tried once in a
// pass: the pass returns its failure.
func TestCronLoop_ARowThatFailsDoesNotStopTheOthers(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)
	w.schedules.add(tenant, "hourly", "0 * * * *", hourly{}, slot)
	w.schedules.failGet[key(tenant, "daily")] = errors.New("connection reset")

	err := w.loop(false).Pass(ctx, w.clock.Now())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection reset")
	assert.Contains(t, err.Error(), "daily")
	assert.Equal(t, 1, w.schedules.getsOf(tenant, "daily"), "the failed row is tried once")
	assert.Len(t, w.runs.of(tenant, "hourly"), 1, "the other due row fires")
	assert.True(t, slot.Equal(w.nextFire("daily")), "the failed row keeps its slot for the next pass")
	assert.Equal(t, map[string]int{"failed": 1, "started": 1}, w.metrics.counted())
}

// A fire that fails is counted as failed once in each pass that tries it: the
// row keeps its slot, so a pass after its backoff tries it again and counts it
// again.
func TestCronLoop_AFailedFireIsCountedOncePerPass(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)
	w.schedules.failGet[key(tenant, "daily")] = errors.New("connection reset")
	loop := w.loop(false)

	require.Error(t, loop.Pass(ctx, w.clock.Now()))
	assert.Equal(t, map[string]int{"failed": 1}, w.metrics.counted(), "the first pass counts its failed fire once")

	require.Error(t, loop.Pass(ctx, w.clock.Now().Add(cronloop.FailureBackoffBase)))
	assert.Equal(t, map[string]int{"failed": 2}, w.metrics.counted(), "the pass after the backoff tries the row again and counts it again")
	assert.Equal(t, 2, w.metrics.callsOf("failed"))
	assert.True(t, slot.Equal(w.nextFire("daily")), "the failed row keeps its slot")
}

// A row whose fire fails is not tried again until its backoff has passed: the
// first retry waits FailureBackoffBase, each failure doubles the wait, and a
// pass that skips the row counts nothing and fails nothing.
func TestCronLoop_AFailingRowWaitsForItsBackoffBeforeItIsTriedAgain(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)
	w.schedules.failGet[key(tenant, "daily")] = errors.New("connection reset")
	loop := w.loop(false)
	gets := func() int { return w.schedules.getsOf(tenant, "daily") }
	base := cronloop.FailureBackoffBase

	require.Error(t, loop.Pass(ctx, slot))
	assert.Equal(t, 1, gets())

	require.NoError(t, loop.Pass(ctx, slot.Add(base-time.Second)), "a pass inside the backoff skips the row")
	assert.Equal(t, 1, gets(), "the row is not tried inside its backoff")
	assert.Equal(t, map[string]int{"failed": 1}, w.metrics.counted(), "a skipped row is not a failed fire")

	require.Error(t, loop.Pass(ctx, slot.Add(base)), "the pass at the end of the backoff tries the row")
	assert.Equal(t, 2, gets())

	require.NoError(t, loop.Pass(ctx, slot.Add(3*base-time.Second)), "each failure doubles the wait: 2 bases after the second")
	assert.Equal(t, 2, gets())
	require.Error(t, loop.Pass(ctx, slot.Add(3*base)))
	assert.Equal(t, 3, gets())
	assert.Equal(t, map[string]int{"failed": 3}, w.metrics.counted())
}

// The wait between tries never exceeds FailureBackoffMax, however many fires of
// the row have failed.
func TestCronLoop_TheBackoffOfAFailingRowIsCapped(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)
	w.schedules.failGet[key(tenant, "daily")] = errors.New("connection reset")
	loop := w.loop(false)

	for i := range 12 {
		now := slot.Add(time.Duration(i) * cronloop.FailureBackoffMax)
		require.Error(t, loop.Pass(ctx, now), "try %d, a whole FailureBackoffMax after the one before", i+1)
	}
	assert.Equal(t, 12, w.schedules.getsOf(tenant, "daily"))
}

// A row in its backoff takes none of a pass's DueLimit places: the pass reads
// the due rows with the places of the rows it will skip added to its limit, so
// DueLimit rows that can fire do fire.
func TestCronLoop_AFailingRowInItsBackoffTakesNoneOfTheDueLimit(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "aaa-failing", "0 1 * * *", daily{1, 0, paris}, slot.Add(-time.Hour))
	w.schedules.failGet[key(tenant, "aaa-failing")] = errors.New("connection reset")
	for i := range 2*cronloop.DueLimit + 50 {
		w.schedules.add(tenant, fmt.Sprintf("s%03d", i), "0 1 * * *", daily{1, 0, paris}, slot)
	}
	loop := w.loop(false)

	require.Error(t, loop.Pass(ctx, slot))
	assert.Equal(t, cronloop.DueLimit-1, w.runs.count(), "the failing row, oldest, took a place of the first pass")

	require.NoError(t, loop.Pass(ctx, slot.Add(time.Second)))
	assert.Equal(t, 2*cronloop.DueLimit-1, w.runs.count(), "in its backoff it takes none of the second pass's places")
}

// An operator's edit raises the schedule's version and ends its backoff: the
// next pass tries the row at once.
func TestCronLoop_AnEditEndsTheBackoffOfAFailingRow(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)
	w.schedules.failGet[key(tenant, "daily")] = errors.New("connection reset")
	loop := w.loop(false)
	require.Error(t, loop.Pass(ctx, slot))

	delete(w.schedules.failGet, key(tenant, "daily"))
	w.schedules.edit(tenant, "daily")

	require.NoError(t, loop.Pass(ctx, slot.Add(time.Second)))
	assert.Len(t, w.runs.of(tenant, "daily"), 1, "the edited row fires inside the old backoff")
}

// A fire that succeeds ends the row's backoff: a later failure of the row waits
// the base again, not the doubled wait it had reached.
func TestCronLoop_ASuccessfulFireEndsTheBackoffOfAFailingRow(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)
	w.schedules.failGet[key(tenant, "daily")] = errors.New("connection reset")
	loop := w.loop(false)
	base := cronloop.FailureBackoffBase
	require.Error(t, loop.Pass(ctx, slot))
	require.Error(t, loop.Pass(ctx, slot.Add(base)))

	delete(w.schedules.failGet, key(tenant, "daily"))
	require.NoError(t, loop.Pass(ctx, slot.Add(3*base)))
	require.Len(t, w.runs.of(tenant, "daily"), 1)

	nextSlot := nov(3, 1, 0)
	w.schedules.failGet[key(tenant, "daily")] = errors.New("connection reset")
	before := w.schedules.getsOf(tenant, "daily")
	require.Error(t, loop.Pass(ctx, nextSlot))
	require.Error(t, loop.Pass(ctx, nextSlot.Add(base)), "the wait after a recovery starts from the base again")
	assert.Equal(t, before+2, w.schedules.getsOf(tenant, "daily"))
}

// A schedule's backoff is counted from the moment its failed fire ended, not
// from the start of the pass: a fire that took 30 seconds to fail leaves the
// whole base wait after that, whatever instant the pass began at.
func TestCronLoop_TheBackoffStartsWhenTheFailedFireEnds(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)
	w.schedules.failGet[key(tenant, "daily")] = errors.New("connection reset")
	w.schedules.loadHook = func(string, string) { w.clock.set(slot.Add(30 * time.Second)) }
	loop := w.loop(false)
	base := cronloop.FailureBackoffBase

	require.Error(t, loop.Pass(ctx, slot))
	assert.Equal(t, 1, w.schedules.getsOf(tenant, "daily"))

	require.NoError(t, loop.Pass(ctx, slot.Add(base+10*time.Second)),
		"the fire failed at +30 s, so the base wait runs to +90 s, not to +60 s")
	assert.Equal(t, 1, w.schedules.getsOf(tenant, "daily"), "not tried inside the wait counted from the failure's end")

	w.schedules.loadHook = nil
	require.Error(t, loop.Pass(ctx, slot.Add(30*time.Second+base)))
	assert.Equal(t, 2, w.schedules.getsOf(tenant, "daily"), "tried once the wait from the failure's end has passed")
}

// A schedule the pass did not read keeps its failure history: past DueLimit the
// read leaves the later schedules out, and absence from a read says nothing of
// whether a schedule recovered, so the delay it earned is not forgotten.
func TestCronLoop_AScheduleOutsideTheDuePageKeepsItsFailureHistory(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	rows := cronloop.DueLimit + 1
	for i := range rows {
		name := fmt.Sprintf("s%03d", i)
		w.schedules.add(tenant, name, "0 1 * * *", daily{1, 0, paris}, slot)
		w.schedules.failGet[key(tenant, name)] = errors.New("connection reset")
	}
	last := fmt.Sprintf("s%03d", rows-1)
	loop := w.loop(false)
	base := cronloop.FailureBackoffBase

	_ = loop.Pass(ctx, slot)                          // the first DueLimit rows fail; the last is not read
	_ = loop.Pass(ctx, slot.Add(10*time.Second))      // the rows in backoff are skipped; the last is tried and fails once
	_ = loop.Pass(ctx, slot.Add(base+10*time.Second)) // every backoff has ended: the read holds the first DueLimit rows, not the last
	assert.Equal(t, 1, w.schedules.getsOf(tenant, last), "the last row was tried once so far")

	_ = loop.Pass(ctx, slot.Add(base+20*time.Second)) // the last row is tried a second time and fails
	assert.Equal(t, 2, w.schedules.getsOf(tenant, last))

	_ = loop.Pass(ctx, slot.Add(2*base+20*time.Second))
	assert.Equal(t, 2, w.schedules.getsOf(tenant, last),
		"a second failure earns two bases: a history forgotten while it sat outside the read would start again at one")
}

// A fire that ends because the loop's context ended, as at shutdown, is not a
// failed fire: the pass returns its error and counts nothing.
func TestCronLoop_AFireEndedByShutdownIsNotCounted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)
	w.schedules.setStuck(tenant, "daily", true)
	loop := w.loop(false)
	go func() {
		// The fire waits on its row's lock once its deadline is recorded.
		for {
			if _, ok := w.schedules.deadlineOf(tenant, "daily"); ok {
				cancel()
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	err := loop.Pass(ctx, w.clock.Now())

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, w.metrics.counted(), "a fire the shutdown ended is not counted")
	assert.True(t, slot.Equal(w.nextFire("daily")), "the fire keeps its slot")
}

// Each pass sets the schedules gauge: the schedules that are scheduled, the ones
// that are paused and the ones whose stored cron no longer parses, across
// tenants.
func TestCronLoop_GaugesTheScheduledPausedAndUnparsableSchedules(t *testing.T) {
	ctx := context.Background()
	w := newWorld(nov(2, 0, 0))
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, nov(2, 1, 0))
	w.schedules.add("globex", "daily", "0 1 * * *", daily{1, 0, paris}, nov(2, 1, 0))
	w.schedules.add(tenant, "hourly", "0 * * * *", hourly{}, nov(2, 1, 0))
	w.schedules.pause(tenant, "hourly")
	w.schedules.addBroken(tenant, "garbled", nov(2, 1, 0))

	w.pass(t, ctx, w.loop(false))

	assert.Equal(t, 2, w.metrics.scheduled)
	assert.Equal(t, 1, w.metrics.paused)
	assert.Equal(t, 1, w.metrics.unparsable)
}

// Run sleeps until the earliest next fire or the tick; a wake signal, which
// the schedules channel's notification sends, makes it pass at once.
func TestCronLoop_WakesOnANotification(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := nov(2, 1, 0)
	w := newWorld(now)
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, nov(3, 1, 0))
	w.runs.created = make(chan *run.Run, 1)
	wake := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.loop(false).Run(ctx, wake, time.Hour)
	}()
	require.Eventually(t, func() bool { return w.schedules.ListDueCalls() >= 1 }, 5*time.Second, 10*time.Millisecond,
		"the loop passes when it starts")

	// An edit makes the schedule due now and notifies.
	w.schedules.setNext(tenant, "daily", now)
	wake <- struct{}{}

	select {
	case r := <-w.runs.created:
		assert.Equal(t, "daily", r.ScheduleName())
		assert.True(t, now.Equal(*r.ScheduledFor()))
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not pass on the wake signal")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return once its context ended")
	}
}

// wallClock is the real clock, for a test of how long Run sleeps.
type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// With nothing due, Run sleeps until the earliest next fire rather than its
// tick: a slot due 50 ms after the loop starts fires at about 50 ms, with no
// wake signal and an hour-long tick. However slowly the first pass runs, the
// slot fires on time: a slot that comes due between a pass and its sleep
// makes the loop pass again at once.
func TestCronLoop_SleepsUntilTheEarliestFire(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	slot := start.Add(50 * time.Millisecond)
	w := newWorld(start)
	w.u.SetClock(wallClock{})
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)
	w.runs.created = make(chan *run.Run, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.loop(false).Run(ctx, nil, time.Hour)
	}()

	select {
	case r := <-w.runs.created:
		elapsed := time.Since(start)
		assert.True(t, slot.Equal(*r.ScheduledFor()), "the run is of the slot")
		assert.GreaterOrEqual(t, elapsed, 50*time.Millisecond, "the slot fires once it is due")
		assert.Less(t, elapsed, 900*time.Millisecond, "the loop slept until the slot, not a second or its tick")
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not fire the slot it slept until")
	}
	cancel()
	<-done
}

// A due row no pass can fire, here one whose stored cron no longer parses,
// does not hold up the other schedules: a good row due 50 ms after the loop
// starts fires at about 50 ms, with no wake signal and an hour-long tick, and
// the broken row makes the loop pass no more than that. The good row fires
// daily twelve hours from the hour of its slot, so its next fire after the
// slot is hours away and none falls in the time the test runs.
func TestCronLoop_ABrokenDueRowDoesNotDelayTheOthers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	slot := start.Add(50 * time.Millisecond)
	w := newWorld(start)
	w.u.SetClock(wallClock{})
	w.schedules.addBroken(tenant, "broken", start.Add(-time.Hour))
	farHour := (slot.In(paris).Hour() + 12) % 24
	w.schedules.add(tenant, "daily", fmt.Sprintf("0 %d * * *", farHour), daily{farHour, 0, paris}, slot)
	w.runs.created = make(chan *run.Run, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.loop(false).Run(ctx, nil, time.Hour)
	}()

	select {
	case r := <-w.runs.created:
		assert.Equal(t, "daily", r.ScheduleName())
		assert.Less(t, time.Since(start), 900*time.Millisecond, "the good row fired when it was due, not at the tick")
	case <-time.After(5 * time.Second):
		t.Fatal("the broken row held up the good row's fire")
	}
	time.Sleep(300 * time.Millisecond)
	// The count is an upper bound. A first clock read 50 ms or more after
	// start, as on a loaded machine, finds the good row due in the first pass
	// (one call). Otherwise the first pass finds only the broken row and the
	// pass at the slot fires the good one (two calls). A loop that passed
	// again and again on the broken row would make hundreds of calls in the
	// 300 ms; a broken row that delayed the good one fails the wait above.
	assert.LessOrEqual(t, w.schedules.ListDueCalls(), 2, "at most a pass at the start and one for the good row; the broken row adds none")
	assert.Empty(t, w.runs.of(tenant, "broken"))
	cancel()
	<-done
}

// steppedClock reads before at its first call and after at every call after
// it.
type steppedClock struct {
	mu            sync.Mutex
	read          bool
	before, after time.Time
}

func (c *steppedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.read {
		c.read = true
		return c.before
	}
	return c.after
}

// A slot that comes due after a pass has read its instant, and before the
// loop reads the clock to sleep, fires at once: the pass ran just before the
// slot, and the sleep starts just after it.
func TestCronLoop_ASlotDueBetweenAPassAndItsSleepFiresAtOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.u.SetClock(&steppedClock{before: slot.Add(-time.Millisecond), after: slot.Add(time.Millisecond)})
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)
	w.runs.created = make(chan *run.Run, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.loop(false).Run(ctx, nil, time.Hour)
	}()

	select {
	case r := <-w.runs.created:
		assert.True(t, slot.Equal(*r.ScheduledFor()))
	case <-time.After(5 * time.Second):
		t.Fatal("the slot that came due after the pass was left to the tick")
	}
	cancel()
	<-done
}

// Each fire runs with a deadline of its own, FireTimeout after it starts, so
// a fire that waits on a lock cannot hold up the rest of the pass for longer.
func TestCronLoop_EachFireHasADeadlineOfItsOwn(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)
	w.schedules.add(tenant, "hourly", "0 * * * *", hourly{}, slot)
	before := time.Now()

	w.pass(t, ctx, w.loop(false))

	after := time.Now()
	assert.Equal(t, 30*time.Second, cronloop.FireTimeout)
	for _, name := range []string{"daily", "hourly"} {
		deadline, ok := w.schedules.deadlineOf(tenant, name)
		require.True(t, ok, "%s: the fire has a deadline", name)
		assert.False(t, deadline.Before(before.Add(cronloop.FireTimeout)), "%s: deadline %s", name, deadline)
		assert.False(t, deadline.After(after.Add(cronloop.FireTimeout)), "%s: deadline %s", name, deadline)
	}
}

// A fire still waiting when its deadline comes, as one waiting for the
// topology lock while a promotion holds it, rolls back and keeps its slot; the
// fires after it in the pass go on, and a later pass fires the slot.
func TestCronLoop_AFireThatRunsOutOfTimeRollsBackAndLeavesTheOthers(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)
	w.schedules.add(tenant, "hourly", "0 * * * *", hourly{}, slot)
	w.schedules.setStuck(tenant, "daily", true)
	loop := w.loop(false)
	cronloop.SetFireTimeout(loop, 50*time.Millisecond)

	started := time.Now()
	err := loop.Pass(ctx, w.clock.Now())

	require.Error(t, err)
	assert.Less(t, time.Since(started), time.Second, "the stuck fire held the pass up for its timeout only")
	assert.Contains(t, err.Error(), `schedule "daily" of tenant "acme" did not end within 50ms`)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Len(t, w.runs.of(tenant, "hourly"), 1, "the fire after the stuck one goes on")
	assert.Empty(t, w.runs.of(tenant, "daily"))
	assert.True(t, slot.Equal(w.nextFire("daily")), "the stuck fire keeps its slot")
	assert.Equal(t, 1, w.u.RollbackCalled, "the stuck fire rolled back")
	assert.Equal(t, map[string]int{"failed": 1, "started": 1}, w.metrics.counted(), "a fire that runs out of time is a failed fire")

	w.schedules.setStuck(tenant, "daily", false)
	require.NoError(t, loop.Pass(ctx, slot.Add(cronloop.FailureBackoffBase)))
	runs := w.runs.of(tenant, "daily")
	require.Len(t, runs, 1)
	assert.True(t, slot.Equal(*runs[0].ScheduledFor()))
}

// The schedules past DueLimit are left due by a pass, and the next pass fires
// them at once: the fires the first pass committed changed the schedules, and
// the notification of that change wakes the loop. Then, with nothing left due,
// it sleeps.
func TestCronLoop_TheSchedulesPastDueLimitFireOnTheWakeOfThePassBefore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	for i := range cronloop.DueLimit + 1 {
		w.schedules.add(tenant, fmt.Sprintf("s%03d", i), "0 1 * * *", daily{1, 0, paris}, slot)
	}
	changed := make(chan struct{}, 1)
	w.schedules.NotifyOnChange(changed)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.loop(false).Run(ctx, changed, time.Hour)
	}()

	require.Eventually(t, func() bool { return w.runs.count() == cronloop.DueLimit+1 }, 5*time.Second, 20*time.Millisecond,
		"every due row fires")
	require.Eventually(t, func() bool { return w.schedules.ListDueCalls() == 3 }, 5*time.Second, 10*time.Millisecond,
		"the last fire's change wakes one more pass")
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, 3, w.schedules.ListDueCalls(), "a pass of DueLimit, one for the rest, one that finds nothing, then sleep")
	cancel()
	<-done
}

// More due rows whose stored cron no longer parses than DueLimit, all older
// than a good row, take none of the pass's places: the pass fires the good row,
// and the broken rows start nothing.
func TestCronLoop_UnparseableRowsPastDueLimitDoNotStarveTheOthers(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	for i := range cronloop.DueLimit + 1 {
		name := fmt.Sprintf("broken-%03d", i)
		w.schedules.addBroken(tenant, name, slot.Add(-time.Duration(cronloop.DueLimit+1-i)*time.Minute))
	}
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)

	w.pass(t, ctx, w.loop(false))

	runs := w.runs.of(tenant, "daily")
	require.Len(t, runs, 1, "the good row fires in the first pass")
	assert.True(t, slot.Equal(*runs[0].ScheduledFor()))
	assert.Equal(t, 1, w.runs.count(), "the broken rows start nothing")
	assert.Equal(t, map[string]int{"started": 1}, w.metrics.counted())
}

// A due row the pass cannot fire, such as one whose stored cron no longer
// parses, keeps a next fire at or before the pass, which does not count toward
// the next fire Run sleeps until. With no other schedule Run waits its tick,
// rather than passing again and again: in a second and a half it passes once.
func TestCronLoop_AnUnparseableDueRowDoesNotMakeTheLoopSpin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := nov(2, 1, 0)
	w := newWorld(now)
	w.schedules.addBroken(tenant, "broken", now.Add(-time.Hour))
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.loop(false).Run(ctx, nil, time.Hour)
	}()

	time.Sleep(1500 * time.Millisecond)
	cancel()
	<-done

	assert.Equal(t, 1, w.schedules.ListDueCalls(), "the pass at the start, then nothing until the tick")
	assert.Empty(t, w.runs.of(tenant, "broken"))
	assert.True(t, strings.Count(w.logs.String(), "level=ERROR") == 0, "a row the pass cannot fire is not a pass failure")
}
