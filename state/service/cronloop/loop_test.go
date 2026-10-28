package cronloop_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/schedule"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/cronloop"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/runstart"
	"github.com/carolsimone/continuo/state/service/uow"
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

// storedSchedule is one row of scheduleRows: the fields the schedules table
// stores. broken marks a row whose stored cron no longer parses, which LockDue
// leaves out and EarliestFire still counts, as the Postgres adapter does.
type storedSchedule struct {
	tenantID, name, cron, timezone, description string
	paused                                      bool
	next                                        *time.Time
	version                                     int64
	updatedBy                                   string
	updatedAt                                   time.Time
	expr                                        schedule.Expression
	broken                                      bool
}

// scheduleRows is an in-memory ScheduleRepository over the rows a test stores.
// Every read rebuilds the aggregate from the row, so what a caller changes on
// an aggregate stays its own until it saves it.
type scheduleRows struct {
	repository.ScheduleRepository
	mu      sync.Mutex
	rows    map[string]*storedSchedule
	lockDue int
	gets    map[string]int
	saves   int
	failGet map[string]error
}

func newScheduleRows() *scheduleRows {
	return &scheduleRows{rows: map[string]*storedSchedule{}, gets: map[string]int{}, failGet: map[string]error{}}
}

// add stores a schedule last edited by alice@example.com at version 3 whose next
// fire is next.
func (r *scheduleRows) add(tenantID, name, cron string, expr schedule.Expression, next time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := next
	r.rows[key(tenantID, name)] = &storedSchedule{
		tenantID: tenantID, name: name, cron: cron, timezone: "Europe/Paris", next: &n,
		version: 3, updatedBy: "alice@example.com", updatedAt: nov(1, 9, 0), expr: expr,
	}
}

func (r *scheduleRows) row(tenantID, name string) storedSchedule {
	r.mu.Lock()
	defer r.mu.Unlock()
	return *r.rows[key(tenantID, name)]
}

func (r *scheduleRows) setNext(tenantID, name string, next time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := next
	r.rows[key(tenantID, name)].next = &n
}

func (r *scheduleRows) lockDueCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lockDue
}

func (s *storedSchedule) aggregate() *schedule.Schedule {
	return schedule.Rehydrate(s.tenantID, s.name, s.cron, s.timezone, s.description, s.paused, s.next,
		s.version, s.updatedBy, s.updatedAt, s.expr)
}

func (r *scheduleRows) LockDue(_ context.Context, now time.Time, limit int) ([]*schedule.Schedule, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lockDue++
	var due []*storedSchedule
	for _, s := range r.rows {
		if s.next != nil && !s.next.After(now) {
			due = append(due, s)
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if !due[i].next.Equal(*due[j].next) {
			return due[i].next.Before(*due[j].next)
		}
		return key(due[i].tenantID, due[i].name) < key(due[j].tenantID, due[j].name)
	})
	if len(due) > limit {
		due = due[:limit]
	}
	out := []*schedule.Schedule{}
	for _, s := range due {
		if !s.broken {
			out = append(out, s.aggregate())
		}
	}
	return out, nil
}

func (r *scheduleRows) EarliestFire(context.Context) (time.Time, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var earliest *time.Time
	for _, s := range r.rows {
		if s.next != nil && (earliest == nil || s.next.Before(*earliest)) {
			earliest = s.next
		}
	}
	if earliest == nil {
		return time.Time{}, false, nil
	}
	return *earliest, true, nil
}

func (r *scheduleRows) GetForUpdate(_ context.Context, tenantID, name string) (*schedule.Schedule, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := key(tenantID, name)
	r.gets[k]++
	if err := r.failGet[k]; err != nil {
		return nil, false, err
	}
	s, ok := r.rows[k]
	if !ok {
		return nil, false, nil
	}
	return s.aggregate(), true, nil
}

// Save writes s while its row is at expectedVersion, as the adapter does; 0
// names no row, and every row of these tests exists.
func (r *scheduleRows) Save(_ context.Context, s *schedule.Schedule, expectedVersion int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.saves++
	row, ok := r.rows[key(s.TenantID(), s.Name())]
	if !ok || row.version != expectedVersion {
		return schedule.ErrStaleVersion
	}
	row.cron, row.timezone, row.description, row.paused = s.Cron(), s.Timezone(), s.Description(), s.Paused()
	row.version, row.updatedBy, row.updatedAt = s.Version(), s.UpdatedBy(), s.UpdatedAt()
	row.next = nil
	if next, ok := s.NextFireAt(); ok {
		row.next = &next
	}
	return nil
}

func (r *scheduleRows) Count(context.Context) (scheduled, paused int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.rows {
		if s.paused {
			paused++
		} else {
			scheduled++
		}
	}
	return scheduled, paused, nil
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

// ordersToRevenue is raw.orders feeding mart.revenue, both in schedule name.
func ordersToRevenue(name string) topology.Graph {
	node := func(schema, table string) topology.Node {
		return topology.Node{
			Key: topology.KeyOf(schema, table), ServiceName: "svc", SchemaName: schema, TableName: table,
			NodeType: "dbt-model", ContentHash: "h-" + table, ImageTag: "svc:1", ScheduleName: name,
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

// firesCounted records the cron fires counted by outcome and the last
// schedules gauge.
type firesCounted struct {
	mu                sync.Mutex
	fires             map[string]int
	scheduled, paused int
}

func (m *firesCounted) CronFire(outcome string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fires[outcome]++
}

func (m *firesCounted) Schedules(scheduled, paused int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.scheduled, m.paused = scheduled, paused
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
// whose daily and hourly schedules each hold raw.orders feeding mart.revenue.
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
	w := &world{
		clock:     &fixedClock{now: now},
		schedules: newScheduleRows(),
		runs:      &cronRuns{active: map[string]bool{}, taken: map[string]bool{}, raceOnce: map[string]bool{}},
		tasks:     &dispatchedTasks{},
		outbox:    &recordedEvents{},
		models:    &unaskedModels{},
		metrics:   &firesCounted{fires: map[string]int{}},
		logs:      &lockedBuffer{},
		u:         &uow.FakeUnitOfWork{},
	}
	w.u.ScheduleRepo = w.schedules
	w.u.TopologyRepo = &liveGraphs{
		LiveReleaseOnly: uow.LiveReleaseOnly{Live: topology.Pointer{TenantID: tenant, ReleaseID: "rel-1", PromotionSeq: 4}},
		graphs:          map[string]topology.Graph{"daily": ordersToRevenue("daily"), "hourly": ordersToRevenue("hourly")},
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
	row := w.schedules.row(tenant, name)
	if row.next == nil {
		return time.Time{}
	}
	return *row.next
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

	require.NoError(t, w.loop(false).Pass(ctx))

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

	row := w.schedules.row(tenant, "daily")
	assert.True(t, nov(3, 1, 0).Equal(*row.next), "next fire: got %s", row.next)
	assert.Equal(t, int64(3), row.version, "a fire leaves the version")
	assert.Equal(t, "alice@example.com", row.updatedBy)
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

	require.NoError(t, w.loop(false).Pass(ctx))
	require.NoError(t, w.loop(false).Pass(ctx))
	require.NoError(t, w.loop(false).Pass(ctx))

	assert.Len(t, w.runs.of(tenant, "daily"), 1)
	assert.Equal(t, 1, w.schedules.saves, "the passes after the fire write nothing")
	assert.Equal(t, map[string]int{"started": 1}, w.metrics.counted())
}

// state is down from 00:55 to 03:10: the loop that starts at 03:10 finds the
// 01:00 slot due and fires it once, late, and counts no missed slot.
func TestCronLoop_ARestartAcrossADueSlotFiresItOnceLate(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(nov(2, 3, 10))
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)

	require.NoError(t, w.loop(false).Pass(ctx))
	require.NoError(t, w.loop(false).Pass(ctx))

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

	require.NoError(t, w.loop(false).Pass(ctx))

	runs := w.runs.of(tenant, "hourly")
	require.Len(t, runs, 1)
	assert.True(t, nov(2, 3, 0).Equal(*runs[0].ScheduledFor()), "scheduled for: got %s", runs[0].ScheduledFor())
	assert.True(t, nov(2, 4, 0).Equal(w.nextFire("hourly")))
	assert.Equal(t, map[string]int{"started": 1, "missed": 2}, w.metrics.counted(), "missed counts slots")
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

	require.NoError(t, w.loop(true).Pass(ctx))

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

	require.NoError(t, w.loop(false).Pass(ctx))

	assert.Empty(t, w.runs.of(tenant, "daily"))
	assert.True(t, nov(3, 1, 0).Equal(w.nextFire("daily")), "the slot is consumed")
	assert.Equal(t, map[string]int{"skipped_active": 1}, w.metrics.counted())

	// The run ends; the next day's slot fires.
	delete(w.runs.active, key(tenant, "daily"))
	w.clock.set(nov(3, 1, 0))
	require.NoError(t, w.loop(false).Pass(ctx))

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

	require.NoError(t, w.loop(false).Pass(ctx))

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
}

// A slot that already has its run (uq_runs_scheduled_for) gets no second run;
// the slot is consumed, and no fire outcome is counted for it.
func TestCronLoop_ASlotThatHasItsRunIsConsumedWithoutASecondRun(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)
	w.runs.taken[slotKey(tenant, "daily", slot)] = true

	require.NoError(t, w.loop(false).Pass(ctx))

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

	require.NoError(t, w.loop(false).Pass(ctx))

	assert.Empty(t, w.runs.of(tenant, "daily"))
	assert.Zero(t, w.schedules.saves, "the fire wrote nothing")
	assert.True(t, slot.Equal(w.nextFire("daily")))
	assert.Equal(t, 1, w.u.RollbackCalled)

	require.NoError(t, w.loop(false).Pass(ctx))

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

	err := w.loop(false).Pass(ctx)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection reset")
	assert.Contains(t, err.Error(), "daily")
	assert.Equal(t, 1, w.schedules.gets[key(tenant, "daily")], "the failed row is tried once")
	assert.Len(t, w.runs.of(tenant, "hourly"), 1, "the other due row fires")
	assert.True(t, slot.Equal(w.nextFire("daily")), "the failed row keeps its slot for the next pass")
}

// Each pass sets the schedules gauge: the schedules that are scheduled and the
// ones that are paused, across tenants.
func TestCronLoop_GaugesTheScheduledAndPausedSchedules(t *testing.T) {
	ctx := context.Background()
	w := newWorld(nov(2, 0, 0))
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, nov(2, 1, 0))
	w.schedules.add("globex", "daily", "0 1 * * *", daily{1, 0, paris}, nov(2, 1, 0))
	w.schedules.add(tenant, "hourly", "0 * * * *", hourly{}, nov(2, 1, 0))
	w.schedules.rows[key(tenant, "hourly")].paused = true
	w.schedules.rows[key(tenant, "hourly")].next = nil

	require.NoError(t, w.loop(false).Pass(ctx))

	assert.Equal(t, 2, w.metrics.scheduled)
	assert.Equal(t, 1, w.metrics.paused)
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
	require.Eventually(t, func() bool { return w.schedules.lockDueCalls() >= 1 }, 5*time.Second, 10*time.Millisecond,
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

// A due row the pass cannot fire, such as one whose stored cron no longer
// parses, leaves the earliest next fire in the past. Run then waits a second
// between passes instead of passing again at once.
func TestCronLoop_AnUnparseableDueRowDoesNotMakeTheLoopSpin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := nov(2, 1, 0)
	w := newWorld(now)
	w.schedules.add(tenant, "broken", "not a cron", hourly{}, now.Add(-time.Hour))
	w.schedules.rows[key(tenant, "broken")].broken = true
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.loop(false).Run(ctx, nil, time.Hour)
	}()

	time.Sleep(1500 * time.Millisecond)
	cancel()
	<-done

	passes := w.schedules.lockDueCalls()
	assert.GreaterOrEqual(t, passes, 1)
	assert.LessOrEqual(t, passes, 3, "a pass a second at most, not a spin")
	assert.Empty(t, w.runs.of(tenant, "broken"))
	assert.True(t, strings.Count(w.logs.String(), "level=ERROR") == 0, "a row the pass cannot fire is not a pass failure")
}
