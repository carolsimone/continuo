package cronloop_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
// stores. broken marks a row whose stored cron no longer parses, which ListDue
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
	listDue int
	gets    map[string]int
	saves   int
	failGet map[string]error
	// deadlines holds the deadline of the context each schedule's row was
	// last loaded with; a row in stuck waits for that context to end, as a
	// fire waiting on a lock does.
	deadlines map[string]time.Time
	stuck     map[string]bool
}

func newScheduleRows() *scheduleRows {
	return &scheduleRows{
		rows: map[string]*storedSchedule{}, gets: map[string]int{}, failGet: map[string]error{},
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

func (r *scheduleRows) listDueCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.listDue
}

func (s *storedSchedule) aggregate() *schedule.Schedule {
	return schedule.Rehydrate(s.tenantID, s.name, s.cron, s.timezone, s.description, s.paused, s.next,
		s.version, s.updatedBy, s.updatedAt, s.expr)
}

func (r *scheduleRows) ListDue(_ context.Context, now time.Time, limit int) ([]*schedule.Schedule, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listDue++
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

func (r *scheduleRows) LoadForUpdate(ctx context.Context, tenantID, name string) (*schedule.Schedule, bool, error) {
	k := key(tenantID, name)
	r.mu.Lock()
	r.gets[k]++
	if d, ok := ctx.Deadline(); ok {
		r.deadlines[k] = d
	}
	stuck := r.stuck[k]
	r.mu.Unlock()
	if stuck {
		<-ctx.Done()
		return nil, false, ctx.Err()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
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
	mu                sync.Mutex
	fires             map[string]int
	calls             map[string]int
	scheduled, paused int
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
	w := &world{
		clock:     &fixedClock{now: now},
		schedules: newScheduleRows(),
		runs:      &cronRuns{active: map[string]bool{}, taken: map[string]bool{}, raceOnce: map[string]bool{}},
		tasks:     &dispatchedTasks{},
		outbox:    &recordedEvents{},
		models:    &unaskedModels{},
		metrics:   &firesCounted{fires: map[string]int{}, calls: map[string]int{}},
		logs:      &lockedBuffer{},
		u:         &uow.FakeUnitOfWork{},
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
	row := w.schedules.row(tenant, name)
	if row.next == nil {
		return time.Time{}
	}
	return *row.next
}

// pass runs one pass of l, requires it to succeed and returns how many
// schedules it fired.
func pass(t *testing.T, ctx context.Context, l *cronloop.Loop) int {
	t.Helper()
	fired, err := l.Pass(ctx)
	require.NoError(t, err)
	return fired
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

	assert.Equal(t, 1, pass(t, ctx, w.loop(false)), "the pass fired one schedule")

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

	assert.Equal(t, 1, pass(t, ctx, w.loop(false)))
	assert.Zero(t, pass(t, ctx, w.loop(false)), "a slot already fired is not fired again")
	assert.Zero(t, pass(t, ctx, w.loop(false)))

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

	pass(t, ctx, w.loop(false))
	pass(t, ctx, w.loop(false))

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

	pass(t, ctx, w.loop(false))

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

	pass(t, ctx, w.loop(true))

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

	pass(t, ctx, w.loop(false))

	assert.Empty(t, w.runs.of(tenant, "daily"))
	assert.True(t, nov(3, 1, 0).Equal(w.nextFire("daily")), "the slot is consumed")
	assert.Equal(t, map[string]int{"skipped_active": 1}, w.metrics.counted())

	// The run ends; the next day's slot fires.
	delete(w.runs.active, key(tenant, "daily"))
	w.clock.set(nov(3, 1, 0))
	pass(t, ctx, w.loop(false))

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

	pass(t, ctx, w.loop(false))

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

	pass(t, ctx, w.loop(false))

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

	pass(t, ctx, w.loop(false))

	assert.Empty(t, w.runs.of(tenant, "daily"))
	assert.Empty(t, w.outbox.dispatched())
	assert.True(t, nov(3, 1, 0).Equal(w.nextFire("daily")), "the slot is consumed")
	assert.Empty(t, w.metrics.counted())
	assert.Contains(t, w.logs.String(), "already has its run")
}

// A run another caller starts between the fire's check and its insert ends the
// fire's transaction: the pass writes nothing and reports no failure, and the
// next pass finds that run active and consumes the slot. The rolled-back fire
// counts as fired, since the next pass can consume its slot.
func TestCronLoop_ARunStartedDuringTheFireIsSkippedOnTheNextPass(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)
	w.runs.raceOnce[key(tenant, "daily")] = true

	assert.Equal(t, 1, pass(t, ctx, w.loop(false)))

	assert.Empty(t, w.runs.of(tenant, "daily"))
	assert.Zero(t, w.schedules.saves, "the fire wrote nothing")
	assert.True(t, slot.Equal(w.nextFire("daily")))
	assert.Equal(t, 1, w.u.RollbackCalled)

	pass(t, ctx, w.loop(false))

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

	fired, err := w.loop(false).Pass(ctx)

	require.Error(t, err)
	assert.Equal(t, 1, fired, "the row that did not fail is fired")
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

	pass(t, ctx, w.loop(false))

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
	require.Eventually(t, func() bool { return w.schedules.listDueCalls() >= 1 }, 5*time.Second, 10*time.Millisecond,
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
// wake signal and an hour-long tick.
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

// Each fire runs with a deadline of its own, FireTimeout after it starts, so
// a fire that waits on a lock cannot hold up the rest of the pass for longer.
func TestCronLoop_EachFireHasADeadlineOfItsOwn(t *testing.T) {
	ctx := context.Background()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)
	w.schedules.add(tenant, "hourly", "0 * * * *", hourly{}, slot)
	before := time.Now()

	assert.Equal(t, 2, pass(t, ctx, w.loop(false)))

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
	fired, err := loop.Pass(ctx)

	require.Error(t, err)
	assert.Less(t, time.Since(started), time.Second, "the stuck fire held the pass up for its timeout only")
	assert.Contains(t, err.Error(), `schedule "daily" of tenant "acme" did not end within 50ms`)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 1, fired, "the fire after the stuck one goes on")
	assert.Len(t, w.runs.of(tenant, "hourly"), 1)
	assert.Empty(t, w.runs.of(tenant, "daily"))
	assert.True(t, slot.Equal(w.nextFire("daily")), "the stuck fire keeps its slot")
	assert.Equal(t, 1, w.u.RollbackCalled, "the stuck fire rolled back")

	w.schedules.setStuck(tenant, "daily", false)
	assert.Equal(t, 1, pass(t, ctx, loop), "a later pass fires the slot")
	runs := w.runs.of(tenant, "daily")
	require.Len(t, runs, 1)
	assert.True(t, slot.Equal(*runs[0].ScheduledFor()))
}

// A schedule left due by a pass that fired others, here one past DueLimit, is
// fired a second later, not at the tick.
func TestCronLoop_ARowLeftDueByAPassThatFiredIsFiredASecondLater(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	for i := range cronloop.DueLimit + 1 {
		w.schedules.add(tenant, fmt.Sprintf("s%03d", i), "0 1 * * *", daily{1, 0, paris}, slot)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.loop(false).Run(ctx, nil, time.Hour)
	}()

	require.Eventually(t, func() bool { return w.runs.count() == cronloop.DueLimit+1 }, 5*time.Second, 20*time.Millisecond,
		"every due row fires")
	assert.Equal(t, 2, w.schedules.listDueCalls(), "a pass of DueLimit, then a second for the rest")
	cancel()
	<-done
}

// A fire that a run started at that moment rolls back leaves its slot for the
// next pass, which Run makes a second later, not at the tick.
func TestCronLoop_ARolledBackFireIsConsumedASecondLater(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slot := nov(2, 1, 0)
	w := newWorld(slot)
	w.schedules.add(tenant, "daily", "0 1 * * *", daily{1, 0, paris}, slot)
	w.runs.raceOnce[key(tenant, "daily")] = true
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.loop(false).Run(ctx, nil, time.Hour)
	}()

	require.Eventually(t, func() bool { return w.metrics.counted()["skipped_active"] == 1 }, 5*time.Second, 20*time.Millisecond,
		"the next pass consumes the slot")
	assert.True(t, nov(3, 1, 0).Equal(w.nextFire("daily")))
	cancel()
	<-done
}

// A due row the pass cannot fire, such as one whose stored cron no longer
// parses, leaves the earliest next fire in the past. A pass that fired nothing
// cannot fire it either, so Run waits its tick rather than passing again a
// second later: in a second and a half it passes once.
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

	assert.Equal(t, 1, w.schedules.listDueCalls(), "the pass at the start, then nothing until the tick")
	assert.Empty(t, w.runs.of(tenant, "broken"))
	assert.True(t, strings.Count(w.logs.String(), "level=ERROR") == 0, "a row the pass cannot fire is not a pass failure")
}
