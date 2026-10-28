package test

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/identity"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/state/adapters/cron"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/cronloop"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/uow"
)

// The cron loop reads the schedules of every tenant. Each test here works in a
// tenant of its own, asserts only on that tenant's rows, deletes them when it
// ends, and passes until its own schedule has fired rather than counting on a
// single pass. Every instant is in March 2026, before the real time, so the
// database's own clock never runs behind a time the loop writes.

// paris is the zone the schedules of these tests are read in.
var paris = func() *time.Location {
	loc, err := time.LoadLocation("Europe/Paris")
	if err != nil {
		panic(err)
	}
	return loc
}()

// march is the instant of hour:minute in Paris on the given day of March 2026,
// before the clocks change on the 29th.
func march(day, hour, minute int) time.Time {
	return time.Date(2026, time.March, day, hour, minute, 0, 0, paris)
}

// cronClock is the clock a tenant's loop reads; a test moves it by hand.
type cronClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *cronClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *cronClock) set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

// syncBuffer is a log sink a loop's goroutine and a test can share.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// cronMetrics counts the cron fires by outcome and the passes that set the
// schedules gauge.
type cronMetrics struct {
	noRunMetrics
	mu     sync.Mutex
	fires  map[string]int
	passes int
}

func (m *cronMetrics) CronFire(outcome string) { m.CronFires(outcome, 1) }

func (m *cronMetrics) CronFires(outcome string, n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fires[outcome] += n
}

func (m *cronMetrics) Schedules(int, int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.passes++
}

func (m *cronMetrics) passCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.passes
}

func (m *cronMetrics) counted(outcome string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fires[outcome]
}

// cronTenant is a tenant of its own whose live release holds one model,
// raw.<schedule>, in each schedule it is made with. Its loops read its clock.
type cronTenant struct {
	id      string
	clock   *cronClock
	parser  ports.CronParser
	logs    *syncBuffer
	logger  *slog.Logger
	metrics *cronMetrics
}

func newCronTenant(t *testing.T, schedules ...string) *cronTenant {
	t.Helper()
	specs := make([]topology.NodeSpec, 0, len(schedules))
	for _, s := range schedules {
		specs = append(specs, scheduledNode("raw", strings.ReplaceAll(s, "-", "_"), "dbt-model", s))
	}
	return newCronTenantOf(t, specs...)
}

// newCronTenantOf is a tenant of its own whose live release holds specs.
func newCronTenantOf(t *testing.T, specs ...topology.NodeSpec) *cronTenant {
	t.Helper()
	parser, err := cron.NewParser()
	require.NoError(t, err)
	logs := &syncBuffer{}
	c := &cronTenant{
		id: "cron-" + uuid.NewString(), clock: &cronClock{now: march(1, 0, 0)}, parser: parser,
		logs: logs, logger: slog.New(slog.NewTextHandler(logs, nil)), metrics: &cronMetrics{fires: map[string]int{}},
	}
	t.Cleanup(func() { c.cleanup() })

	releaseID := "rel-" + uuid.NewString()[:8]
	rel, err := topology.NewRelease(c.id, releaseID, 1,
		"s3://continuo/tenants/"+c.id+"/topologies/"+releaseID+"/topology.json.gz", "sha-"+releaseID, specs)
	require.NoError(t, err)
	ctx := context.Background()
	tx, err := stateDB.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback() //nolint:errcheck
	topo := postgres.NewTopologyRepository(stateDB, tx)
	require.NoError(t, topo.LockTopology(ctx))
	_, err = topo.SaveRelease(ctx, rel)
	require.NoError(t, err)
	require.NoError(t, topo.SetLivePointer(ctx, c.id, topology.Pointer{ReleaseID: releaseID, PromotionSeq: 1}, time.Now()))
	require.NoError(t, tx.Commit())
	return c
}

// cleanup deletes every row of the tenant.
func (c *cronTenant) cleanup() {
	ctx := context.Background()
	_, _ = stateDB.ExecContext(ctx, `DELETE FROM state_outbox WHERE aggregate_id IN (SELECT run_id FROM runs WHERE tenant_id = $1)`, c.id)
	for _, table := range []string{"runs", "node_leases", "schedules", "live_release", "topology_releases"} {
		_, _ = stateDB.ExecContext(ctx, `DELETE FROM `+table+` WHERE tenant_id = $1`, c.id)
	}
}

// factory returns a unit of work that reads the tenant's clock.
func (c *cronTenant) factory() uow.UnitOfWork {
	return postgres.NewPostgresUnitOfWork(stateDB, c.clock, c.logger, postgres.WithCronParser(c.parser))
}

// loop is a cron loop over state's database; with maintenance on it starts no
// run.
func (c *cronTenant) loop(maintenance bool) *cronloop.Loop {
	return cronloop.New(c.factory, svchandlers.NewStartRunHandler(defaultPlanner, c.logger), c.metrics, maintenance, c.logger)
}

// schedule stores the tenant's schedule of cron in Paris, at version 1, whose
// next fire is next.
func (c *cronTenant) schedule(t *testing.T, name, cronExpr string, next time.Time) {
	t.Helper()
	_, err := stateDB.Exec(`
		INSERT INTO schedules (tenant_id, name, cron, timezone, next_fire_at, updated_by, updated_at)
		VALUES ($1, $2, $3, 'Europe/Paris', $4, 'alice@example.com', $5)`, c.id, name, cronExpr, next, march(1, 0, 0))
	require.NoError(t, err)
}

// scheduleRow is the stored state of one of the tenant's schedules.
type scheduleRow struct {
	Cron       string     `db:"cron"`
	NextFireAt *time.Time `db:"next_fire_at"`
	Version    int64      `db:"version"`
	UpdatedBy  string     `db:"updated_by"`
}

func (c *cronTenant) row(t *testing.T, name string) scheduleRow {
	t.Helper()
	var r scheduleRow
	require.NoError(t, stateDB.Get(&r, `SELECT cron, next_fire_at, version, updated_by FROM schedules WHERE tenant_id = $1 AND name = $2`, c.id, name))
	return r
}

// nextFire is the stored next fire of the tenant's schedule; the zero time when
// it has none.
func (c *cronTenant) nextFire(t *testing.T, name string) time.Time {
	t.Helper()
	r := c.row(t, name)
	if r.NextFireAt == nil {
		return time.Time{}
	}
	return *r.NextFireAt
}

// cronRun is one stored run of the tenant's schedule.
type cronRun struct {
	RunID         uuid.UUID      `db:"run_id"`
	Kind          string         `db:"kind"`
	Status        string         `db:"status"`
	ScheduledFor  *time.Time     `db:"scheduled_for"`
	InitiatedBy   string         `db:"initiated_by"`
	ReleaseID     sql.NullString `db:"release_id"`
	FailureReason sql.NullString `db:"failure_reason"`
}

func (c *cronTenant) runs(t *testing.T, name string) []cronRun {
	t.Helper()
	var out []cronRun
	require.NoError(t, stateDB.Select(&out, `
		SELECT run_id, kind, status, scheduled_for, initiated_by, release_id, failure_reason
		  FROM runs WHERE tenant_id = $1 AND schedule_name = $2 ORDER BY scheduled_for, created_at`, c.id, name))
	return out
}

// passUntil passes loop until done holds, for at most ten seconds: another
// pass can hold a row for a moment, and a pass leaves a row it finds locked
// for the next one.
func (c *cronTenant) passUntil(t *testing.T, loop *cronloop.Loop, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		require.NoError(t, loop.Pass(context.Background(), c.clock.Now()))
		if done() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the loop never fired the tenant's due schedule")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// movedFrom is whether the schedule's next fire is no longer before.
func (c *cronTenant) movedFrom(t *testing.T, name string, before time.Time) func() bool {
	return func() bool { return !c.nextFire(t, name).Equal(before) }
}

// The trace of a fire: at 01:00 the daily slot fires as one run of kind cron,
// scheduled for 01:00, initiated by the system, on the tenant's live release,
// with its ready task dispatched; the next fire moves to tomorrow 01:00 and the
// version and authorship the operator left stay.
func TestCronLoop_FiresTheDueSlotOnceAndMovesTheNextFire(t *testing.T) {
	c := newCronTenant(t, "daily")
	slot := march(10, 1, 0)
	c.clock.set(slot)
	c.schedule(t, "daily", "0 1 * * *", slot)

	c.passUntil(t, c.loop(false), c.movedFrom(t, "daily", slot))

	runs := c.runs(t, "daily")
	require.Len(t, runs, 1)
	r := runs[0]
	assert.Equal(t, "cron", r.Kind)
	assert.Equal(t, "running", r.Status)
	require.NotNil(t, r.ScheduledFor)
	assert.True(t, slot.Equal(*r.ScheduledFor), "scheduled for: got %s", r.ScheduledFor)
	assert.Equal(t, identity.SystemUserID, r.InitiatedBy)
	assert.True(t, r.ReleaseID.Valid, "the run names the live release")
	assert.Equal(t, 1, countOf(t, stateDB, `SELECT count(*) FROM state_outbox WHERE aggregate_id = $1`, r.RunID),
		"the ready task's dispatch is in the outbox")

	row := c.row(t, "daily")
	require.NotNil(t, row.NextFireAt)
	assert.True(t, march(11, 1, 0).Equal(*row.NextFireAt), "next fire: got %s", row.NextFireAt)
	assert.Equal(t, int64(1), row.Version, "a fire leaves the version")
	assert.Equal(t, "alice@example.com", row.UpdatedBy)
	assert.Equal(t, 1, c.metrics.counted("started"))
}

// Two passes that find the same slot due at once, a restarted loop's and
// another replica's, serialise on the schedule's row: one run is made.
func TestCronLoop_TwoPassesOnOneSlotMakeOneRun(t *testing.T) {
	c := newCronTenant(t, "daily")
	slot := march(10, 1, 0)
	c.clock.set(slot)
	c.schedule(t, "daily", "0 1 * * *", slot)

	start := make(chan struct{})
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			errs <- c.loop(false).Pass(context.Background(), c.clock.Now())
		}()
	}
	close(start)
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
	c.passUntil(t, c.loop(false), c.movedFrom(t, "daily", slot))

	runs := c.runs(t, "daily")
	require.Len(t, runs, 1)
	assert.True(t, slot.Equal(*runs[0].ScheduledFor))
	assert.True(t, march(11, 1, 0).Equal(c.nextFire(t, "daily")))
}

// state is down from 00:55 to 03:10: the loop that starts at 03:10 finds the
// 01:00 slot due and fires it once, late; another pass after it makes nothing.
func TestCronLoop_ARestartAcrossADueSlotFiresItOnceLate(t *testing.T) {
	c := newCronTenant(t, "daily")
	slot := march(10, 1, 0)
	c.clock.set(march(10, 3, 10))
	c.schedule(t, "daily", "0 1 * * *", slot)

	c.passUntil(t, c.loop(false), c.movedFrom(t, "daily", slot))
	require.NoError(t, c.loop(false).Pass(context.Background(), c.clock.Now()))

	runs := c.runs(t, "daily")
	require.Len(t, runs, 1)
	assert.True(t, slot.Equal(*runs[0].ScheduledFor), "the late fire is of the slot it was due for")
	assert.True(t, march(11, 1, 0).Equal(c.nextFire(t, "daily")))
	assert.Zero(t, c.metrics.counted("missed"))
}

// After the same outage an hourly cron fires its 03:00 slot only; the 01:00
// and 02:00 slots are counted and logged as missed, and get no run.
func TestCronLoop_OnlyTheLatestMissedSlotFires(t *testing.T) {
	c := newCronTenant(t, "hourly")
	c.clock.set(march(10, 3, 10))
	c.schedule(t, "hourly", "0 * * * *", march(10, 1, 0))

	c.passUntil(t, c.loop(false), c.movedFrom(t, "hourly", march(10, 1, 0)))

	runs := c.runs(t, "hourly")
	require.Len(t, runs, 1)
	assert.True(t, march(10, 3, 0).Equal(*runs[0].ScheduledFor), "scheduled for: got %s", runs[0].ScheduledFor)
	assert.True(t, march(10, 4, 0).Equal(c.nextFire(t, "hourly")))
	assert.Equal(t, 2, c.metrics.counted("missed"))
	assert.Contains(t, c.logs.String(), "missed=2")
}

// In maintenance mode the due slot is consumed with no run.
func TestCronLoop_MaintenanceConsumesTheSlot(t *testing.T) {
	c := newCronTenant(t, "daily")
	slot := march(10, 1, 0)
	c.clock.set(slot)
	c.schedule(t, "daily", "0 1 * * *", slot)

	c.passUntil(t, c.loop(true), c.movedFrom(t, "daily", slot))

	assert.Empty(t, c.runs(t, "daily"))
	assert.True(t, march(11, 1, 0).Equal(c.nextFire(t, "daily")))
	assert.Equal(t, 1, c.metrics.counted("skipped_maintenance"))
	assert.Contains(t, c.logs.String(), "maintenance")
}

// The slot due while the schedule's previous run is still running is consumed
// with no run; the slot after it, once that run has ended, fires.
func TestCronLoop_AnActiveRunConsumesTheSlot(t *testing.T) {
	c := newCronTenant(t, "daily")
	c.clock.set(march(10, 1, 0))
	c.schedule(t, "daily", "0 1 * * *", march(10, 1, 0))
	c.passUntil(t, c.loop(false), c.movedFrom(t, "daily", march(10, 1, 0)))
	first := c.runs(t, "daily")
	require.Len(t, first, 1)

	c.clock.set(march(11, 1, 0))
	c.passUntil(t, c.loop(false), c.movedFrom(t, "daily", march(11, 1, 0)))

	assert.Len(t, c.runs(t, "daily"), 1, "the slot of an active schedule gets no run")
	assert.True(t, march(12, 1, 0).Equal(c.nextFire(t, "daily")))
	assert.Equal(t, 1, c.metrics.counted("skipped_active"))

	_, err := stateDB.Exec(`UPDATE runs SET status = 'succeeded', completed_at = now() WHERE run_id = $1`, first[0].RunID)
	require.NoError(t, err)
	releaseLeasesOf(t, stateDB, first[0].RunID)
	c.clock.set(march(12, 1, 0))
	c.passUntil(t, c.loop(false), c.movedFrom(t, "daily", march(12, 1, 0)))

	runs := c.runs(t, "daily")
	require.Len(t, runs, 2)
	assert.True(t, march(12, 1, 0).Equal(*runs[1].ScheduledFor))
	assert.Equal(t, "running", runs[1].Status)
}

// A fire of weekly, a schedule with a row whose tag no model of the live
// release carries, records a failed run for the slot with failure reason
// empty_selection.
func TestCronLoop_AScheduleWithNoModelsRecordsAFailedRun(t *testing.T) {
	c := newCronTenant(t, "daily")
	slot := march(9, 3, 0)
	c.clock.set(slot)
	c.schedule(t, "weekly", "0 3 * * 1", slot)

	c.passUntil(t, c.loop(false), c.movedFrom(t, "weekly", slot))

	runs := c.runs(t, "weekly")
	require.Len(t, runs, 1)
	assert.Equal(t, "failed", runs[0].Status)
	assert.Equal(t, "empty_selection", runs[0].FailureReason.String)
	assert.True(t, slot.Equal(*runs[0].ScheduledFor))
	assert.True(t, march(16, 3, 0).Equal(c.nextFire(t, "weekly")))
	assert.Equal(t, 1, c.metrics.counted("empty_selection"))
}

// A fire of legacy, whose model in the live release has a node type the
// node_type vocabulary does not declare, records a failed run for the slot
// with failure reason invalid_node_type, and counts that outcome rather than
// empty_selection.
func TestCronLoop_AScheduleWithAnInvalidNodeTypeRecordsAFailedRun(t *testing.T) {
	c := newCronTenantOf(t, scheduledNode("raw", "legacy", "spreadsheet", "legacy"))
	slot := march(10, 1, 0)
	c.clock.set(slot)
	c.schedule(t, "legacy", "0 1 * * *", slot)

	c.passUntil(t, c.loop(false), c.movedFrom(t, "legacy", slot))

	runs := c.runs(t, "legacy")
	require.Len(t, runs, 1)
	assert.Equal(t, "failed", runs[0].Status)
	assert.Equal(t, "invalid_node_type", runs[0].FailureReason.String)
	assert.True(t, slot.Equal(*runs[0].ScheduledFor))
	assert.True(t, march(11, 1, 0).Equal(c.nextFire(t, "legacy")))
	assert.Equal(t, 1, c.metrics.counted("invalid_node_type"))
	assert.Zero(t, c.metrics.counted("empty_selection"))
	assert.Contains(t, c.logs.String(), "failure_reason=invalid_node_type")
}

// A row whose schedule name is longer than a run carries, which UpsertSchedule
// refuses but a row written by hand can hold, cannot fire: its fire fails with
// the run's refusal and rolls back, so no run is written and its slot stays
// due, and the pass counts the fire failed and fires the tenant's other due
// schedule.
func TestCronLoop_ANameLongerThanARunCarriesFailsItsFire(t *testing.T) {
	long := "cron-long-" + strings.Repeat("l", run.MaxScheduleNameChars+1-len("cron-long-"))
	c := newCronTenantOf(t,
		scheduledNode("raw", "daily", "dbt-model", "daily"),
		scheduledNode("raw", "long_tag", "dbt-model", long))
	slot := march(10, 1, 0)
	c.clock.set(slot)
	c.schedule(t, long, "0 1 * * *", slot)
	c.schedule(t, "daily", "0 1 * * *", slot)

	err := c.loop(false).Pass(context.Background(), c.clock.Now())

	require.ErrorIs(t, err, run.ErrScheduleNameTooLong)
	assert.Contains(t, err.Error(), long, "the pass's error names the schedule")
	assert.Empty(t, c.runs(t, long), "no run is written")
	assert.True(t, slot.Equal(c.nextFire(t, long)), "the slot stays due: next fire %s", c.nextFire(t, long))
	assert.Equal(t, 1, c.metrics.counted("failed"))
	require.Len(t, c.runs(t, "daily"), 1, "the pass fires the other due schedule")
	assert.Equal(t, 1, c.metrics.counted("started"))
}

// A slot that already has its run, which uq_runs_scheduled_for allows once,
// gets no second run, and the slot is consumed.
func TestCronLoop_ASlotThatHasItsRunIsConsumedWithoutASecondRun(t *testing.T) {
	c := newCronTenant(t, "daily")
	slot := march(10, 1, 0)
	c.clock.set(slot)
	existing := uuid.New()
	_, err := stateDB.Exec(`
		INSERT INTO runs (tenant_id, run_id, schedule_name, status, created_at, completed_at, kind, initiated_by, scheduled_for)
		VALUES ($1, $2, 'daily', 'succeeded', $3, now(), 'cron', 'system', $3)`, c.id, existing, slot)
	require.NoError(t, err)
	c.schedule(t, "daily", "0 1 * * *", slot)

	c.passUntil(t, c.loop(false), c.movedFrom(t, "daily", slot))

	runs := c.runs(t, "daily")
	require.Len(t, runs, 1)
	assert.Equal(t, existing, runs[0].RunID, "the slot keeps the run it had")
	assert.True(t, march(11, 1, 0).Equal(c.nextFire(t, "daily")))
	assert.Zero(t, c.metrics.counted("started"))
	assert.Contains(t, c.logs.String(), "already has its run")
}

// upsert edits the tenant's schedule as an operator does: it reads the row
// without a lock, replaces its cron counting from the tenant's now, and saves it
// at the version it read.
func (c *cronTenant) upsert(ctx context.Context, name, cronExpr string) error {
	u := c.factory()
	if err := u.Begin(ctx); err != nil {
		return err
	}
	defer u.Rollback() //nolint:errcheck
	s, _, err := u.Schedules().Get(ctx, c.id, name)
	if err != nil {
		return err
	}
	expr, err := c.parser.Parse(cronExpr, "Europe/Paris")
	if err != nil {
		return err
	}
	read := s.Version()
	s.Replace(cronExpr, "Europe/Paris", "", false, expr, "bob@example.com", c.clock.Now())
	if err := u.Schedules().Save(ctx, s, read); err != nil {
		return err
	}
	return u.Commit()
}

// waitForLockWaiters waits until n sessions of the database wait for a lock.
func waitForLockWaiters(t *testing.T, n int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return countOf(t, stateDB, `
			SELECT count(*) FROM pg_stat_activity
			 WHERE datname = current_database() AND wait_event_type = 'Lock'`) >= n
	}, 10*time.Second, 20*time.Millisecond, "%d sessions never waited for a lock", n)
}

// receive waits for the result on ch.
func receive(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(10 * time.Second):
		t.Fatalf("%s never finished", what)
		return nil
	}
}

// An operator saves the schedule while its slot fires. The fire holds the
// schedule's row from the moment it reads it: the save waits for the fire to
// commit, then writes the new cron over the row at the version it read, which
// the fire left as it was. The slot has its one run, and the next fire follows
// the saved cron from the time of the save.
func TestCronLoop_AnUpsertRacingTheFireLeavesOneRunAndTheNewCron(t *testing.T) {
	ctx := context.Background()
	c := newCronTenant(t, "daily")
	slot := march(10, 1, 0)
	c.clock.set(slot)
	c.schedule(t, "daily", "0 1 * * *", slot)

	// The fire takes the topology lock once it holds the row; holding that
	// lock keeps the fire inside its transaction until the save waits too.
	holder, err := stateDB.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer holder.Rollback() //nolint:errcheck
	require.NoError(t, postgres.NewTopologyRepository(stateDB, holder).LockTopology(ctx))

	fired := make(chan error, 1)
	go func() {
		fired <- c.loop(false).Pass(ctx, c.clock.Now())
	}()
	waitForLockWaiters(t, 1)
	saved := make(chan error, 1)
	go func() { saved <- c.upsert(ctx, "daily", "30 1 * * *") }()
	waitForLockWaiters(t, 2)
	require.NoError(t, holder.Rollback())

	require.NoError(t, receive(t, fired, "the fire"))
	require.NoError(t, receive(t, saved, "the save"))

	runs := c.runs(t, "daily")
	require.Len(t, runs, 1)
	assert.True(t, slot.Equal(*runs[0].ScheduledFor))
	row := c.row(t, "daily")
	assert.Equal(t, "30 1 * * *", row.Cron)
	assert.Equal(t, int64(2), row.Version)
	assert.Equal(t, "bob@example.com", row.UpdatedBy)
	require.NotNil(t, row.NextFireAt)
	assert.True(t, march(10, 1, 30).Equal(*row.NextFireAt), "the next fire follows the saved cron: got %s", row.NextFireAt)
}

// A change to a schedule notifies the schedules channel: the loop, asleep until
// its hour-long tick with nothing due, passes at once and fires the schedule
// the change made due.
func TestCronLoop_WakesOnANotification(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newCronTenant(t, "daily")
	slot := march(10, 1, 0)
	c.clock.set(slot)
	waker, err := pkgoutbox.NewPostgresWaker(ctx, stateDSN, postgres.SchedulesChannel, c.logger)
	require.NoError(t, err)
	defer waker.Close() //nolint:errcheck
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.loop(false).Run(ctx, waker.Wake(), time.Hour)
	}()
	require.Eventually(t, func() bool { return c.metrics.passCount() >= 1 }, 10*time.Second, 20*time.Millisecond,
		"the loop passes when it starts")

	c.schedule(t, "daily", "0 1 * * *", slot)

	require.Eventually(t, func() bool { return len(c.runs(t, "daily")) == 1 }, 10*time.Second, 50*time.Millisecond,
		"the notification of the insert woke the loop")
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return once its context ended")
	}
}

// A due schedule whose stored cron no longer parses is left out of every pass
// and keeps its past next fire, which is not after the pass and so does not
// count toward the next fire the loop sleeps until. The loop waits its
// hour-long tick rather than passing again and again: in a second and a half
// it passes, and logs the row, once.
func TestCronLoop_AnUnparseableDueRowDoesNotMakeTheLoopSpin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newCronTenant(t, "daily")
	c.clock.set(march(10, 1, 0))
	c.schedule(t, "broken", "not a cron", march(10, 0, 0))
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.loop(false).Run(ctx, nil, time.Hour)
	}()

	time.Sleep(1500 * time.Millisecond)
	cancel()
	<-done

	assert.Equal(t, 1, c.metrics.passCount(), "the pass at the start, then nothing until the tick")
	assert.Equal(t, 1, strings.Count(c.logs.String(), "schedule_name=broken"), "the row is logged once a pass")
	assert.Empty(t, c.runs(t, "broken"))
}
