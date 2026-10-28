//go:build integration

package postgres_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/cron"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/schedule"
	"github.com/carolsimone/continuo/state/service/ports"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ repository.ScheduleRepository = (*postgres.ScheduleRepository)(nil)

// The database these tests use is shared with every other test of the module,
// which run at the same time in other packages, and ListDue and EarliestFireAfter
// read the schedules of every tenant. So that no other session meets a row of
// these tests:
//   - A test that scans (ListDue, EarliestFireAfter, an unparseable row) inserts its
//     rows in a transaction that is rolled back, which no other session sees.
//   - The one test that needs two sessions commits rows whose next fires are in
//     the year 2999 and reads them with a `now` just after, which a pass at the
//     real time never reaches; it holds an advisory lock for its duration, so
//     two runs of it do not take each other's rows.
//   - Every other test commits only schedules created in 2099, so none has a
//     next fire at or before the real time.

// farFuture is the creation time of the schedules these tests commit.
var farFuture = time.Date(2099, time.January, 1, 8, 30, 0, 0, time.UTC)

// scheduleScanLockKey is the advisory lock the test that commits due rows holds.
const scheduleScanLockKey = 0x5c4ed01e

// scheduleParser returns the cron parser the services run with.
func scheduleParser(t *testing.T) ports.CronParser {
	t.Helper()
	parser, err := cron.NewParser()
	require.NoError(t, err)
	return parser
}

// scheduleRepoTenant returns a tenant no other test uses, whose schedules are
// deleted when the test ends.
func scheduleRepoTenant(t *testing.T, db *sqlx.DB) string {
	t.Helper()
	tenant := "sched-" + uuid.NewString()
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM schedules WHERE tenant_id = $1`, tenant) })
	return tenant
}

// scratchTx opens a transaction that is rolled back when the test ends. The
// rows it inserts are never visible to another session.
func scratchTx(t *testing.T, db *sqlx.DB) *sqlx.Tx {
	t.Helper()
	tx, err := db.Beginx()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx
}

// newScheduleRepo binds a repository to tx (nil for the pool) that logs nothing.
func newScheduleRepo(db *sqlx.DB, tx *sqlx.Tx, parser ports.CronParser) *postgres.ScheduleRepository {
	return postgres.NewScheduleRepository(db, tx, parser, discardLogger())
}

// newLoggingScheduleRepo is newScheduleRepo with the log it writes returned.
func newLoggingScheduleRepo(db *sqlx.DB, tx *sqlx.Tx, parser ports.CronParser) (*postgres.ScheduleRepository, *bytes.Buffer) {
	logs := &bytes.Buffer{}
	return postgres.NewScheduleRepository(db, tx, parser, slog.New(slog.NewTextHandler(logs, nil))), logs
}

// scheduleUnitOfWork returns a unit of work that parses crons with parser.
func scheduleUnitOfWork(db *sqlx.DB, parser ports.CronParser) *postgres.PostgresUnitOfWork {
	return postgres.NewPostgresUnitOfWork(db, ports.SystemClock{}, discardLogger(), postgres.WithCronParser(parser))
}

// holdScheduleScanLock takes the advisory lock of the test that commits due
// rows until the test ends, waiting up to a minute for another run to end.
func holdScheduleScanLock(t *testing.T, db *sqlx.DB) {
	t.Helper()
	tx, err := db.Beginx()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	_, err = tx.Exec(`SET LOCAL lock_timeout = '60s'`)
	require.NoError(t, err)
	_, err = tx.Exec(`SELECT pg_advisory_xact_lock($1)`, scheduleScanLockKey)
	require.NoError(t, err)
}

// hourlySchedule returns the schedule an operator creates at `created`: every
// hour on the hour, UTC.
func hourlySchedule(t *testing.T, parser ports.CronParser, tenant, name string, created time.Time) *schedule.Schedule {
	t.Helper()
	return describedSchedule(t, parser, tenant, name, "every hour", created)
}

// describedSchedule is hourlySchedule with a description of its own.
func describedSchedule(t *testing.T, parser ports.CronParser, tenant, name, description string, created time.Time) *schedule.Schedule {
	t.Helper()
	expr, err := parser.Parse("0 * * * *", "UTC")
	require.NoError(t, err)
	return schedule.New(tenant, name, "0 * * * *", "UTC", description, false, expr, "alice@example.com", created)
}

// dueSchedule returns a stored hourly schedule whose next fire is nextFire.
func dueSchedule(t *testing.T, parser ports.CronParser, tenant, name string, nextFire time.Time) *schedule.Schedule {
	t.Helper()
	expr, err := parser.Parse("0 * * * *", "UTC")
	require.NoError(t, err)
	return schedule.Rehydrate(tenant, name, "0 * * * *", "UTC", "", false, &nextFire, 1, "alice@example.com", nextFire, expr)
}

// execer runs a statement; a *sqlx.DB and a *sqlx.Tx are both one.
type execer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// rawSchedule inserts a schedule row, bypassing the repository, whose cron is
// cron in timezone and whose next fire is nextFire (none when nil).
func rawSchedule(t *testing.T, ex execer, tenant, name, cron, timezone string, nextFire *time.Time) {
	t.Helper()
	_, err := ex.Exec(`
		INSERT INTO schedules (tenant_id, name, cron, timezone, next_fire_at, updated_by)
		VALUES ($1, $2, $3, $4, $5, 'repository-test')`, tenant, name, cron, timezone, nextFire)
	require.NoError(t, err)
}

// namesOf returns the names of the tenant's schedules among got, in order.
func namesOf(tenant string, got []*schedule.Schedule) []string {
	names := []string{}
	for _, s := range got {
		if s.TenantID() == tenant {
			names = append(names, s.Name())
		}
	}
	return names
}

func mustGet(t *testing.T, repo repository.ScheduleRepository, tenant, name string) *schedule.Schedule {
	t.Helper()
	got, ok, err := repo.Get(context.Background(), tenant, name)
	require.NoError(t, err)
	require.True(t, ok, "schedule %s of tenant %s is stored", name, tenant)
	return got
}

// A schedule is stored at the version the aggregate holds. Each save of an edit
// names the version its caller read and writes the aggregate's own next
// version, so the stored version counts up by one an edit and the save adds
// nothing to it.
func TestSchedules_SaveBumpsTheVersion(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	repo := newScheduleRepo(db, nil, parser)
	tenant := scheduleRepoTenant(t, db)
	created := farFuture
	require.NoError(t, repo.Save(ctx, hourlySchedule(t, parser, tenant, "nightly", created), 0))

	got := mustGet(t, repo, tenant, "nightly")
	assert.Equal(t, int64(1), got.Version())
	assert.Equal(t, "0 * * * *", got.Cron())
	assert.Equal(t, "UTC", got.Timezone())
	assert.Equal(t, "every hour", got.Description())
	assert.False(t, got.Paused())
	assert.Equal(t, "alice@example.com", got.UpdatedBy())
	assert.True(t, created.Equal(got.UpdatedAt()), "updated at: got %s", got.UpdatedAt())
	next, ok := got.NextFireAt()
	require.True(t, ok)
	assert.True(t, created.Add(30*time.Minute).Equal(next), "next fire: got %s", next)

	// An edit read at version 1.
	edited := created.Add(35 * time.Minute)
	expr, err := parser.Parse("30 2 * * *", "Europe/Paris")
	require.NoError(t, err)
	got.Replace("30 2 * * *", "Europe/Paris", "nightly at 02:30", false, expr, "bob@example.com", edited)
	require.Equal(t, int64(2), got.Version())
	require.NoError(t, repo.Save(ctx, got, 1))

	again := mustGet(t, repo, tenant, "nightly")
	assert.Equal(t, int64(2), again.Version(), "the save writes the aggregate's version and adds none")
	assert.Equal(t, "30 2 * * *", again.Cron())
	assert.Equal(t, "Europe/Paris", again.Timezone())
	assert.Equal(t, "nightly at 02:30", again.Description())
	assert.Equal(t, "bob@example.com", again.UpdatedBy())
	assert.True(t, edited.Equal(again.UpdatedAt()))
	againNext, ok := again.NextFireAt()
	require.True(t, ok)
	// The stored cron is read again in its zone: a day after one slot is the
	// next.
	slot, skipped, due := again.Due(againNext.Add(25 * time.Hour))
	require.True(t, due)
	assert.True(t, againNext.Add(24*time.Hour).Equal(slot), "slot: got %s", slot)
	assert.Equal(t, 1, skipped)

	// A pause stores no next fire.
	again.Replace("30 2 * * *", "Europe/Paris", "nightly at 02:30", true, expr, "bob@example.com", edited.Add(time.Hour))
	require.NoError(t, repo.Save(ctx, again, 2))
	paused := mustGet(t, repo, tenant, "nightly")
	assert.Equal(t, int64(3), paused.Version())
	assert.True(t, paused.Paused())
	_, ok = paused.NextFireAt()
	assert.False(t, ok)
	var stored *time.Time
	require.NoError(t, db.Get(&stored, `SELECT next_fire_at FROM schedules WHERE tenant_id = $1 AND name = 'nightly'`, tenant))
	assert.Nil(t, stored, "a paused schedule has no next_fire_at")
}

// A save that names a version the stored row no longer has writes nothing: two
// editors who read version 1 do not both get to write version 2.
func TestSchedules_ASaveWithAStaleVersionIsRefused(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	repo := newScheduleRepo(db, nil, parser)
	tenant := scheduleRepoTenant(t, db)
	created := farFuture
	require.NoError(t, repo.Save(ctx, hourlySchedule(t, parser, tenant, "nightly", created), 0))
	expr, err := parser.Parse("0 * * * *", "UTC")
	require.NoError(t, err)
	first, second := mustGet(t, repo, tenant, "nightly"), mustGet(t, repo, tenant, "nightly")

	first.Replace("0 * * * *", "UTC", "from the first editor", false, expr, "alice@example.com", created.Add(time.Minute))
	require.NoError(t, repo.Save(ctx, first, 1))
	second.Replace("0 * * * *", "UTC", "from the second editor", false, expr, "bob@example.com", created.Add(2*time.Minute))
	err = repo.Save(ctx, second, 1)

	require.ErrorIs(t, err, schedule.ErrStaleVersion)
	stored := mustGet(t, repo, tenant, "nightly")
	assert.Equal(t, int64(2), stored.Version())
	assert.Equal(t, "from the first editor", stored.Description())
	assert.Equal(t, "alice@example.com", stored.UpdatedBy())

	// A version the row has not reached is as stale as one it has passed.
	second.Replace("0 * * * *", "UTC", "from nowhere", false, expr, "bob@example.com", created.Add(3*time.Minute))
	require.ErrorIs(t, repo.Save(ctx, second, 7), schedule.ErrStaleVersion)

	// A row deleted since it was read has no version to match.
	deleted, err := repo.Delete(ctx, tenant, "nightly")
	require.NoError(t, err)
	require.True(t, deleted)
	require.ErrorIs(t, repo.Save(ctx, second, 2), schedule.ErrStaleVersion)
	_, ok, err := repo.Get(ctx, tenant, "nightly")
	require.NoError(t, err)
	assert.False(t, ok, "a refused save inserts nothing")
}

// A save with no expected version names no row: it creates the row, and over a
// row that exists it writes nothing and is stale, so a save never takes the
// stored version back.
func TestSchedules_ASaveWithNoExpectedVersionOnlyCreates(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	repo := newScheduleRepo(db, nil, parser)
	tenant := scheduleRepoTenant(t, db)
	created := farFuture
	require.NoError(t, repo.Save(ctx, hourlySchedule(t, parser, tenant, "nightly", created), 0))
	stored := mustGet(t, repo, tenant, "nightly")
	assert.Equal(t, int64(1), stored.Version())

	expr, err := parser.Parse("0 * * * *", "UTC")
	require.NoError(t, err)
	stored.Replace("0 * * * *", "UTC", "edited", false, expr, "bob@example.com", created.Add(time.Hour))
	require.NoError(t, repo.Save(ctx, stored, 1))

	again := hourlySchedule(t, parser, tenant, "nightly", created.Add(2*time.Hour))
	err = repo.Save(ctx, again, 0)

	require.ErrorIs(t, err, schedule.ErrStaleVersion)
	kept := mustGet(t, repo, tenant, "nightly")
	assert.Equal(t, int64(2), kept.Version(), "the row keeps its version")
	assert.Equal(t, "edited", kept.Description())
	assert.Equal(t, "bob@example.com", kept.UpdatedBy())
}

// A fire changes the next fire and nothing else: saved at the version it read,
// which a fire leaves as it is, it keeps the version, author and edit time the
// operator left, fire after fire.
func TestSchedules_AFireSaveLeavesTheVersionAndTheAuthorship(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	repo := newScheduleRepo(db, nil, parser)
	tenant := scheduleRepoTenant(t, db)
	created := farFuture
	expr, err := parser.Parse("0 * * * *", "UTC")
	require.NoError(t, err)
	s := hourlySchedule(t, parser, tenant, "nightly", created)
	edited := created.Add(35 * time.Minute)
	s.Replace("0 * * * *", "UTC", "every hour", false, expr, "bob@example.com", edited)
	require.NoError(t, repo.Save(ctx, s, 0))
	slotTime := time.Date(2099, time.January, 1, 12, 0, 0, 0, time.UTC)

	for fire := range 2 {
		got := mustGet(t, repo, tenant, "nightly")
		slot, _, due := got.Due(slotTime.Add(10 * time.Minute))
		require.True(t, due)
		got.Fired(slot)
		require.NoError(t, repo.Save(ctx, got, got.Version()), "fire %d", fire)

		after := mustGet(t, repo, tenant, "nightly")
		assert.Equal(t, int64(2), after.Version(), "fire %d", fire)
		assert.Equal(t, "bob@example.com", after.UpdatedBy())
		assert.True(t, edited.Equal(after.UpdatedAt()), "updated at: got %s", after.UpdatedAt())
		next, ok := after.NextFireAt()
		require.True(t, ok)
		assert.True(t, slotTime.Add(time.Hour).Equal(next), "next fire: got %s", next)

		// Put the next fire back for the second round.
		_, err := db.Exec(`UPDATE schedules SET next_fire_at = $3 WHERE tenant_id = $1 AND name = $2`,
			tenant, "nightly", slotTime)
		require.NoError(t, err)
	}
}

// Two creators of one new name end as one row: the second waits for the first,
// then finds the row and writes nothing, stale; neither sees the primary-key
// conflict.
func TestSchedules_TwoCreatorsOfOneNameLeaveOneRow(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	tenant := scheduleRepoTenant(t, db)
	created := farFuture
	firstUoW, secondUoW := scheduleUnitOfWork(db, parser), scheduleUnitOfWork(db, parser)
	require.NoError(t, firstUoW.Begin(ctx))
	defer func() { _ = firstUoW.Rollback() }()
	require.NoError(t, secondUoW.Begin(ctx))
	defer func() { _ = secondUoW.Rollback() }()
	first := describedSchedule(t, parser, tenant, "nightly", "the first creator", created)
	second := describedSchedule(t, parser, tenant, "nightly", "the second creator", created.Add(time.Minute))
	require.NoError(t, firstUoW.Schedules().Save(ctx, first, 0))

	secondDone := make(chan error, 1)
	go func() { secondDone <- secondUoW.Schedules().Save(ctx, second, 0) }()
	select {
	case err := <-secondDone:
		t.Fatalf("the second creator did not wait for the first to end: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	require.NoError(t, firstUoW.Commit())
	select {
	case err := <-secondDone:
		require.ErrorIs(t, err, schedule.ErrStaleVersion, "the second creator finds the first's row")
		assert.NotContains(t, err.Error(), "duplicate key", "the second creator sees no conflict")
	case <-time.After(10 * time.Second):
		t.Fatal("the second creator never finished after the first committed")
	}
	require.NoError(t, secondUoW.Commit(), "the stale save leaves the transaction usable")

	var rows int
	require.NoError(t, db.Get(&rows, `SELECT count(*) FROM schedules WHERE tenant_id = $1 AND name = 'nightly'`, tenant))
	assert.Equal(t, 1, rows)
	stored := mustGet(t, newScheduleRepo(db, nil, parser), tenant, "nightly")
	assert.Equal(t, "the first creator", stored.Description(), "the second save writes nothing")
	assert.Equal(t, int64(1), stored.Version())
}

// ListDue takes the due schedules of every tenant, the earliest next fire
// first, up to the limit, and leaves the rest and the schedules with no next
// fire alone.
func TestSchedules_ListDueTakesTheDueRowsOldestFirstUpToTheLimit(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	tx := scratchTx(t, db)
	repo := newScheduleRepo(db, tx, parser)
	tenant, other := scheduleRepoTenant(t, db), scheduleRepoTenant(t, db)
	at := func(year, month, day int) *time.Time {
		instant := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
		return &instant
	}
	rawSchedule(t, tx, tenant, "b-second", "0 * * * *", "UTC", at(1975, 1, 2))
	rawSchedule(t, tx, other, "a-first", "0 * * * *", "UTC", at(1975, 1, 1))
	rawSchedule(t, tx, tenant, "c-third", "0 * * * *", "UTC", at(1975, 1, 3))
	rawSchedule(t, tx, tenant, "d-not-yet", "0 * * * *", "UTC", at(1975, 6, 1))
	rawSchedule(t, tx, tenant, "e-no-fire", "0 * * * *", "UTC", nil)

	all, err := repo.ListDue(ctx, *at(1975, 2, 1), 10)
	require.NoError(t, err)
	assert.Equal(t, []string{"b-second", "c-third"}, namesOf(tenant, all))
	assert.Equal(t, []string{"a-first"}, namesOf(other, all), "every tenant's due schedules are taken")
	require.GreaterOrEqual(t, len(all), 3)
	assert.Equal(t, other, all[0].TenantID(), "the earliest next fire comes first")
	assert.Equal(t, "a-first", all[0].Name())

	limited, err := repo.ListDue(ctx, *at(1975, 2, 1), 2)
	require.NoError(t, err)
	require.Len(t, limited, 2)
	assert.Equal(t, "a-first", limited[0].Name())
	assert.Equal(t, "b-second", limited[1].Name())
}

// A pass leaves out a due schedule whose row another transaction holds, such
// as another replica's fire of it, instead of waiting for that transaction,
// and takes it once the transaction has ended. Called outside a transaction,
// as the cron loop calls it, ListDue keeps no lock once it has returned. The
// rows are committed, for the holder and the pass are two sessions; their next
// fires are in the year 2999.
func TestSchedules_ListDueSkipsARowAnotherTransactionHolds(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	holdScheduleScanLock(t, db)
	tenant := scheduleRepoTenant(t, db)
	first := time.Date(2999, 1, 1, 0, 0, 0, 0, time.UTC)
	second := time.Date(2999, 1, 2, 0, 0, 0, 0, time.UTC)
	rawSchedule(t, db, tenant, "first", "0 * * * *", "UTC", &first)
	rawSchedule(t, db, tenant, "second", "0 * * * *", "UTC", &second)

	holder := scheduleUnitOfWork(db, parser)
	require.NoError(t, holder.Begin(ctx))
	defer func() { _ = holder.Rollback() }()
	_, ok, err := holder.Schedules().LoadForUpdate(ctx, tenant, "first")
	require.NoError(t, err)
	require.True(t, ok)

	// A wait for a held row would end in the deadline, not in the answer.
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	pass := newScheduleRepo(db, nil, parser)
	got, err := pass.ListDue(bounded, second.Add(time.Hour), 1000)
	require.NoError(t, err)
	assert.Equal(t, []string{"second"}, namesOf(tenant, got), "the row the holder has is left out")

	require.NoError(t, holder.Rollback())
	again, err := pass.ListDue(bounded, second.Add(time.Hour), 1000)
	require.NoError(t, err)
	assert.Equal(t, []string{"first", "second"}, namesOf(tenant, again), "both are taken once the holder has ended")

	fire := scheduleUnitOfWork(db, parser)
	require.NoError(t, fire.Begin(bounded))
	defer func() { _ = fire.Rollback() }()
	_, ok, err = fire.Schedules().LoadForUpdate(bounded, tenant, "first")
	require.NoError(t, err, "the pass's read keeps no lock a fire would wait for")
	assert.True(t, ok)
}

// EarliestFireAfter is the earliest next fire strictly after its bound among
// the schedules of every tenant; a schedule with none, which is what a paused
// one has, is not counted. A fire at the bound is not after it, and a bound
// finer than the column's microseconds still counts a fire less than a
// microsecond after it. The rows are in 1971 and 1972, earlier than any row
// another test commits, and are inserted in a transaction that is rolled back.
func TestSchedules_EarliestFireAfterSpansTenantsAndIsStrict(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	tx := scratchTx(t, db)
	repo := newScheduleRepo(db, tx, parser)
	tenant, other := scheduleRepoTenant(t, db), scheduleRepoTenant(t, db)
	first := time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC)
	second := time.Date(1972, 1, 1, 0, 0, 0, 0, time.UTC)
	rawSchedule(t, tx, tenant, "second", "0 * * * *", "UTC", &second)
	rawSchedule(t, tx, other, "first", "0 * * * *", "UTC", &first)
	rawSchedule(t, tx, other, "paused", "0 * * * *", "UTC", nil)

	for _, c := range []struct {
		name        string
		after, want time.Time
	}{
		{"before both: the earlier of two tenants' fires", time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), first},
		{"a microsecond before the first", first.Add(-time.Microsecond), first},
		{"less than a microsecond before the first", first.Add(-time.Nanosecond), first},
		{"at the first, which is not after it", first, second},
		{"just after the first", first.Add(time.Nanosecond), second},
	} {
		got, ok, err := repo.EarliestFireAfter(ctx, c.after)
		require.NoError(t, err, c.name)
		require.True(t, ok, c.name)
		assert.True(t, c.want.Equal(got), "%s: got %s", c.name, got)
	}

	got, ok, err := repo.EarliestFireAfter(ctx, second)
	require.NoError(t, err)
	if ok {
		assert.True(t, got.After(second), "a fire after the last of these rows is another test's: got %s", got)
	}
	_, ok, err = repo.EarliestFireAfter(ctx, time.Date(9000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	assert.False(t, ok, "no schedule fires after the year 9000")
}

// A paused schedule fires nothing, whatever its row holds: a paused row written
// by hand with a due next_fire_at is neither listed as due, where it would hold
// one of the pass's slots on every pass, nor counted as the earliest next fire.
// The rows are in 1960 and 1961, earlier than any row another test commits, and
// are inserted in a transaction that is rolled back.
func TestSchedules_APausedRowWithANextFireIsNeitherDueNorTheEarliestFire(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	tx := scratchTx(t, db)
	repo := newScheduleRepo(db, tx, parser)
	tenant := scheduleRepoTenant(t, db)
	pausedAt := time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)
	scheduledAt := time.Date(1961, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err := tx.Exec(`
		INSERT INTO schedules (tenant_id, name, cron, timezone, paused, next_fire_at, updated_by)
		VALUES ($1, 'paused', '0 * * * *', 'UTC', true, $2, 'repository-test')`, tenant, pausedAt)
	require.NoError(t, err)
	rawSchedule(t, tx, tenant, "scheduled", "0 * * * *", "UTC", &scheduledAt)

	due, err := repo.ListDue(ctx, time.Date(1962, 1, 1, 0, 0, 0, 0, time.UTC), 1)
	require.NoError(t, err)
	assert.Equal(t, []string{"scheduled"}, namesOf(tenant, due), "the paused row takes no slot of the pass")

	earliest, ok, err := repo.EarliestFireAfter(ctx, time.Date(1959, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, scheduledAt.Equal(earliest), "the paused row's next_fire_at is not a fire: got %s", earliest)
}

// A stored cron or timezone the parser refuses does not stop a pass: ListDue
// returns the schedules that parse, leaves the others out, and logs each one it
// left out with its tenant, name, cron and zone.
func TestSchedules_ListDueSkipsAStoredCronThatNoLongerParses(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	tx := scratchTx(t, db)
	repo, logs := newLoggingScheduleRepo(db, tx, parser)
	tenant := scheduleRepoTenant(t, db)
	good := time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC)
	brokenCron := time.Date(1989, 1, 1, 0, 0, 0, 0, time.UTC)
	brokenZone := time.Date(1989, 6, 1, 0, 0, 0, 0, time.UTC)
	rawSchedule(t, tx, tenant, "good", "0 * * * *", "UTC", &good)
	rawSchedule(t, tx, tenant, "broken-cron", "not a cron", "UTC", &brokenCron)
	rawSchedule(t, tx, tenant, "broken-zone", "0 * * * *", "Mars/Olympus", &brokenZone)

	got, err := repo.ListDue(ctx, time.Date(1991, 1, 1, 0, 0, 0, 0, time.UTC), 10)

	require.NoError(t, err)
	assert.Equal(t, []string{"good"}, namesOf(tenant, got))
	logged := logs.String()
	for _, want := range []string{"level=ERROR", tenant, "broken-cron", "not a cron", "broken-zone", "Mars/Olympus"} {
		assert.Contains(t, logged, want)
	}
	assert.NotContains(t, logged, "schedule_name=good")
}

// More due rows whose stored cron or timezone no longer parses than the limit,
// all older than the readable ones, take none of the limit's places: ListDue
// still returns the readable due rows of the tenant and of another tenant,
// oldest first, up to the limit, so a pass fires them. The rows are in 1940 and
// are inserted in a transaction that is rolled back.
func TestSchedules_ListDueTakesTheReadableRowsPastMoreUnreadableOnesThanTheLimit(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	tx := scratchTx(t, db)
	repo := newScheduleRepo(db, tx, parser)
	tenant, other := scheduleRepoTenant(t, db), scheduleRepoTenant(t, db)
	start := time.Date(1940, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range 101 {
		next := start.Add(time.Duration(i) * time.Minute)
		cron, zone := "not a cron", "UTC"
		if i%2 == 1 {
			cron, zone = "0 * * * *", "Mars/Olympus"
		}
		rawSchedule(t, tx, tenant, fmt.Sprintf("broken-%03d", i), cron, zone, &next)
	}
	otherDue := time.Date(1940, 6, 1, 0, 0, 0, 0, time.UTC)
	ownDue := time.Date(1940, 6, 2, 0, 0, 0, 0, time.UTC)
	rawSchedule(t, tx, other, "healthy", "0 * * * *", "UTC", &otherDue)
	rawSchedule(t, tx, tenant, "healthy", "0 * * * *", "UTC", &ownDue)
	now := time.Date(1941, 1, 1, 0, 0, 0, 0, time.UTC)

	got, err := repo.ListDue(ctx, now, 100)

	require.NoError(t, err)
	assert.Equal(t, []string{"healthy"}, namesOf(other, got), "another tenant's readable row is taken")
	assert.Equal(t, []string{"healthy"}, namesOf(tenant, got), "the tenant's own readable row is taken")

	first, err := repo.ListDue(ctx, now, 1)
	require.NoError(t, err)
	require.Len(t, first, 1, "the limit counts readable rows")
	assert.Equal(t, other, first[0].TenantID(), "the oldest readable row comes first")
	assert.Equal(t, "healthy", first[0].Name())
}

// However many due rows do not parse, one ListDue logs the first ten of them,
// oldest next fire first, each with its tenant, name, cron and zone, and the
// rest in one line that counts them. The rows are in 1945 and are inserted in
// a transaction that is rolled back.
func TestSchedules_ListDueNamesTheFirstUnreadableRowsAndCountsTheRest(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	tx := scratchTx(t, db)
	repo, logs := newLoggingScheduleRepo(db, tx, parser)
	tenant := scheduleRepoTenant(t, db)
	start := time.Date(1945, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range 25 {
		next := start.Add(time.Duration(i) * time.Minute)
		rawSchedule(t, tx, tenant, fmt.Sprintf("broken-%03d", i), fmt.Sprintf("not a cron %d", i), "UTC", &next)
	}

	got, err := repo.ListDue(ctx, time.Date(1946, 1, 1, 0, 0, 0, 0, time.UTC), 100)

	require.NoError(t, err)
	assert.Empty(t, namesOf(tenant, got))
	logged := logs.String()
	assert.Equal(t, 10, strings.Count(logged, "Left out a due schedule"), "the first ten are named: %s", logged)
	for i := range 10 {
		assert.Contains(t, logged, fmt.Sprintf("schedule_name=broken-%03d", i))
	}
	assert.Contains(t, logged, `cron="not a cron 0"`)
	assert.Contains(t, logged, "tenant_id="+tenant)
	assert.NotContains(t, logged, "schedule_name=broken-010", "the eleventh is counted, not named")
	assert.Contains(t, logged, "more=15")
	assert.Contains(t, logged, "left_out=25")
	assert.Equal(t, 11, strings.Count(logged, "level=ERROR"))
}

// Inside a transaction ListDue locks the rows it returns and none of the rows
// it leaves out for a stored cron that does not parse, which another session
// can still lock. The rows are committed, for the lock is seen from a second
// session; their next fires are in the year 2999.
func TestSchedules_ListDueInATransactionLocksOnlyTheRowsItReturns(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	holdScheduleScanLock(t, db)
	tenant := scheduleRepoTenant(t, db)
	brokenAt := time.Date(2999, 1, 1, 0, 0, 0, 0, time.UTC)
	goodAt := time.Date(2999, 1, 2, 0, 0, 0, 0, time.UTC)
	rawSchedule(t, db, tenant, "broken", "not a cron", "UTC", &brokenAt)
	rawSchedule(t, db, tenant, "good", "0 * * * *", "UTC", &goodAt)

	pass := scheduleUnitOfWork(db, parser)
	require.NoError(t, pass.Begin(ctx))
	defer func() { _ = pass.Rollback() }()
	// The database is shared, and the committed due rows of other suites sort
	// before these: a limit of 1000, as the test of a row another transaction
	// holds uses, keeps "good" inside it whatever else is due.
	got, err := pass.Schedules().ListDue(ctx, goodAt.Add(time.Hour), 1000)
	require.NoError(t, err)
	assert.Equal(t, []string{"good"}, namesOf(tenant, got))

	other := scratchTx(t, db)
	var name string
	require.NoError(t, other.Get(&name,
		`SELECT name FROM schedules WHERE tenant_id = $1 AND name = 'broken' FOR UPDATE NOWAIT`, tenant),
		"the row left out is not locked")
	err = other.Get(&name, `SELECT name FROM schedules WHERE tenant_id = $1 AND name = 'good' FOR UPDATE NOWAIT`, tenant)
	require.Error(t, err, "the row returned is locked")
	assert.Contains(t, err.Error(), "could not obtain lock")
}

// List, like ListDue, returns the schedules that parse and logs the ones that
// do not.
func TestSchedules_ListSkipsAStoredCronThatNoLongerParses(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	tx := scratchTx(t, db)
	repo, logs := newLoggingScheduleRepo(db, tx, parser)
	tenant := scheduleRepoTenant(t, db)
	rawSchedule(t, tx, tenant, "b-good", "0 * * * *", "UTC", nil)
	rawSchedule(t, tx, tenant, "a-broken-cron", "not a cron", "UTC", nil)
	rawSchedule(t, tx, tenant, "c-broken-zone", "0 * * * *", "Mars/Olympus", nil)
	rawSchedule(t, tx, tenant, "d-good", "30 2 * * *", "Europe/Paris", nil)

	got, err := repo.List(ctx, tenant)

	require.NoError(t, err)
	assert.Equal(t, []string{"b-good", "d-good"}, namesOf(tenant, got))
	logged := logs.String()
	for _, want := range []string{"level=ERROR", tenant, "a-broken-cron", "not a cron", "c-broken-zone", "Mars/Olympus"} {
		assert.Contains(t, logged, want)
	}
}

// List and ListStoredCronInvalid order names byte by byte, as Go compares
// strings: an upper-case name comes before every lower-case one, whatever the
// database's collation.
func TestSchedules_ListsOrderNamesByteByByte(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	tx := scratchTx(t, db)
	repo := newScheduleRepo(db, tx, parser)
	tenant := scheduleRepoTenant(t, db)
	for _, name := range []string{"b-daily", "Z-daily", "a-daily"} {
		rawSchedule(t, tx, tenant, name, "0 * * * *", "UTC", nil)
		rawSchedule(t, tx, tenant, name+"-broken", "not a cron", "UTC", nil)
	}

	listed, err := repo.List(ctx, tenant)
	require.NoError(t, err)
	broken, err := repo.ListStoredCronInvalid(ctx, tenant)
	require.NoError(t, err)

	assert.Equal(t, []string{"Z-daily", "a-daily", "b-daily"}, namesOf(tenant, listed))
	assert.Equal(t, []string{"Z-daily-broken", "a-daily-broken", "b-daily-broken"}, broken)
}

// ListStoredCronInvalid names, by name, the tenant's rows whose stored cron or
// zone no longer parses, the rows List leaves out, and logs nothing for them.
// Another tenant's rows are not named.
func TestSchedules_ListStoredCronInvalidNamesTheRowsListLeavesOut(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	tx := scratchTx(t, db)
	repo, logs := newLoggingScheduleRepo(db, tx, parser)
	tenant := scheduleRepoTenant(t, db)
	other := scheduleRepoTenant(t, db)
	rawSchedule(t, tx, tenant, "b-good", "0 * * * *", "UTC", nil)
	rawSchedule(t, tx, tenant, "c-broken-zone", "0 * * * *", "Mars/Olympus", nil)
	rawSchedule(t, tx, tenant, "a-broken-cron", "not a cron", "UTC", nil)
	rawSchedule(t, tx, tenant, "d-good", "30 2 * * *", "Europe/Paris", nil)
	rawSchedule(t, tx, other, "theirs-broken", "not a cron", "UTC", nil)

	got, err := repo.ListStoredCronInvalid(ctx, tenant)

	require.NoError(t, err)
	assert.Equal(t, []string{"a-broken-cron", "c-broken-zone"}, got)
	assert.Empty(t, logs.String(), "naming the rows logs nothing per row")

	theirs, err := repo.ListStoredCronInvalid(ctx, other)
	require.NoError(t, err)
	assert.Equal(t, []string{"theirs-broken"}, theirs)

	none, err := repo.ListStoredCronInvalid(ctx, "sched-no-rows-"+uuid.NewString())
	require.NoError(t, err)
	assert.Empty(t, none)

	_, err = newScheduleRepo(db, tx, nil).ListStoredCronInvalid(ctx, tenant)
	require.Error(t, err, "a repository with no parser cannot tell which rows parse")
}

// A lookup of one schedule is not skipped: Get fails with an error that wraps
// schedule.ErrStoredCronInvalid and names the tenant, schedule, cron and zone.
func TestSchedules_GetOfAStoredCronThatNoLongerParsesFails(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	tx := scratchTx(t, db)
	repo := newScheduleRepo(db, tx, parser)
	tenant := scheduleRepoTenant(t, db)
	rawSchedule(t, tx, tenant, "broken-cron", "not a cron", "UTC", nil)
	rawSchedule(t, tx, tenant, "broken-zone", "0 * * * *", "Mars/Olympus", nil)
	rawSchedule(t, tx, tenant, "good", "0 * * * *", "UTC", nil)

	for name, parts := range map[string][]string{
		"broken-cron": {tenant, "broken-cron", "not a cron", "UTC"},
		"broken-zone": {tenant, "broken-zone", "0 * * * *", "Mars/Olympus"},
	} {
		got, ok, err := repo.Get(ctx, tenant, name)

		require.Error(t, err, name)
		assert.ErrorIs(t, err, schedule.ErrStoredCronInvalid, name)
		assert.False(t, ok, name)
		assert.Nil(t, got, name)
		for _, part := range parts {
			assert.Contains(t, err.Error(), part, name)
		}
	}
	mustGet(t, repo, tenant, "good")
}

// Exists says whether the tenant has a row of the name without reading its
// cron: a row whose stored cron or zone no longer parses exists, as does one
// read by a repository with no parser, and another tenant's row is not this
// tenant's. Inside a transaction it sees that transaction's rows.
func TestSchedules_ExistsWhateverTheStoredCron(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tx := scratchTx(t, db)
	tenant := scheduleRepoTenant(t, db)
	rawSchedule(t, tx, tenant, "good", "0 * * * *", "UTC", nil)
	rawSchedule(t, tx, tenant, "broken-cron", "not a cron", "UTC", nil)
	rawSchedule(t, tx, tenant, "broken-zone", "0 * * * *", "Mars/Olympus", nil)
	other := scheduleRepoTenant(t, db)
	rawSchedule(t, tx, other, "elsewhere", "0 * * * *", "UTC", nil)

	for _, repo := range []*postgres.ScheduleRepository{
		newScheduleRepo(db, tx, scheduleParser(t)),
		newScheduleRepo(db, tx, nil),
	} {
		for name, want := range map[string]bool{"good": true, "broken-cron": true, "broken-zone": true, "elsewhere": false, "none": false} {
			got, err := repo.Exists(ctx, tenant, name)
			require.NoError(t, err, name)
			assert.Equal(t, want, got, name)
		}
	}
	got, err := newScheduleRepo(db, nil, nil).Exists(ctx, tenant, "good")
	require.NoError(t, err)
	assert.False(t, got, "the pool does not see a row the open transaction stored")
}

// The error Get returns for a stored cron that no longer parses carries the
// version the row is at, so an edit that saves over it can raise that version.
func TestSchedules_GetOfAStoredCronThatNoLongerParsesNamesItsVersion(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tx := scratchTx(t, db)
	repo := newScheduleRepo(db, tx, scheduleParser(t))
	tenant := scheduleRepoTenant(t, db)
	rawSchedule(t, tx, tenant, "broken", "0 * * * *", "Mars/Olympus", nil)
	_, err := tx.Exec(`UPDATE schedules SET version = 7 WHERE tenant_id = $1 AND name = 'broken'`, tenant)
	require.NoError(t, err)

	_, _, err = repo.Get(ctx, tenant, "broken")

	var invalid *schedule.StoredCronInvalidError
	require.ErrorAs(t, err, &invalid)
	assert.Equal(t, int64(7), invalid.Version)
	assert.Equal(t, tenant, invalid.TenantID)
	assert.Equal(t, "broken", invalid.Name)
}

// Saving a schedule over a row whose stored cron no longer parses, at the
// version the row has, repairs it; with no expected version the save is a
// create, which such a row makes stale like any other. Delete removes such a
// row. None of them reads the stored cron.
func TestSchedules_SaveOverAStoredCronThatNoLongerParsesRepairsIt(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	tx := scratchTx(t, db)
	repo := newScheduleRepo(db, tx, parser)
	tenant := scheduleRepoTenant(t, db)
	rawSchedule(t, tx, tenant, "repaired-by-version", "0 * * * *", "Mars/Olympus", nil)
	rawSchedule(t, tx, tenant, "deleted", "not a cron", "UTC", nil)
	_, _, err := repo.Get(ctx, tenant, "repaired-by-version")
	var unreadable *schedule.StoredCronInvalidError
	require.ErrorAs(t, err, &unreadable)

	require.ErrorIs(t, repo.Save(ctx, hourlySchedule(t, parser, tenant, "repaired-by-version", farFuture), 0), schedule.ErrStaleVersion)

	// The raw row is at version 1, the version the error names.
	require.Equal(t, int64(1), unreadable.Version)
	expr, err := parser.Parse("0 * * * *", "UTC")
	require.NoError(t, err)
	edited := hourlySchedule(t, parser, tenant, "repaired-by-version", farFuture)
	edited.Replace("0 * * * *", "UTC", "repaired", false, expr, "bob@example.com", farFuture.Add(time.Minute))
	require.NoError(t, repo.Save(ctx, edited, unreadable.Version))
	byVersion := mustGet(t, repo, tenant, "repaired-by-version")
	assert.Equal(t, "UTC", byVersion.Timezone())
	assert.Equal(t, "repaired", byVersion.Description())
	assert.Equal(t, int64(2), byVersion.Version())

	deleted, err := repo.Delete(ctx, tenant, "deleted")
	require.NoError(t, err)
	assert.True(t, deleted)
	_, ok, err := repo.Get(ctx, tenant, "deleted")
	require.NoError(t, err)
	assert.False(t, ok)
}

// A repository with no parser cannot rebuild a schedule's expression. That is a
// defect of whoever built it, not of a row, so it is an error wherever a row is
// loaded and no row is skipped for it.
func TestSchedules_WithoutACronParserALoadIsRefused(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tx := scratchTx(t, db)
	repo := newScheduleRepo(db, tx, nil)
	tenant := scheduleRepoTenant(t, db)
	due := time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC)
	rawSchedule(t, tx, tenant, "nightly", "0 * * * *", "UTC", &due)

	_, _, err := repo.Get(ctx, tenant, "nightly")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cron parser")
	assert.NotErrorIs(t, err, schedule.ErrStoredCronInvalid)

	_, err = repo.List(ctx, tenant)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cron parser")

	_, err = repo.ListDue(ctx, due.Add(time.Hour), 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cron parser")

	_, ok, err := repo.Get(ctx, tenant, "absent")
	require.NoError(t, err, "a schedule that is not stored needs no parser")
	assert.False(t, ok)
}

func TestSchedules_GetAndListAreScopedToATenant(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	repo := newScheduleRepo(db, nil, parser)
	tenant, other := scheduleRepoTenant(t, db), scheduleRepoTenant(t, db)
	created := farFuture
	for _, s := range []*schedule.Schedule{
		hourlySchedule(t, parser, tenant, "weekly", created),
		hourlySchedule(t, parser, tenant, "daily", created),
		hourlySchedule(t, parser, other, "daily", created),
	} {
		require.NoError(t, repo.Save(ctx, s, 0))
	}

	listed, err := repo.List(ctx, tenant)
	require.NoError(t, err)
	assert.Equal(t, []string{"daily", "weekly"}, namesOf(tenant, listed), "ordered by name")
	assert.Len(t, listed, 2, "another tenant's schedules are not listed")

	none, err := repo.List(ctx, "sched-"+uuid.NewString())
	require.NoError(t, err)
	assert.Empty(t, none)

	_, ok, err := repo.Get(ctx, tenant, "absent")
	require.NoError(t, err)
	assert.False(t, ok)
	_, ok, err = repo.Get(ctx, other, "weekly")
	require.NoError(t, err)
	assert.False(t, ok, "a name another tenant holds is not this tenant's")
}

func TestSchedules_DeleteReportsWhetherTheRowWasThere(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	repo := newScheduleRepo(db, nil, parser)
	tenant, other := scheduleRepoTenant(t, db), scheduleRepoTenant(t, db)
	created := farFuture
	require.NoError(t, repo.Save(ctx, hourlySchedule(t, parser, tenant, "nightly", created), 0))
	require.NoError(t, repo.Save(ctx, hourlySchedule(t, parser, other, "nightly", created), 0))

	deleted, err := repo.Delete(ctx, tenant, "nightly")
	require.NoError(t, err)
	assert.True(t, deleted)
	_, ok, err := repo.Get(ctx, tenant, "nightly")
	require.NoError(t, err)
	assert.False(t, ok)
	mustGet(t, repo, other, "nightly")

	deleted, err = repo.Delete(ctx, tenant, "nightly")
	require.NoError(t, err)
	assert.False(t, deleted, "a second delete finds nothing")
}

// The unit of work binds the repository to its transaction: what a transaction
// saves is its own until it commits, and a rollback discards it. The schedule
// that is due is only ever saved in a transaction that is rolled back.
func TestSchedules_TheUnitOfWorkBindsTheRepositoryToItsTransaction(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	pool := newScheduleRepo(db, nil, parser)
	tenant := scheduleRepoTenant(t, db)
	u := scheduleUnitOfWork(db, parser)
	require.NoError(t, u.Begin(ctx))
	defer func() { _ = u.Rollback() }()
	due := time.Date(1985, 1, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, u.Schedules().Save(ctx, dueSchedule(t, parser, tenant, "due", due), 0))
	require.NoError(t, u.Schedules().Save(ctx, hourlySchedule(t, parser, tenant, "dropped", farFuture), 0))

	_, ok, err := pool.Get(ctx, tenant, "dropped")
	require.NoError(t, err)
	assert.False(t, ok, "the pool does not see a schedule the open transaction saved")
	_, ok, err = u.Schedules().Get(ctx, tenant, "dropped")
	require.NoError(t, err)
	assert.True(t, ok, "the transaction sees its own save")
	locked, err := u.Schedules().ListDue(ctx, due.Add(time.Hour), 10)
	require.NoError(t, err)
	assert.Equal(t, []string{"due"}, namesOf(tenant, locked), "ListDue runs in the transaction")

	require.NoError(t, u.Rollback())
	_, ok, err = pool.Get(ctx, tenant, "dropped")
	require.NoError(t, err)
	assert.False(t, ok, "a rollback discards the save")
	_, ok, err = pool.Get(ctx, tenant, "due")
	require.NoError(t, err)
	assert.False(t, ok, "a rollback discards the save")

	require.NoError(t, u.Begin(ctx))
	require.NoError(t, u.Schedules().Save(ctx, hourlySchedule(t, parser, tenant, "kept", farFuture), 0))
	require.NoError(t, u.Commit())
	mustGet(t, pool, tenant, "kept")
}

// LoadForUpdate reads the schedule and holds its row until the transaction
// ends: a second transaction's LoadForUpdate waits for the first, then reads
// the row as the first left it. The schedules are created in 2099.
func TestSchedules_LoadForUpdateLocksTheRowUntilTheTransactionEnds(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	tenant := scheduleRepoTenant(t, db)
	require.NoError(t, newScheduleRepo(db, nil, parser).Save(ctx, hourlySchedule(t, parser, tenant, "nightly", farFuture), 0))

	holder := scheduleUnitOfWork(db, parser)
	require.NoError(t, holder.Begin(ctx))
	defer func() { _ = holder.Rollback() }()
	held, ok, err := holder.Schedules().LoadForUpdate(ctx, tenant, "nightly")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "nightly", held.Name())
	assert.Equal(t, int64(1), held.Version())

	waiter := scheduleUnitOfWork(db, parser)
	require.NoError(t, waiter.Begin(ctx))
	defer func() { _ = waiter.Rollback() }()
	read := make(chan error, 1)
	var seen string
	go func() {
		s, _, err := waiter.Schedules().LoadForUpdate(ctx, tenant, "nightly")
		if err == nil {
			seen = s.Description()
		}
		read <- err
	}()
	select {
	case err := <-read:
		t.Fatalf("the second LoadForUpdate did not wait for the first transaction: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	expr, err := parser.Parse("0 * * * *", "UTC")
	require.NoError(t, err)
	held.Replace("0 * * * *", "UTC", "edited by the holder", false, expr, "bob@example.com", farFuture.Add(time.Minute))
	require.NoError(t, holder.Schedules().Save(ctx, held, 1))
	require.NoError(t, holder.Commit())
	select {
	case err := <-read:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the second LoadForUpdate never finished after the first committed")
	}
	assert.Equal(t, "edited by the holder", seen, "the waiter reads the row the holder committed")
}

// LoadForUpdate locks inside a transaction only: outside one it is an error. A
// schedule that is not stored is not found, and one whose stored cron no
// longer parses is the error Get returns for it.
func TestSchedules_LoadForUpdateNeedsATransaction(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	tenant := scheduleRepoTenant(t, db)

	_, _, err := newScheduleRepo(db, nil, parser).LoadForUpdate(ctx, tenant, "nightly")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transaction")

	tx := scratchTx(t, db)
	repo := newScheduleRepo(db, tx, parser)
	got, ok, err := repo.LoadForUpdate(ctx, tenant, "absent")
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, got)

	rawSchedule(t, tx, tenant, "broken", "not a cron", "UTC", nil)
	_, ok, err = repo.LoadForUpdate(ctx, tenant, "broken")
	require.ErrorIs(t, err, schedule.ErrStoredCronInvalid)
	assert.False(t, ok)
}

// Count counts the schedules of every tenant by state: those not paused are
// scheduled. The rows are inserted in a transaction that is rolled back, and
// both counts are read in its snapshot, so what other tests commit meanwhile
// does not change the difference.
func TestSchedules_CountSpansTenantsByState(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	tenant, other := scheduleRepoTenant(t, db), scheduleRepoTenant(t, db)
	tx, err := db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	repo := newScheduleRepo(db, tx, parser)
	scheduledBefore, pausedBefore, err := repo.Count(ctx)
	require.NoError(t, err)

	next := farFuture
	rawSchedule(t, tx, tenant, "daily", "0 * * * *", "UTC", &next)
	rawSchedule(t, tx, other, "daily", "0 * * * *", "UTC", &next)
	rawSchedule(t, tx, other, "broken", "not a cron", "UTC", &next)
	_, err = tx.Exec(`INSERT INTO schedules (tenant_id, name, cron, timezone, paused, updated_by)
		VALUES ($1, 'paused', '0 * * * *', 'UTC', true, 'repository-test')`, tenant)
	require.NoError(t, err)

	scheduled, paused, err := repo.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, scheduled-scheduledBefore, "every schedule not paused, whether or not its cron still parses")
	assert.Equal(t, 1, paused-pausedBefore)
}
