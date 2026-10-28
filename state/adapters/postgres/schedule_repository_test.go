//go:build integration

package postgres_test

import (
	"context"
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

// Tests that read schedules of every tenant (LockDue, EarliestFire) give their
// rows next fires in the 1970s and 1980s, earlier than any instant another
// test or the services give a schedule, so the rows they expect come first
// whatever else the database holds.

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

// scheduleUnitOfWork returns a unit of work that parses crons with parser.
func scheduleUnitOfWork(db *sqlx.DB, parser ports.CronParser) *postgres.PostgresUnitOfWork {
	return postgres.NewPostgresUnitOfWork(db, ports.SystemClock{}, discardLogger(), postgres.WithCronParser(parser))
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

// rawSchedule inserts a schedule row whose cron is cron, UTC, and whose next
// fire is nextFire (none when nil), bypassing the repository.
func rawSchedule(t *testing.T, db *sqlx.DB, tenant, name, cron string, nextFire *time.Time) {
	t.Helper()
	_, err := db.Exec(`
		INSERT INTO schedules (tenant_id, name, cron, timezone, next_fire_at, updated_by)
		VALUES ($1, $2, $3, 'UTC', $4, 'repository-test')`, tenant, name, cron, nextFire)
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
	repo := postgres.NewScheduleRepository(db, nil, parser)
	tenant := scheduleRepoTenant(t, db)
	created := time.Date(2026, 10, 28, 8, 30, 0, 0, time.UTC)
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
	assert.True(t, time.Date(2026, 10, 28, 9, 0, 0, 0, time.UTC).Equal(next), "next fire: got %s", next)

	// An edit read at version 1.
	edited := time.Date(2026, 10, 28, 9, 5, 0, 0, time.UTC)
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
	repo := postgres.NewScheduleRepository(db, nil, parser)
	tenant := scheduleRepoTenant(t, db)
	created := time.Date(2026, 10, 28, 8, 30, 0, 0, time.UTC)
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

// A save with no expected version writes whatever the row holds: it creates the
// row, and an existing row is overwritten.
func TestSchedules_ASaveWithNoExpectedVersionWritesWithoutACheck(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	repo := postgres.NewScheduleRepository(db, nil, parser)
	tenant := scheduleRepoTenant(t, db)
	created := time.Date(2026, 10, 28, 8, 30, 0, 0, time.UTC)
	require.NoError(t, repo.Save(ctx, hourlySchedule(t, parser, tenant, "nightly", created), 0))

	expr, err := parser.Parse("0 * * * *", "UTC")
	require.NoError(t, err)
	over := hourlySchedule(t, parser, tenant, "nightly", created.Add(time.Hour))
	over.Replace("0 * * * *", "UTC", "overwritten", false, expr, "bob@example.com", created.Add(2*time.Hour))
	require.NoError(t, repo.Save(ctx, over, 0))

	stored := mustGet(t, repo, tenant, "nightly")
	assert.Equal(t, "overwritten", stored.Description())
	assert.Equal(t, int64(2), stored.Version(), "the stored version is the aggregate's")
}

// A fire changes the next fire and nothing else: the version, author and edit
// time the operator left stay as they were, however the save names the
// version.
func TestSchedules_AFireSaveLeavesTheVersionAndTheAuthorship(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	repo := postgres.NewScheduleRepository(db, nil, parser)
	tenant := scheduleRepoTenant(t, db)
	created := time.Date(2026, 10, 28, 8, 30, 0, 0, time.UTC)
	expr, err := parser.Parse("0 * * * *", "UTC")
	require.NoError(t, err)
	s := hourlySchedule(t, parser, tenant, "nightly", created)
	edited := created.Add(35 * time.Minute)
	s.Replace("0 * * * *", "UTC", "every hour", false, expr, "bob@example.com", edited)
	require.NoError(t, repo.Save(ctx, s, 0))

	for _, expected := range []int64{0, 2} {
		got := mustGet(t, repo, tenant, "nightly")
		slot, _, due := got.Due(time.Date(2026, 10, 28, 12, 10, 0, 0, time.UTC))
		require.True(t, due)
		got.Fired(slot)
		require.NoError(t, repo.Save(ctx, got, expected), "expected version %d", expected)

		after := mustGet(t, repo, tenant, "nightly")
		assert.Equal(t, int64(2), after.Version(), "expected version %d", expected)
		assert.Equal(t, "bob@example.com", after.UpdatedBy())
		assert.True(t, edited.Equal(after.UpdatedAt()), "updated at: got %s", after.UpdatedAt())
		next, ok := after.NextFireAt()
		require.True(t, ok)
		assert.True(t, time.Date(2026, 10, 28, 13, 0, 0, 0, time.UTC).Equal(next), "next fire: got %s", next)

		// Put the next fire back for the second round.
		_, err := db.Exec(`UPDATE schedules SET next_fire_at = $3 WHERE tenant_id = $1 AND name = $2`,
			tenant, "nightly", time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC))
		require.NoError(t, err)
	}
}

// Two creators of one new name end as one row: the second waits for the first,
// then writes over it, and neither sees the primary-key conflict.
func TestSchedules_TwoCreatorsOfOneNameLeaveOneRow(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	tenant := scheduleRepoTenant(t, db)
	created := time.Date(2026, 10, 28, 8, 30, 0, 0, time.UTC)
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
		require.NoError(t, err, "the second creator sees no conflict")
	case <-time.After(10 * time.Second):
		t.Fatal("the second creator never finished after the first committed")
	}
	require.NoError(t, secondUoW.Commit())

	var rows int
	require.NoError(t, db.Get(&rows, `SELECT count(*) FROM schedules WHERE tenant_id = $1 AND name = 'nightly'`, tenant))
	assert.Equal(t, 1, rows)
	stored := mustGet(t, postgres.NewScheduleRepository(db, nil, parser), tenant, "nightly")
	assert.Equal(t, "the second creator", stored.Description(), "the later save wins")
}

// LockDue takes the due schedules of every tenant, the earliest next fire
// first, up to the limit, and leaves the rest and the schedules with no next
// fire alone.
func TestSchedules_LockDueTakesTheDueRowsOldestFirstUpToTheLimit(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	tenant, other := scheduleRepoTenant(t, db), scheduleRepoTenant(t, db)
	at := func(year, month, day int) *time.Time {
		instant := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
		return &instant
	}
	rawSchedule(t, db, tenant, "b-second", "0 * * * *", at(1975, 1, 2))
	rawSchedule(t, db, other, "a-first", "0 * * * *", at(1975, 1, 1))
	rawSchedule(t, db, tenant, "c-third", "0 * * * *", at(1975, 1, 3))
	rawSchedule(t, db, tenant, "d-not-yet", "0 * * * *", at(1975, 6, 1))
	rawSchedule(t, db, tenant, "e-no-fire", "0 * * * *", nil)
	u := scheduleUnitOfWork(db, parser)
	require.NoError(t, u.Begin(ctx))
	defer func() { _ = u.Rollback() }()

	all, err := u.Schedules().LockDue(ctx, *at(1975, 2, 1), 10)
	require.NoError(t, err)
	assert.Equal(t, []string{"b-second", "c-third"}, namesOf(tenant, all))
	assert.Equal(t, []string{"a-first"}, namesOf(other, all), "every tenant's due schedules are taken")
	require.GreaterOrEqual(t, len(all), 3)
	assert.Equal(t, other, all[0].TenantID(), "the earliest next fire comes first")
	assert.Equal(t, "a-first", all[0].Name())

	require.NoError(t, u.Rollback())
	require.NoError(t, u.Begin(ctx))
	limited, err := u.Schedules().LockDue(ctx, *at(1975, 2, 1), 2)
	require.NoError(t, err)
	require.Len(t, limited, 2)
	assert.Equal(t, "a-first", limited[0].Name())
	assert.Equal(t, "b-second", limited[1].Name())
}

// A pass that finds a schedule locked by another pass leaves it out instead of
// waiting for that pass, so two passes never take the same schedule, and takes
// it once the other pass has ended.
func TestSchedules_LockDueSkipsARowAnotherPassHolds(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	tenant := scheduleRepoTenant(t, db)
	first := time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)
	second := time.Date(1980, 1, 2, 0, 0, 0, 0, time.UTC)
	rawSchedule(t, db, tenant, "first", "0 * * * *", &first)
	rawSchedule(t, db, tenant, "second", "0 * * * *", &second)

	holder := scheduleUnitOfWork(db, parser)
	require.NoError(t, holder.Begin(ctx))
	defer func() { _ = holder.Rollback() }()
	held, err := holder.Schedules().LockDue(ctx, first.Add(time.Hour), 10)
	require.NoError(t, err)
	require.Equal(t, []string{"first"}, namesOf(tenant, held))

	// A wait for the held row would end in the deadline, not in the answer.
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	skipper := scheduleUnitOfWork(db, parser)
	require.NoError(t, skipper.Begin(bounded))
	defer func() { _ = skipper.Rollback() }()
	got, err := skipper.Schedules().LockDue(bounded, second.Add(time.Hour), 10)
	require.NoError(t, err)
	assert.Equal(t, []string{"second"}, namesOf(tenant, got), "the row the holder has is left out")

	require.NoError(t, holder.Rollback())
	require.NoError(t, skipper.Rollback())

	later := scheduleUnitOfWork(db, parser)
	require.NoError(t, later.Begin(ctx))
	defer func() { _ = later.Rollback() }()
	again, err := later.Schedules().LockDue(ctx, second.Add(time.Hour), 10)
	require.NoError(t, err)
	assert.Equal(t, []string{"first", "second"}, namesOf(tenant, again), "both are free once the other passes have ended")
}

// EarliestFire is the earliest next fire among the schedules of every tenant;
// a schedule with none, which is what a paused one has, is not counted.
func TestSchedules_EarliestFireSpansTenants(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	repo := postgres.NewScheduleRepository(db, nil, parser)
	tenant, other := scheduleRepoTenant(t, db), scheduleRepoTenant(t, db)
	later := time.Date(1972, 1, 1, 0, 0, 0, 0, time.UTC)
	earliest := time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC)
	rawSchedule(t, db, tenant, "later", "0 * * * *", &later)
	got, ok, err := repo.EarliestFire(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, later.Equal(got), "one tenant's next fire: got %s", got)

	rawSchedule(t, db, other, "earliest", "0 * * * *", &earliest)
	rawSchedule(t, db, other, "paused", "0 * * * *", nil)
	got, ok, err = repo.EarliestFire(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, earliest.Equal(got), "the earlier of two tenants' next fires: got %s", got)
}

// A stored cron the parser refuses is an error that names the tenant and the
// schedule, wherever the row is read; the row is not skipped.
func TestSchedules_AStoredCronThatNoLongerParsesNamesTheSchedule(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	repo := postgres.NewScheduleRepository(db, nil, parser)
	tenant := scheduleRepoTenant(t, db)
	due := time.Date(1981, 1, 1, 0, 0, 0, 0, time.UTC)
	rawSchedule(t, db, tenant, "broken", "not a cron", &due)

	_, _, err := repo.Get(ctx, tenant, "broken")
	require.Error(t, err)
	assert.Contains(t, err.Error(), tenant)
	assert.Contains(t, err.Error(), "broken")

	_, err = repo.List(ctx, tenant)
	require.Error(t, err)
	assert.Contains(t, err.Error(), tenant)
	assert.Contains(t, err.Error(), "broken")

	u := scheduleUnitOfWork(db, parser)
	require.NoError(t, u.Begin(ctx))
	defer func() { _ = u.Rollback() }()
	_, err = u.Schedules().LockDue(ctx, due.Add(time.Hour), 1000)
	require.Error(t, err)
	assert.Contains(t, err.Error(), tenant)
	assert.Contains(t, err.Error(), "broken")
}

// A repository with no parser cannot rebuild a schedule's expression; it says
// so instead of failing on a missing parser.
func TestSchedules_WithoutACronParserALoadIsRefused(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := scheduleRepoTenant(t, db)
	rawSchedule(t, db, tenant, "nightly", "0 * * * *", nil)
	u := postgres.NewPostgresUnitOfWork(db, ports.SystemClock{}, discardLogger())

	_, _, err := u.Schedules().Get(ctx, tenant, "nightly")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cron parser")
	_, ok, err := u.Schedules().Get(ctx, tenant, "absent")
	require.NoError(t, err, "a schedule that is not stored needs no parser")
	assert.False(t, ok)
}

func TestSchedules_GetAndListAreScopedToATenant(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	repo := postgres.NewScheduleRepository(db, nil, parser)
	tenant, other := scheduleRepoTenant(t, db), scheduleRepoTenant(t, db)
	created := time.Date(2026, 10, 28, 8, 30, 0, 0, time.UTC)
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
	repo := postgres.NewScheduleRepository(db, nil, parser)
	tenant, other := scheduleRepoTenant(t, db), scheduleRepoTenant(t, db)
	created := time.Date(2026, 10, 28, 8, 30, 0, 0, time.UTC)
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
// saves is its own until it commits, and a rollback discards it.
func TestSchedules_TheUnitOfWorkBindsTheRepositoryToItsTransaction(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	parser := scheduleParser(t)
	pool := postgres.NewScheduleRepository(db, nil, parser)
	tenant := scheduleRepoTenant(t, db)
	created := time.Date(2026, 10, 28, 8, 30, 0, 0, time.UTC)
	u := scheduleUnitOfWork(db, parser)
	require.NoError(t, u.Begin(ctx))
	defer func() { _ = u.Rollback() }()
	require.NoError(t, u.Schedules().Save(ctx, hourlySchedule(t, parser, tenant, "kept", created), 0))
	due := time.Date(1985, 1, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, u.Schedules().Save(ctx, dueSchedule(t, parser, tenant, "due", due), 0))

	_, ok, err := pool.Get(ctx, tenant, "kept")
	require.NoError(t, err)
	assert.False(t, ok, "the pool does not see a schedule the open transaction saved")
	_, ok, err = u.Schedules().Get(ctx, tenant, "kept")
	require.NoError(t, err)
	assert.True(t, ok, "the transaction sees its own save")
	locked, err := u.Schedules().LockDue(ctx, due.Add(time.Hour), 10)
	require.NoError(t, err)
	assert.Equal(t, []string{"due"}, namesOf(tenant, locked), "LockDue runs in the transaction")

	require.NoError(t, u.Commit())
	mustGet(t, pool, tenant, "kept")

	require.NoError(t, u.Begin(ctx))
	require.NoError(t, u.Schedules().Save(ctx, hourlySchedule(t, parser, tenant, "dropped", created), 0))
	require.NoError(t, u.Rollback())
	_, ok, err = pool.Get(ctx, tenant, "dropped")
	require.NoError(t, err)
	assert.False(t, ok, "a rollback discards the save")
}
