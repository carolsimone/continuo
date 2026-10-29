// Package repositorytest holds contract suites for the repository ports of the
// state domain, so an in-memory fake and the adapter behind a port are held to
// the same behaviour by the same assertions.
package repositorytest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repository "github.com/carolsimone/continuo/run-controller/domain/repository"
	"github.com/carolsimone/continuo/run-controller/domain/schedule"
)

// CronParser reads a cron in a timezone, as the suite needs it to build
// schedules.
type CronParser interface {
	Parse(cron, timezone string) (schedule.Expression, error)
}

// PlantedSchedule is a row for a ScheduleStore to hold exactly as given: its
// cron and timezone need not parse, and its version need not be one an edit
// would have reached.
type PlantedSchedule struct {
	TenantID, Name, Cron, Timezone string
	Paused                         bool
	NextFireAt                     *time.Time
	Version                        int64
}

// ScheduleStore is a repository.ScheduleRepository the suite can also fill with
// rows no caller could save, such as one whose cron the parser refuses. A store
// is empty of the suite's tenants and has a transaction open, so LoadForUpdate
// works; whatever the suite writes is gone when its test ends.
type ScheduleStore interface {
	repository.ScheduleRepository
	// Plant stores row as given, in the same scope the repository reads.
	Plant(t *testing.T, row PlantedSchedule)
}

const (
	hourly  = "0 * * * *"
	nightly = "30 2 * * *"
	utc     = "UTC"
	paris   = "Europe/Paris"
	garbled = "not a cron"
	badZone = "Mars/Olympus"
)

// day is an instant of February 1901. The suite keeps every next fire in that
// month and asks for the schedules due or next after dates in it, so rows other
// tests leave in a shared database, none of them that old, are never in an
// answer.
func day(d, hour int) time.Time {
	return time.Date(1901, time.February, d, hour, 0, 0, 0, time.UTC)
}

func at(t time.Time) *time.Time { return &t }

// RunScheduleContract holds the implementation newStore builds to the behaviour
// every repository.ScheduleRepository shares. parser builds the schedules it
// saves and must refuse the crons "not a cron" and the timezone
// "Mars/Olympus"; each test gets a store and tenants of its own from newStore.
func RunScheduleContract(t *testing.T, parser CronParser, newStore func(t *testing.T) ScheduleStore) {
	t.Helper()
	ctx := context.Background()
	tenant := func() string { return "contract-" + uuid.NewString() }
	build := func(t *testing.T, tenantID, name, cron, zone string, created time.Time) *schedule.Schedule {
		t.Helper()
		expr, err := parser.Parse(cron, zone)
		require.NoError(t, err)
		s, err := schedule.New(tenantID, name, cron, zone, "for "+name, false, expr, "alice@example.com", created)
		require.NoError(t, err)
		return s
	}
	names := func(schedules []*schedule.Schedule) []string {
		out := make([]string, 0, len(schedules))
		for _, s := range schedules {
			out = append(out, s.Name())
		}
		return out
	}

	t.Run("a created schedule reads back as saved", func(t *testing.T) {
		store, tenantID := newStore(t), tenant()
		created := day(1, 0)
		require.NoError(t, store.Save(ctx, build(t, tenantID, "nightly", nightly, paris, created), 0))

		got, ok, err := store.Get(ctx, tenantID, "nightly")

		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, tenantID, got.TenantID())
		assert.Equal(t, "nightly", got.Name())
		assert.Equal(t, nightly, got.Cron())
		assert.Equal(t, paris, got.Timezone())
		assert.Equal(t, "for nightly", got.Description())
		assert.False(t, got.Paused())
		assert.Equal(t, int64(1), got.Version())
		assert.Equal(t, "alice@example.com", got.UpdatedBy())
		assert.True(t, created.Equal(got.UpdatedAt()), "edit time: got %s", got.UpdatedAt())
		next, ok := got.NextFireAt()
		require.True(t, ok)
		assert.True(t, next.After(created), "next fire: got %s", next)

		_, ok, err = store.Get(ctx, tenantID, "absent")
		require.NoError(t, err)
		assert.False(t, ok)
		_, ok, err = store.Get(ctx, tenant(), "nightly")
		require.NoError(t, err)
		assert.False(t, ok, "another tenant's schedule of that name is not this tenant's")
	})

	t.Run("a save is stale unless the stored row is at the version it names", func(t *testing.T) {
		store, tenantID := newStore(t), tenant()
		require.NoError(t, store.Save(ctx, build(t, tenantID, "daily", hourly, utc, day(1, 0)), 0))
		err := store.Save(ctx, build(t, tenantID, "daily", hourly, utc, day(1, 1)), 0)
		require.ErrorIs(t, err, schedule.ErrStaleVersion, "version 0 only creates")

		first, _, err := store.Get(ctx, tenantID, "daily")
		require.NoError(t, err)
		second, _, err := store.Get(ctx, tenantID, "daily")
		require.NoError(t, err)
		expr, err := parser.Parse(nightly, paris)
		require.NoError(t, err)
		first.Replace(nightly, paris, "first editor", false, expr, "bob@example.com", day(2, 0))
		second.Replace(nightly, paris, "second editor", false, expr, "carol@example.com", day(2, 1))
		require.NoError(t, store.Save(ctx, first, 1))
		require.ErrorIs(t, store.Save(ctx, second, 1), schedule.ErrStaleVersion, "the row moved on to version 2")
		got, _, err := store.Get(ctx, tenantID, "daily")
		require.NoError(t, err)
		assert.Equal(t, int64(2), got.Version())
		assert.Equal(t, "first editor", got.Description())
		assert.Equal(t, "bob@example.com", got.UpdatedBy())

		absent := build(t, tenantID, "absent", hourly, utc, day(1, 0))
		require.ErrorIs(t, store.Save(ctx, absent, 1), schedule.ErrStaleVersion, "no row is at version 1")
	})

	t.Run("a save stores the version, author and edit time as the schedule holds them", func(t *testing.T) {
		store, tenantID := newStore(t), tenant()
		require.NoError(t, store.Save(ctx, build(t, tenantID, "daily", hourly, utc, day(1, 0)), 0))
		s, _, err := store.Get(ctx, tenantID, "daily")
		require.NoError(t, err)
		slot, _ := s.NextFireAt()

		s.Fired(slot)
		require.NoError(t, store.Save(ctx, s, s.Version()))

		got, _, err := store.Get(ctx, tenantID, "daily")
		require.NoError(t, err)
		assert.Equal(t, int64(1), got.Version(), "a fire leaves the version")
		assert.Equal(t, "alice@example.com", got.UpdatedBy(), "a fire leaves the author")
		assert.True(t, day(1, 0).Equal(got.UpdatedAt()), "a fire leaves the edit time")
		next, ok := got.NextFireAt()
		require.True(t, ok)
		assert.True(t, next.After(slot), "the next fire moved past the slot: %s", next)
	})

	t.Run("exists and delete tell whether there was a row, whatever its cron", func(t *testing.T) {
		store, tenantID := newStore(t), tenant()
		exists, err := store.Exists(ctx, tenantID, "daily")
		require.NoError(t, err)
		assert.False(t, exists)
		deleted, err := store.Delete(ctx, tenantID, "daily")
		require.NoError(t, err)
		assert.False(t, deleted)

		require.NoError(t, store.Save(ctx, build(t, tenantID, "daily", hourly, utc, day(1, 0)), 0))
		store.Plant(t, PlantedSchedule{TenantID: tenantID, Name: "broken", Cron: garbled, Timezone: utc, Version: 2})
		for _, name := range []string{"daily", "broken"} {
			exists, err = store.Exists(ctx, tenantID, name)
			require.NoError(t, err)
			assert.True(t, exists, name)
			deleted, err = store.Delete(ctx, tenantID, name)
			require.NoError(t, err)
			assert.True(t, deleted, name)
			exists, err = store.Exists(ctx, tenantID, name)
			require.NoError(t, err)
			assert.False(t, exists, name)
		}
	})

	t.Run("a row whose cron or timezone no longer parses fails a read and is saved over", func(t *testing.T) {
		store, tenantID := newStore(t), tenant()
		store.Plant(t, PlantedSchedule{TenantID: tenantID, Name: "broken-cron", Cron: garbled, Timezone: utc, Version: 4})
		store.Plant(t, PlantedSchedule{TenantID: tenantID, Name: "broken-zone", Cron: hourly, Timezone: badZone, Version: 7})

		for name, version := range map[string]int64{"broken-cron": 4, "broken-zone": 7} {
			for read, load := range map[string]func() (*schedule.Schedule, bool, error){
				"Get":           func() (*schedule.Schedule, bool, error) { return store.Get(ctx, tenantID, name) },
				"LoadForUpdate": func() (*schedule.Schedule, bool, error) { return store.LoadForUpdate(ctx, tenantID, name) },
			} {
				got, ok, err := load()
				require.ErrorIs(t, err, schedule.ErrStoredCronInvalid, "%s of %s", read, name)
				var invalid *schedule.StoredCronInvalidError
				require.True(t, errors.As(err, &invalid), "%s of %s", read, name)
				assert.Equal(t, version, invalid.Version, "the error carries the row's version")
				assert.Equal(t, name, invalid.Name)
				assert.False(t, ok)
				assert.Nil(t, got)
			}
		}

		expr, err := parser.Parse(hourly, utc)
		require.NoError(t, err)
		repaired, err := schedule.ReplaceUnreadable(tenantID, "broken-cron", 4, hourly, utc, "repaired", false, expr, "bob@example.com", day(1, 0))
		require.NoError(t, err)
		require.NoError(t, store.Save(ctx, repaired, 4))
		got, ok, err := store.Get(ctx, tenantID, "broken-cron")
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, int64(5), got.Version(), "the repair raised the stored version")
		assert.Equal(t, "repaired", got.Description())
	})

	t.Run("a load of an absent schedule for update finds nothing", func(t *testing.T) {
		store, tenantID := newStore(t), tenant()
		got, ok, err := store.LoadForUpdate(ctx, tenantID, "absent")
		require.NoError(t, err)
		assert.False(t, ok)
		assert.Nil(t, got)

		require.NoError(t, store.Save(ctx, build(t, tenantID, "daily", hourly, utc, day(1, 0)), 0))
		got, ok, err = store.LoadForUpdate(ctx, tenantID, "daily")
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, "daily", got.Name())
	})

	t.Run("a list names the schedules that parse apart from those that do not, byte by byte", func(t *testing.T) {
		store, tenantID, other := newStore(t), tenant(), tenant()
		for _, name := range []string{"b-daily", "Z-daily", "a-daily"} {
			require.NoError(t, store.Save(ctx, build(t, tenantID, name, hourly, utc, day(1, 0)), 0))
			store.Plant(t, PlantedSchedule{TenantID: tenantID, Name: name + "-broken", Cron: garbled, Timezone: utc, Version: 1})
		}
		store.Plant(t, PlantedSchedule{TenantID: tenantID, Name: "zone-broken", Cron: hourly, Timezone: badZone, Version: 1})
		store.Plant(t, PlantedSchedule{TenantID: other, Name: "theirs-broken", Cron: garbled, Timezone: utc, Version: 1})

		listed, unreadable, err := store.ListWithUnreadable(ctx, tenantID)

		require.NoError(t, err)
		assert.Equal(t, []string{"Z-daily", "a-daily", "b-daily"}, names(listed))
		assert.Equal(t, []string{"Z-daily-broken", "a-daily-broken", "b-daily-broken", "zone-broken"}, unreadable)

		theirs, theirsUnreadable, err := store.ListWithUnreadable(ctx, other)
		require.NoError(t, err)
		assert.Empty(t, theirs)
		assert.Equal(t, []string{"theirs-broken"}, theirsUnreadable)

		none, noneUnreadable, err := store.ListWithUnreadable(ctx, tenant())
		require.NoError(t, err)
		assert.Empty(t, none)
		assert.NotNil(t, noneUnreadable, "no unreadable name is an empty list")
		assert.Empty(t, noneUnreadable)
	})

	t.Run("the due schedules are the unpaused ones whose next fire has come, oldest first", func(t *testing.T) {
		store, tenantID := newStore(t), tenant()
		store.Plant(t, PlantedSchedule{TenantID: tenantID, Name: "late", Cron: hourly, Timezone: utc, NextFireAt: at(day(3, 0)), Version: 1})
		store.Plant(t, PlantedSchedule{TenantID: tenantID, Name: "early", Cron: hourly, Timezone: utc, NextFireAt: at(day(2, 0)), Version: 1})
		store.Plant(t, PlantedSchedule{TenantID: tenantID, Name: "future", Cron: hourly, Timezone: utc, NextFireAt: at(day(20, 0)), Version: 1})
		store.Plant(t, PlantedSchedule{TenantID: tenantID, Name: "paused", Cron: hourly, Timezone: utc, Paused: true, NextFireAt: at(day(1, 0)), Version: 1})
		store.Plant(t, PlantedSchedule{TenantID: tenantID, Name: "never", Cron: hourly, Timezone: utc, Version: 1})
		store.Plant(t, PlantedSchedule{TenantID: tenantID, Name: "broken", Cron: garbled, Timezone: utc, NextFireAt: at(day(1, 12)), Version: 1})
		now := day(10, 0)

		due, err := store.ListDue(ctx, now, 100)
		require.NoError(t, err)
		assert.Equal(t, []string{"early", "late"}, names(due), "not the paused, the future, the one with no next fire or the unparsable")

		one, err := store.ListDue(ctx, now, 1)
		require.NoError(t, err)
		assert.Equal(t, []string{"early"}, names(one), "the unparsable row, older still, takes none of the limit's places")

		exactly, err := store.ListDue(ctx, day(3, 0), 100)
		require.NoError(t, err)
		assert.Equal(t, []string{"early", "late"}, names(exactly), "a next fire at now is due")
	})

	t.Run("the earliest next fire after an instant skips paused schedules and counts unparsable ones", func(t *testing.T) {
		store, tenantID := newStore(t), tenant()
		store.Plant(t, PlantedSchedule{TenantID: tenantID, Name: "broken", Cron: garbled, Timezone: utc, NextFireAt: at(day(5, 0)), Version: 1})
		store.Plant(t, PlantedSchedule{TenantID: tenantID, Name: "paused", Cron: hourly, Timezone: utc, Paused: true, NextFireAt: at(day(3, 0)), Version: 1})
		store.Plant(t, PlantedSchedule{TenantID: tenantID, Name: "good", Cron: hourly, Timezone: utc, NextFireAt: at(day(7, 0)), Version: 1})

		got, ok, err := store.EarliestFireAfter(ctx, day(4, 0))
		require.NoError(t, err)
		require.True(t, ok)
		assert.True(t, day(5, 0).Equal(got), "the paused fire at the 3rd is not counted, the unparsable one at the 5th is: %s", got)

		got, ok, err = store.EarliestFireAfter(ctx, day(5, 0))
		require.NoError(t, err)
		require.True(t, ok)
		assert.True(t, day(7, 0).Equal(got), "a fire at the instant itself is not after it: %s", got)
	})

	t.Run("a count puts each schedule in one state: unparsable, else paused, else scheduled", func(t *testing.T) {
		store, tenantID := newStore(t), tenant()
		scheduledBefore, pausedBefore, unparsableBefore, err := store.Count(ctx)
		require.NoError(t, err)

		store.Plant(t, PlantedSchedule{TenantID: tenantID, Name: "daily", Cron: hourly, Timezone: utc, NextFireAt: at(day(5, 0)), Version: 1})
		store.Plant(t, PlantedSchedule{TenantID: tenantID, Name: "paused", Cron: hourly, Timezone: utc, Paused: true, Version: 1})
		store.Plant(t, PlantedSchedule{TenantID: tenantID, Name: "broken-cron", Cron: garbled, Timezone: utc, NextFireAt: at(day(5, 0)), Version: 1})
		store.Plant(t, PlantedSchedule{TenantID: tenantID, Name: "broken-zone", Cron: hourly, Timezone: badZone, Version: 1})
		store.Plant(t, PlantedSchedule{TenantID: tenantID, Name: "paused-broken", Cron: garbled, Timezone: utc, Paused: true, Version: 1})

		scheduled, paused, unparsable, err := store.Count(ctx)

		require.NoError(t, err)
		assert.Equal(t, 1, scheduled-scheduledBefore)
		assert.Equal(t, 1, paused-pausedBefore)
		assert.Equal(t, 3, unparsable-unparsableBefore)
	})
}
