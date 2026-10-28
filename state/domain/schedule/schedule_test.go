package schedule_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/state/domain/schedule"
)

// everyHour fires on the hour.
type everyHour struct{}

func (everyHour) Next(after time.Time) time.Time {
	return after.Truncate(time.Hour).Add(time.Hour)
}

// everyMinute fires on the minute and counts its calls.
type everyMinute struct{ calls *int }

func (e everyMinute) Next(after time.Time) time.Time {
	*e.calls++
	return after.Truncate(time.Minute).Add(time.Minute)
}

// dailyAt fires once a day at the given hour, UTC.
type dailyAt struct{ hour int }

func (d dailyAt) Next(after time.Time) time.Time {
	slot := time.Date(after.Year(), after.Month(), after.Day(), d.hour, 0, 0, 0, time.UTC)
	if !slot.After(after) {
		slot = slot.AddDate(0, 0, 1)
	}
	return slot
}

// never has no fire after any instant.
type never struct{}

func (never) Next(time.Time) time.Time { return time.Time{} }

// stuck answers the instant it was given, a contract violation that a
// misbehaving expression could commit.
type stuck struct{}

func (stuck) Next(after time.Time) time.Time { return after }

func at(hour, minute int) time.Time {
	return time.Date(2026, 10, 28, hour, minute, 0, 0, time.UTC)
}

func newHourly(t *testing.T, now time.Time) *schedule.Schedule {
	t.Helper()
	return schedule.New("default", "daily", "0 * * * *", "UTC", "every hour", false, everyHour{}, "alice@example.com", now)
}

func TestSchedule_NewStartsAtVersionOneWithTheNextFire(t *testing.T) {
	now := at(0, 30)
	s := newHourly(t, now)

	assert.Equal(t, "default", s.TenantID())
	assert.Equal(t, "daily", s.Name())
	assert.Equal(t, "0 * * * *", s.Cron())
	assert.Equal(t, "UTC", s.Timezone())
	assert.Equal(t, "every hour", s.Description())
	assert.False(t, s.Paused())
	assert.Equal(t, int64(1), s.Version())
	assert.Equal(t, "alice@example.com", s.UpdatedBy())
	assert.True(t, now.Equal(s.UpdatedAt()))
	next, ok := s.NextFireAt()
	require.True(t, ok)
	assert.True(t, at(1, 0).Equal(next), "next fire: got %s", next)
}

func TestSchedule_DueIsTheLatestMissedSlot(t *testing.T) {
	// The loop last ran before 00:55, so the schedule's next fire is 01:00;
	// the process comes back at 03:10.
	s := newHourly(t, at(0, 55))

	slot, skipped, ok := s.Due(at(3, 10))

	require.True(t, ok)
	assert.True(t, at(3, 0).Equal(slot), "slot: got %s want 03:00", slot)
	assert.Equal(t, 2, skipped, "the 01:00 and 02:00 slots are skipped")
}

func TestSchedule_DueIsOpenFromTheNextFireOn(t *testing.T) {
	s := newHourly(t, at(0, 55))

	_, _, ok := s.Due(at(0, 59))
	assert.False(t, ok, "before the next fire nothing is due")

	slot, skipped, ok := s.Due(at(1, 0))
	require.True(t, ok, "a slot is due at the instant it names")
	assert.True(t, at(1, 0).Equal(slot))
	assert.Equal(t, 0, skipped)
}

func TestSchedule_FiredMovesTheNextFireAfterTheSlot(t *testing.T) {
	s := newHourly(t, at(0, 55))

	s.Fired(at(3, 0))

	next, ok := s.NextFireAt()
	require.True(t, ok)
	assert.True(t, at(4, 0).Equal(next), "next fire: got %s want 04:00", next)
	_, _, due := s.Due(at(3, 59))
	assert.False(t, due, "the fired slot is not due again")
}

func TestSchedule_FiredLeavesVersionAndAuthorship(t *testing.T) {
	created := at(0, 55)
	s := newHourly(t, created)
	s.Replace("0 * * * *", "UTC", "every hour", false, everyHour{}, "bob@example.com", at(1, 5))
	require.Equal(t, int64(2), s.Version())

	s.Fired(at(2, 0))

	assert.Equal(t, int64(2), s.Version(), "a fire is not an operator edit")
	assert.Equal(t, "bob@example.com", s.UpdatedBy())
	assert.True(t, at(1, 5).Equal(s.UpdatedAt()))
}

func TestSchedule_ReplaceCountsFromNow(t *testing.T) {
	s := newHourly(t, at(0, 55))
	require.Equal(t, int64(1), s.Version())

	s.Replace("0 7 * * *", "UTC", "daily at seven", false, dailyAt{hour: 7}, "bob@example.com", at(5, 10))

	next, ok := s.NextFireAt()
	require.True(t, ok)
	assert.True(t, at(7, 0).Equal(next), "next fire counts from now, not from the old slot: got %s", next)
	assert.Equal(t, int64(2), s.Version())
	assert.Equal(t, "bob@example.com", s.UpdatedBy())
	assert.True(t, at(5, 10).Equal(s.UpdatedAt()))
	assert.Equal(t, "0 7 * * *", s.Cron())
	assert.Equal(t, "daily at seven", s.Description())
}

func TestSchedule_ReplaceOnASlotCountsStrictlyAfterIt(t *testing.T) {
	s := newHourly(t, at(0, 55))

	s.Replace("0 * * * *", "UTC", "", false, everyHour{}, "bob@example.com", at(6, 0))

	next, ok := s.NextFireAt()
	require.True(t, ok)
	assert.True(t, at(7, 0).Equal(next), "a save at 06:00 does not fire the 06:00 slot: got %s", next)
}

func TestSchedule_ReplaceKeepsTheVersionCountingUp(t *testing.T) {
	s := newHourly(t, at(0, 55))

	for want := int64(2); want <= 4; want++ {
		s.Replace("0 * * * *", "UTC", "", false, everyHour{}, "bob@example.com", at(1, 0))
		assert.Equal(t, want, s.Version())
	}
}

func TestSchedule_PausedHasNoNextFire(t *testing.T) {
	s := schedule.New("default", "daily", "0 * * * *", "UTC", "", true, everyHour{}, "alice@example.com", at(0, 30))

	_, ok := s.NextFireAt()
	assert.False(t, ok)
	assert.True(t, s.Paused())
	_, _, due := s.Due(at(23, 0))
	assert.False(t, due, "a paused schedule is never due")

	s.Fired(at(1, 0))
	_, ok = s.NextFireAt()
	assert.False(t, ok, "a fire does not wake a paused schedule")

	s.Replace("0 * * * *", "UTC", "", false, everyHour{}, "bob@example.com", at(2, 30))
	next, ok := s.NextFireAt()
	require.True(t, ok, "a resume counts from the save")
	assert.True(t, at(3, 0).Equal(next))

	s.Replace("0 * * * *", "UTC", "", true, everyHour{}, "bob@example.com", at(3, 30))
	_, ok = s.NextFireAt()
	assert.False(t, ok, "a pause takes the next fire away")
}

func TestSchedule_AnExpressionWithNoFurtherFireLeavesNoNextFire(t *testing.T) {
	s := schedule.New("default", "once", "0 0 30 2 *", "UTC", "", false, never{}, "alice@example.com", at(0, 30))
	_, ok := s.NextFireAt()
	assert.False(t, ok)

	s = newHourly(t, at(0, 30))
	s.Replace("0 0 30 2 *", "UTC", "", false, never{}, "bob@example.com", at(1, 30))
	_, ok = s.NextFireAt()
	assert.False(t, ok)
}

func TestSchedule_DueSurvivesAnExpressionThatDoesNotAdvance(t *testing.T) {
	s := schedule.Rehydrate("default", "daily", "0 * * * *", "UTC", "", false, ptr(at(1, 0)), 1, "alice@example.com", at(0, 0), stuck{})

	slot, skipped, ok := s.Due(at(9, 0))

	require.True(t, ok)
	assert.True(t, at(1, 0).Equal(slot))
	assert.Equal(t, 0, skipped)
}

func TestSchedule_DueStaysCheapWhenManySlotsWereMissed(t *testing.T) {
	// A per-minute schedule that was down for a year has over 500,000 slots
	// behind it.
	var calls int
	nextFire := time.Date(2025, 10, 28, 0, 0, 0, 0, time.UTC)
	s := schedule.Rehydrate("default", "busy", "* * * * *", "UTC", "", false, &nextFire, 1, "alice@example.com", nextFire, everyMinute{calls: &calls})
	now := time.Date(2026, 10, 28, 3, 10, 30, 0, time.UTC)

	slot, skipped, ok := s.Due(now)

	require.True(t, ok)
	assert.True(t, time.Date(2026, 10, 28, 3, 10, 0, 0, time.UTC).Equal(slot), "slot: got %s want 03:10", slot)
	assert.Equal(t, 1000, skipped, "the count saturates at 1000")
	assert.Less(t, calls, 1200, "the walk is bounded; Next ran %d times", calls)
}

func TestSchedule_DueCountsExactlyUpToItsLimit(t *testing.T) {
	// Slots are minutes from 00:00; the loop was down until slot index
	// `missed` (zero-based) came due, so `missed` earlier slots are skipped.
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		missed      int
		wantSkipped int
	}{
		{missed: 0, wantSkipped: 0},
		{missed: 1, wantSkipped: 1},
		{missed: 999, wantSkipped: 999},
		{missed: 1000, wantSkipped: 1000},
		{missed: 1001, wantSkipped: 1000},
		{missed: 1002, wantSkipped: 1000},
		{missed: 2047, wantSkipped: 1000},
		{missed: 100000, wantSkipped: 1000},
	}
	for _, tc := range cases {
		var calls int
		first := start
		s := schedule.Rehydrate("default", "busy", "* * * * *", "UTC", "", false, &first, 1, "alice@example.com", start, everyMinute{calls: &calls})
		latest := start.Add(time.Duration(tc.missed) * time.Minute)

		slot, skipped, ok := s.Due(latest.Add(30 * time.Second))

		require.True(t, ok, "missed %d", tc.missed)
		assert.True(t, latest.Equal(slot), "missed %d: slot got %s want %s", tc.missed, slot, latest)
		assert.Equal(t, tc.wantSkipped, skipped, "missed %d", tc.missed)
	}
}

func TestSchedule_RehydrateRestoresTheStoredFields(t *testing.T) {
	next := at(1, 0)
	updated := at(0, 10)

	s := schedule.Rehydrate("default", "daily", "0 * * * *", "Europe/Paris", "every hour", false, &next, 7, "alice@example.com", updated, everyHour{})

	assert.Equal(t, "default", s.TenantID())
	assert.Equal(t, "daily", s.Name())
	assert.Equal(t, "0 * * * *", s.Cron())
	assert.Equal(t, "Europe/Paris", s.Timezone())
	assert.Equal(t, "every hour", s.Description())
	assert.False(t, s.Paused())
	assert.Equal(t, int64(7), s.Version())
	assert.Equal(t, "alice@example.com", s.UpdatedBy())
	assert.True(t, updated.Equal(s.UpdatedAt()))
	got, ok := s.NextFireAt()
	require.True(t, ok)
	assert.True(t, next.Equal(got))

	slot, skipped, due := s.Due(at(2, 30))
	require.True(t, due)
	assert.True(t, at(2, 0).Equal(slot))
	assert.Equal(t, 1, skipped)
}

func TestSchedule_RehydratePausedHasNoNextFireWhateverWasStored(t *testing.T) {
	next := at(1, 0)

	s := schedule.Rehydrate("default", "daily", "0 * * * *", "UTC", "", true, &next, 3, "alice@example.com", at(0, 10), everyHour{})

	_, ok := s.NextFireAt()
	assert.False(t, ok)
	_, _, due := s.Due(at(9, 0))
	assert.False(t, due)
}

func TestSchedule_RehydrateWithoutANextFireIsNeverDue(t *testing.T) {
	s := schedule.Rehydrate("default", "daily", "0 * * * *", "UTC", "", false, nil, 3, "alice@example.com", at(0, 10), everyHour{})

	_, ok := s.NextFireAt()
	assert.False(t, ok)
	_, _, due := s.Due(at(9, 0))
	assert.False(t, due)
}

func TestSchedule_NextFireAtIsACopy(t *testing.T) {
	next := at(1, 0)
	s := schedule.Rehydrate("default", "daily", "0 * * * *", "UTC", "", false, &next, 3, "alice@example.com", at(0, 10), everyHour{})

	next = at(23, 0)

	got, ok := s.NextFireAt()
	require.True(t, ok)
	assert.True(t, at(1, 0).Equal(got), "the aggregate owns its next fire: got %s", got)
}

func ptr[T any](v T) *T { return &v }
