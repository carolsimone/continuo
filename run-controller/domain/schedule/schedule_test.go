package schedule_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/run-controller/domain/schedule"
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

// interval fires every d of elapsed time, as an `@every` interval does: its
// fire after an instant is d after the whole second at or before it, so from
// its second fire on it fires every d, and its first fire after an instant that
// is not a fire lies off those. period is what it reports as its Period. It
// counts its calls of Next.
type interval struct {
	d, period time.Duration
	calls     *int
}

func every(d time.Duration, calls *int) interval { return interval{d: d, period: d, calls: calls} }

func (e interval) Next(after time.Time) time.Time {
	*e.calls++
	return after.Add(e.d - time.Duration(after.Nanosecond()))
}

func (e interval) Period() time.Duration { return e.period }

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
	s, err := schedule.New("default", "daily", "0 * * * *", "UTC", "every hour", false, everyHour{}, "alice@example.com", now)
	require.NoError(t, err)
	return s
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

// An edit over a stored row that can no longer be rebuilt keeps the row's
// identity and version alone: it is at the version after the stored one, with
// the edit's fields and author, and its next fire counts from now; none while
// it is paused.
func TestSchedule_ReplaceUnreadableRaisesTheStoredVersion(t *testing.T) {
	now := at(0, 30)

	s, err := schedule.ReplaceUnreadable("default", "daily", 5, "0 * * * *", "UTC", "every hour", false, everyHour{}, "bob@example.com", now)
	require.NoError(t, err)

	assert.Equal(t, "default", s.TenantID())
	assert.Equal(t, "daily", s.Name())
	assert.Equal(t, "0 * * * *", s.Cron())
	assert.Equal(t, "UTC", s.Timezone())
	assert.Equal(t, "every hour", s.Description())
	assert.False(t, s.Paused())
	assert.Equal(t, int64(6), s.Version())
	assert.Equal(t, "bob@example.com", s.UpdatedBy())
	assert.True(t, now.Equal(s.UpdatedAt()))
	next, ok := s.NextFireAt()
	require.True(t, ok)
	assert.True(t, at(1, 0).Equal(next), "next fire: got %s", next)

	paused, err := schedule.ReplaceUnreadable("default", "daily", 5, "0 * * * *", "UTC", "", true, everyHour{}, "bob@example.com", now)
	require.NoError(t, err)
	assert.Equal(t, int64(6), paused.Version())
	assert.True(t, paused.Paused())
	_, ok = paused.NextFireAt()
	assert.False(t, ok, "a paused schedule has no next fire")
}

// A schedule is identified by its tenant and name and fires through its
// expression, so New and ReplaceUnreadable refuse a schedule with none of them
// rather than build one that cannot be stored or fired.
func TestSchedule_NewRefusesWhatCannotBeStoredOrFired(t *testing.T) {
	now := at(0, 30)
	for name, tc := range map[string]struct {
		tenant, name string
		expr         schedule.Expression
		want         error
	}{
		"no tenant":     {"", "daily", everyHour{}, schedule.ErrTenantRequired},
		"no name":       {"default", "", everyHour{}, schedule.ErrNameRequired},
		"no expression": {"default", "daily", nil, schedule.ErrExpressionRequired},
	} {
		t.Run(name, func(t *testing.T) {
			s, err := schedule.New(tc.tenant, tc.name, "0 * * * *", "UTC", "", false, tc.expr, "alice@example.com", now)
			require.ErrorIs(t, err, tc.want)
			assert.Nil(t, s)

			s, err = schedule.ReplaceUnreadable(tc.tenant, tc.name, 5, "0 * * * *", "UTC", "", false, tc.expr, "alice@example.com", now)
			require.ErrorIs(t, err, tc.want)
			assert.Nil(t, s)
		})
	}
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
	s, err := schedule.New("default", "daily", "0 * * * *", "UTC", "", true, everyHour{}, "alice@example.com", at(0, 30))
	require.NoError(t, err)

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
	s, err := schedule.New("default", "once", "0 0 30 2 *", "UTC", "", false, never{}, "alice@example.com", at(0, 30))
	require.NoError(t, err)
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

// An interval more than 1000 slots overdue fires the latest slot on its own
// interval, counted from the stored next fire, not a slot counted from some
// instant between two of its fires, and its next fire is one interval later.
// It costs a few calls of Next, however many slots were missed.
func TestSchedule_DueOfAnIntervalPastAThousandSlotsStaysOnItsInterval(t *testing.T) {
	var calls int
	first := at(0, 0)
	s := schedule.Rehydrate("default", "busy", "@every 1m", "UTC", "", false, &first, 1, "alice@example.com", first, every(time.Minute, &calls))

	slot, skipped, ok := s.Due(time.Date(2026, 10, 28, 16, 42, 30, 0, time.UTC))

	require.True(t, ok)
	assert.True(t, at(16, 42).Equal(slot), "slot: got %s want 16:42:00", slot)
	assert.Equal(t, 1000, skipped, "the count saturates at 1000")
	assert.Less(t, calls, 10, "the slot is counted, not walked; Next ran %d times", calls)
	s.Fired(slot)
	next, ok := s.NextFireAt()
	require.True(t, ok)
	assert.True(t, at(16, 43).Equal(next), "next fire: got %s want 16:43:00", next)
}

// An interval's slots count from the stored next fire: after it, every slot is
// a whole number of intervals after its second fire. The count of skipped
// slots is exact up to 1000 and saturates there, as it does for any other
// expression.
func TestSchedule_DueOfAnIntervalCountsExactlyUpToItsLimit(t *testing.T) {
	first := time.Date(2026, 10, 1, 0, 0, 0, 250*int(time.Millisecond), time.UTC)
	second := time.Date(2026, 10, 1, 0, 1, 0, 0, time.UTC)
	cases := []struct {
		name        string
		now         time.Time
		wantSlot    time.Time
		wantSkipped int
	}{
		{"at the stored next fire", first, first, 0},
		{"before the second fire", second.Add(-time.Nanosecond), first, 0},
		{"at the second fire", second, second, 1},
		{"five slots on", second.Add(4*time.Minute + 30*time.Second), second.Add(4 * time.Minute), 5},
		{"999 slots on", second.Add(998 * time.Minute), second.Add(998 * time.Minute), 999},
		{"1000 slots on", second.Add(999*time.Minute + time.Second), second.Add(999 * time.Minute), 1000},
		{"1001 slots on", second.Add(1000 * time.Minute), second.Add(1000 * time.Minute), 1000},
		{"a million slots on", second.Add(999999*time.Minute + 59*time.Second), second.Add(999999 * time.Minute), 1000},
	}
	for _, tc := range cases {
		var calls int
		next := first
		s := schedule.Rehydrate("default", "busy", "@every 1m", "UTC", "", false, &next, 1, "alice@example.com", first, every(time.Minute, &calls))

		slot, skipped, ok := s.Due(tc.now)

		require.True(t, ok, tc.name)
		assert.True(t, tc.wantSlot.Equal(slot), "%s: slot got %s want %s", tc.name, slot, tc.wantSlot)
		assert.Equal(t, tc.wantSkipped, skipped, tc.name)
		assert.Less(t, calls, 10, "%s: Next ran %d times", tc.name, calls)
	}
}

// A span longer than a time.Duration holds, about 292 years, still reaches the
// latest slot of an interval.
func TestSchedule_DueOfAnIntervalSpansCenturies(t *testing.T) {
	var calls int
	first := time.Date(1700, 1, 1, 0, 0, 0, 0, time.UTC)
	s := schedule.Rehydrate("default", "busy", "@every 1m", "UTC", "", false, &first, 1, "alice@example.com", first, every(time.Minute, &calls))

	slot, skipped, ok := s.Due(time.Date(2026, 10, 28, 16, 42, 30, 0, time.UTC))

	require.True(t, ok)
	assert.True(t, at(16, 42).Equal(slot), "slot: got %s want 2026-10-28 16:42:00", slot)
	assert.Equal(t, 1000, skipped)
}

// An interval that reports no period above zero cannot be counted, so its
// slots are found as any other expression's are: walked, then searched for.
func TestSchedule_DueOfAnIntervalWithNoPeriodIsWalked(t *testing.T) {
	var calls int
	first := at(0, 0)
	grid := interval{d: time.Minute, calls: &calls}
	s := schedule.Rehydrate("default", "busy", "@every 1m", "UTC", "", false, &first, 1, "alice@example.com", first, grid)

	slot, skipped, ok := s.Due(at(0, 5))

	require.True(t, ok)
	assert.True(t, at(0, 5).Equal(slot), "slot: got %s", slot)
	assert.Equal(t, 5, skipped)
	assert.Equal(t, 6, calls, "each slot is walked")
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
