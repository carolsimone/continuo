package cron_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/run-controller/domain/schedule"
)

// fireDates drives a schedule the way the cron loop does: the clock advances a
// minute at a time, and every pass that finds a slot due fires it. It returns
// the number of fires per local calendar day of the schedule's zone.
func fireDates(t *testing.T, s *schedule.Schedule, loc *time.Location, from, to time.Time) map[string]int {
	t.Helper()
	fires := map[string]int{}
	for now := from; now.Before(to); now = now.Add(time.Minute) {
		slot, _, ok := s.Due(now)
		if !ok {
			continue
		}
		fires[slot.In(loc).Format("2006-01-02")]++
		s.Fired(slot)
	}
	return fires
}

func TestSchedule_FiresOncePerDayAcrossBothDSTChanges(t *testing.T) {
	loc, err := time.LoadLocation("Europe/Paris")
	require.NoError(t, err)
	expr := mustParse(t, "30 2 * * *", "Europe/Paris")

	cases := []struct {
		name string
		from time.Time
		to   time.Time
		days []string
	}{
		{
			name: "the clocks go back on 2026-10-25 (02:00-03:00 repeats)",
			from: utc(2026, time.October, 23, 12, 0),
			to:   utc(2026, time.October, 27, 12, 0),
			days: []string{"2026-10-24", "2026-10-25", "2026-10-26", "2026-10-27"},
		},
		{
			name: "the clocks go forward on 2027-03-28 (02:30 does not exist)",
			from: utc(2027, time.March, 26, 12, 0),
			to:   utc(2027, time.March, 30, 12, 0),
			days: []string{"2027-03-27", "2027-03-28", "2027-03-29", "2027-03-30"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := schedule.New("default", "nightly", "30 2 * * *", "Europe/Paris", "", false, expr, "alice@example.com", tc.from)
			require.NoError(t, err)

			fires := fireDates(t, s, loc, tc.from, tc.to)

			for _, day := range tc.days {
				assert.Equal(t, 1, fires[day], "fires on %s (all: %v)", day, fires)
			}
			assert.Equal(t, 1, fires[tc.days[1]], "the transition day %s fires exactly once", tc.days[1])
		})
	}
}

// slotsAt walks the slots of expr from first, one call of Next at a time, and
// returns the slot n fires after first for each n of missed, in ascending
// order: the slots a walk over every missed slot reaches.
func slotsAt(t *testing.T, expr schedule.Expression, first time.Time, missed []int) map[int]time.Time {
	t.Helper()
	want := map[int]time.Time{}
	slot, walked := first, 0
	for _, n := range missed {
		for ; walked < n; walked++ {
			next := expr.Next(slot)
			if !next.After(slot) {
				t.Fatalf("the walk stops after %s", slot)
			}
			slot = next
		}
		want[n] = slot
	}
	return want
}

// A pass at any instant after an outage fires the slot a walk over every slot
// from the stored next fire reaches: the latest at or before the pass, with the
// slots before it counted up to 1000. The fire moves the next fire on to the
// slot after it, so the schedule stays on the slots its cron names. It holds
// for the five-field crons of every kind, a per-minute cron down for a year
// among them, for wall-clock crons across clock changes, and for `@every`
// intervals, whose slots count from the stored next fire, a sub-second one
// included. Each outage is tried with the pass at its latest slot, halfway to
// the slot after it and a nanosecond before that one.
func TestSchedule_DueFiresTheSlotAWalkOverEverySlotReaches(t *testing.T) {
	cases := []struct {
		spec, zone string
		first      time.Time
		missed     []int
	}{
		{"* * * * *", "UTC", utc(2025, time.October, 28, 0, 0), []int{1000, 1001, 365 * 24 * 60}},
		{"*/7 * * * *", "UTC", utc(2026, time.October, 1, 0, 0), []int{999, 1000, 1001, 1002, 2047, 100000}},
		{"0 3 * * *", "Europe/Paris", utc(2026, time.October, 20, 1, 0), []int{4, 5, 6, 1000, 1001, 1002, 5000}},
		{"0 * * * *", "Europe/Paris", utc(2026, time.October, 20, 0, 0), []int{1000, 1001, 5000}},
		{"@every 1m", "UTC", utc(2026, time.October, 28, 0, 0), []int{0, 1, 999, 1000, 1001, 1002, 1000000}},
		{"@every 90s", "UTC", utc(2026, time.October, 28, 0, 0), []int{1001, 1000000}},
		{"@every 1m", "UTC", utc(2026, time.October, 28, 0, 0).Add(250 * time.Millisecond), []int{0, 1, 1001, 1000000}},
	}
	for _, tc := range cases {
		expr := mustParse(t, tc.spec, tc.zone)
		want := slotsAt(t, expr, tc.first, tc.missed)
		for _, n := range tc.missed {
			latest := want[n]
			after := expr.Next(latest)
			for _, now := range []time.Time{latest, latest.Add(after.Sub(latest) / 2), after.Add(-time.Nanosecond)} {
				name := fmt.Sprintf("%s in %s from %s, %d slots missed, at %s", tc.spec, tc.zone,
					tc.first.Format(time.RFC3339Nano), n, now.Format(time.RFC3339Nano))
				first := tc.first
				s := schedule.Rehydrate("default", "busy", tc.spec, tc.zone, "", false, &first, 1, "alice@example.com", first, expr)

				slot, skipped, ok := s.Due(now)

				require.True(t, ok, name)
				assert.True(t, latest.Equal(slot), "%s: slot got %s want %s", name, slot.Format(time.RFC3339Nano), latest.Format(time.RFC3339Nano))
				assert.Equal(t, min(n, 1000), skipped, name)
				s.Fired(slot)
				next, ok := s.NextFireAt()
				require.True(t, ok, name)
				assert.True(t, after.Equal(next), "%s: next fire got %s want %s", name, next.Format(time.RFC3339Nano), after.Format(time.RFC3339Nano))
			}
		}
	}
}

// An `@every 1m` schedule whose stored next fire is 00:00:00Z, passed at
// 16:42:30Z, 1002 slots later, fires its 16:42:00Z slot and next fires at
// 16:43:00Z: a whole minute after the stored next fire, as every slot of it is.
func TestSchedule_AnEveryIntervalPastAThousandSlotsFiresOnItsInterval(t *testing.T) {
	expr := mustParse(t, "@every 1m", "UTC")
	first := utc(2026, time.October, 28, 0, 0)
	s := schedule.Rehydrate("default", "busy", "@every 1m", "UTC", "", false, &first, 1, "alice@example.com", first, expr)

	slot, skipped, ok := s.Due(time.Date(2026, time.October, 28, 16, 42, 30, 0, time.UTC))

	require.True(t, ok)
	assert.True(t, utc(2026, time.October, 28, 16, 42).Equal(slot), "slot: got %s want 16:42:00Z", slot)
	assert.Equal(t, 1000, skipped, "the count saturates at 1000")
	s.Fired(slot)
	next, ok := s.NextFireAt()
	require.True(t, ok)
	assert.True(t, utc(2026, time.October, 28, 16, 43).Equal(next), "next fire: got %s want 16:43:00Z", next)
}

func TestSchedule_AnHourlyCronFiresEveryElapsedHourAcrossBothDSTChanges(t *testing.T) {
	loc, err := time.LoadLocation("Europe/Paris")
	require.NoError(t, err)
	expr := mustParse(t, "0 * * * *", "Europe/Paris")

	for _, tc := range []struct {
		date string
		from time.Time
		want int
	}{
		{"2026-10-25", utc(2026, time.October, 24, 12, 0), 25},
		{"2027-03-28", utc(2027, time.March, 27, 12, 0), 23},
	} {
		s, err := schedule.New("default", "hourly", "0 * * * *", "Europe/Paris", "", false, expr, "alice@example.com", tc.from)
		require.NoError(t, err)

		fires := fireDates(t, s, loc, tc.from, tc.from.Add(72*time.Hour))

		assert.Equal(t, tc.want, fires[tc.date], "fires on %s", tc.date)
	}
}
