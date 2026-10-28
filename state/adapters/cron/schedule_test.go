package cron_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/state/domain/schedule"
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
			s := schedule.New("default", "nightly", "30 2 * * *", "Europe/Paris", "", false, expr, "alice@example.com", tc.from)

			fires := fireDates(t, s, loc, tc.from, tc.to)

			for _, day := range tc.days {
				assert.Equal(t, 1, fires[day], "fires on %s (all: %v)", day, fires)
			}
			assert.Equal(t, 1, fires[tc.days[1]], "the transition day %s fires exactly once", tc.days[1])
		})
	}
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
		s := schedule.New("default", "hourly", "0 * * * *", "Europe/Paris", "", false, expr, "alice@example.com", tc.from)

		fires := fireDates(t, s, loc, tc.from, tc.from.Add(72*time.Hour))

		assert.Equal(t, tc.want, fires[tc.date], "fires on %s", tc.date)
	}
}
