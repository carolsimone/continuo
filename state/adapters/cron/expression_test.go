package cron_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// firesOn returns every instant the expression fires during the local calendar
// day `date` (YYYY-MM-DD) of the zone, found by chaining Next from the instant
// before that day's first second.
func firesOn(t *testing.T, spec, zone, date string) []time.Time {
	t.Helper()
	loc, err := time.LoadLocation(zone)
	require.NoError(t, err)
	day, err := time.ParseInLocation("2006-01-02", date, loc)
	require.NoError(t, err)
	expr := mustParse(t, spec, zone)

	var fires []time.Time
	cursor := day.Add(-time.Second)
	for i := 0; i < 5000; i++ {
		cursor = expr.Next(cursor)
		if cursor.IsZero() {
			break
		}
		local := cursor.In(loc)
		if local.Format("2006-01-02") != date {
			if local.After(day) {
				break
			}
			continue
		}
		fires = append(fires, cursor)
	}
	return fires
}

func TestExpression_AWallClockCronFiresOncePerLocalDayAcrossDSTChanges(t *testing.T) {
	cases := []struct {
		name string
		spec string
		zone string
		date string
		want []time.Time
	}{
		{
			name: "Paris, 02:30 on the day the clocks go back (02:00-03:00 repeats)",
			spec: "30 2 * * *", zone: "Europe/Paris", date: "2026-10-25",
			want: []time.Time{utc(2026, time.October, 25, 0, 30)}, // 02:30 CEST, the first pass
		},
		{
			name: "Paris, 02:30 on the day the clocks go forward (02:30 does not exist)",
			spec: "30 2 * * *", zone: "Europe/Paris", date: "2027-03-28",
			want: []time.Time{utc(2027, time.March, 28, 1, 0)}, // 03:00 CEST, the first valid instant
		},
		{
			name: "Paris, 02:30 on an ordinary winter day",
			spec: "30 2 * * *", zone: "Europe/Paris", date: "2027-01-10",
			want: []time.Time{utc(2027, time.January, 10, 1, 30)},
		},
		{
			name: "Paris, 02:00 on the day the clocks go back",
			spec: "0 2 * * *", zone: "Europe/Paris", date: "2026-10-25",
			want: []time.Time{utc(2026, time.October, 25, 0, 0)},
		},
		{
			name: "Paris, 02:00 on the day the clocks go forward",
			spec: "0 2 * * *", zone: "Europe/Paris", date: "2027-03-28",
			want: []time.Time{utc(2027, time.March, 28, 1, 0)},
		},
		{
			name: "Paris, 03:00 on the day the clocks go forward is the first valid instant too",
			spec: "0 3 * * *", zone: "Europe/Paris", date: "2027-03-28",
			want: []time.Time{utc(2027, time.March, 28, 1, 0)},
		},
		{
			name: "Paris, 03:00 on the day the clocks go back is after the repeat",
			spec: "0 3 * * *", zone: "Europe/Paris", date: "2026-10-25",
			want: []time.Time{utc(2026, time.October, 25, 2, 0)},
		},
		{
			name: "Paris, two times in the skipped hour fire once, at the first valid instant",
			spec: "0,30 2 * * *", zone: "Europe/Paris", date: "2027-03-28",
			want: []time.Time{utc(2027, time.March, 28, 1, 0)},
		},
		{
			name: "Paris, a descriptor stays at midnight on both days",
			spec: "@daily", zone: "Europe/Paris", date: "2026-10-25",
			want: []time.Time{utc(2026, time.October, 24, 22, 0)},
		},
		{
			name: "New York, 02:30 on the day the clocks go forward",
			spec: "30 2 * * *", zone: "America/New_York", date: "2027-03-14",
			want: []time.Time{utc(2027, time.March, 14, 7, 0)}, // 03:00 EDT
		},
		{
			name: "New York, 01:30 on the day the clocks go back (01:00-02:00 repeats)",
			spec: "30 1 * * *", zone: "America/New_York", date: "2027-11-07",
			want: []time.Time{utc(2027, time.November, 7, 5, 30)}, // 01:30 EDT, the first pass
		},
		{
			name: "Lord Howe, a 30-minute gap: 02:15 does not exist on 2026-10-04",
			spec: "15 2 * * *", zone: "Australia/Lord_Howe", date: "2026-10-04",
			want: []time.Time{utc(2026, time.October, 3, 15, 30)}, // 02:30 +11:00
		},
		{
			name: "Lord Howe, a 30-minute repeat: 01:45 happens twice on 2027-04-04",
			spec: "45 1 * * *", zone: "Australia/Lord_Howe", date: "2027-04-04",
			want: []time.Time{utc(2027, time.April, 3, 14, 45)}, // 01:45 +11:00, the first pass
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := firesOn(t, tc.spec, tc.zone, tc.date)

			require.Len(t, got, len(tc.want), "fires on %s: %v", tc.date, got)
			for i := range tc.want {
				assert.True(t, tc.want[i].Equal(got[i]), "fire %d: got %s want %s", i, got[i].UTC(), tc.want[i])
			}
		})
	}
}

func TestExpression_ASlotInTheSecondPassOfARepeatedHourIsNotFiredAgain(t *testing.T) {
	expr := mustParse(t, "30 2 * * *", "Europe/Paris")
	// 02:15 CET, the second time 02:00-03:00 is read on 2026-10-25, after the
	// 02:30 CEST slot has fired.
	got := expr.Next(utc(2026, time.October, 25, 1, 15))

	assert.True(t, utc(2026, time.October, 26, 1, 30).Equal(got), "next fire: got %s want 02:30 CET on the 26th", got)

	// From the first pass, the same slot is still ahead.
	got = expr.Next(utc(2026, time.October, 25, 0, 15))
	assert.True(t, utc(2026, time.October, 25, 0, 30).Equal(got), "got %s", got)
}

func TestExpression_AnInstantAtTheTransitionStartsTheNextDay(t *testing.T) {
	expr := mustParse(t, "30 2 * * *", "Europe/Paris")
	transition := utc(2027, time.March, 28, 1, 0) // 03:00 CEST, the slot itself

	got := expr.Next(transition)

	assert.True(t, utc(2027, time.March, 29, 0, 30).Equal(got), "got %s want 02:30 CEST on the 29th", got)
	got = expr.Next(transition.Add(-time.Second))
	assert.True(t, transition.Equal(got), "one second before, the slot is ahead: got %s", got)
}

func TestExpression_AnHourlyCronFollowsElapsedTimeAcrossDSTChanges(t *testing.T) {
	cases := []struct {
		name  string
		spec  string
		date  string
		count int
	}{
		{"hourly, a 25-hour day", "0 * * * *", "2026-10-25", 25},
		{"hourly, a 23-hour day", "0 * * * *", "2027-03-28", 23},
		{"hourly, a 24-hour day", "0 * * * *", "2027-01-10", 24},
		{"every 30 minutes, a 25-hour day", "*/30 * * * *", "2026-10-25", 50},
		{"every 30 minutes, a 23-hour day", "*/30 * * * *", "2027-03-28", 46},
		{"@hourly, a 25-hour day", "@hourly", "2026-10-25", 25},
		{"every minute, a 23-hour day", "* * * * *", "2027-03-28", 23 * 60},
		{"every minute of an hour that is named is read on the wall clock: once through the repeat", "* 2 * * *", "2026-10-25", 60},
		{"every minute of an hour that is named is read on the wall clock: one fire for the skipped hour", "* 2 * * *", "2027-03-28", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := firesOn(t, tc.spec, "Europe/Paris", tc.date)

			assert.Len(t, got, tc.count)
		})
	}
}

func TestExpression_AnHourlyCronNeverGoesLongerThanOneIntervalWithoutFiring(t *testing.T) {
	// Across the whole of the two transition weekends, an hourly cron fires
	// 3600 s apart, without a double or a hole.
	expr := mustParse(t, "0 * * * *", "Europe/Paris")
	for _, start := range []time.Time{utc(2026, time.October, 24, 0, 0), utc(2027, time.March, 27, 0, 0)} {
		prev := expr.Next(start)
		for i := 0; i < 72; i++ {
			next := expr.Next(prev)
			assert.Equal(t, time.Hour, next.Sub(prev), "after %s", prev)
			prev = next
		}
	}
}

func TestExpression_AFixedIntervalIgnoresTheZone(t *testing.T) {
	expr := mustParse(t, "@every 90m", "Europe/Paris")

	got := expr.Next(utc(2026, time.October, 25, 0, 0))

	assert.True(t, utc(2026, time.October, 25, 1, 30).Equal(got), "got %s", got)
}

func TestExpression_DayOfWeekAndDayOfMonthReadTheLocalDate(t *testing.T) {
	// The weekday is the one on the clocks of Paris: 01:00 CET on a Monday is
	// 00:00Z the same Monday.
	expr := mustParse(t, "0 1 * * 1", "Europe/Paris")

	got := expr.Next(utc(2027, time.January, 5, 12, 0)) // a Tuesday

	assert.True(t, utc(2027, time.January, 11, 0, 0).Equal(got), "got %s", got) // Monday the 11th, 01:00 CET
}

func TestExpression_NoFireWithinTheHorizonIsTheZeroTime(t *testing.T) {
	// 29 February after 2096 is more than five years away from March 2097.
	expr := mustParse(t, "0 0 29 2 *", "UTC")

	got := expr.Next(utc(2097, time.March, 1, 0, 0))

	assert.True(t, got.IsZero(), "got %s", got)
}

func TestExpression_ADailyCronFiresOncePerLocalDayAllYear(t *testing.T) {
	// Every local date of two years carries exactly one fire of a daily cron,
	// whatever zone's clock changes fall on it: an hour, a half hour, at
	// midnight, in either hemisphere.
	zones := []string{"Europe/Paris", "America/New_York", "Australia/Lord_Howe", "Africa/Cairo", "Asia/Beirut", "America/Santiago"}
	crons := []string{"0 0 * * *", "30 23 * * *", "30 0 * * *", "15 1 * * *", "45 1 * * *", "0 2 * * *", "30 2 * * *", "0 3 * * *", "59 23 * * *"}
	for _, zone := range zones {
		loc, err := time.LoadLocation(zone)
		require.NoError(t, err)
		for _, spec := range crons {
			expr := mustParse(t, spec, zone)
			cursor := time.Date(2026, time.January, 1, 0, 0, 0, 0, loc).Add(-time.Second)
			end := time.Date(2028, time.January, 1, 0, 0, 0, 0, loc)
			perDay := map[string]int{}
			for cursor.Before(end) {
				cursor = expr.Next(cursor)
				perDay[cursor.In(loc).Format("2006-01-02")]++
			}
			for day := time.Date(2026, time.January, 1, 12, 0, 0, 0, loc); day.Before(end); day = day.AddDate(0, 0, 1) {
				key := day.Format("2006-01-02")
				if perDay[key] != 1 {
					t.Errorf("%s %q: %d fires on %s, want 1", zone, spec, perDay[key], key)
				}
			}
		}
	}
}
