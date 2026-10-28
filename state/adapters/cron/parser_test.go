package cron_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/state/adapters/cron"
	"github.com/carolsimone/continuo/state/domain/schedule"
	"github.com/carolsimone/continuo/state/service/ports"
)

var _ ports.CronParser = (*cron.Parser)(nil)

func newParser(t *testing.T) *cron.Parser {
	t.Helper()
	p, err := cron.NewParser()
	require.NoError(t, err)
	return p
}

func mustParse(t *testing.T, spec, zone string) schedule.Expression {
	t.Helper()
	expr, err := newParser(t).Parse(spec, zone)
	require.NoError(t, err, "parse %q in %s", spec, zone)
	return expr
}

func utc(year int, month time.Month, day, hour, minute int) time.Time {
	return time.Date(year, month, day, hour, minute, 0, 0, time.UTC)
}

func TestParser_AcceptsDescriptors(t *testing.T) {
	zone := "Europe/Paris"
	after := utc(2026, time.October, 27, 12, 0)
	cases := []struct {
		spec string
		want time.Time
	}{
		{"@daily", utc(2026, time.October, 27, 23, 0)},      // 00:00 CET on the 28th
		{"@midnight", utc(2026, time.October, 27, 23, 0)},   // the same instant
		{"@hourly", utc(2026, time.October, 27, 13, 0)},     // 14:00 CET, the next hour boundary
		{"@weekly", utc(2026, time.October, 31, 23, 0)},     // Sunday 1 November, 00:00 CET
		{"@monthly", utc(2026, time.October, 31, 23, 0)},    // 1 November, 00:00 CET
		{"@yearly", utc(2026, time.December, 31, 23, 0)},    // 1 January, 00:00 CET
		{"@every 90m", utc(2026, time.October, 27, 13, 30)}, // 90 minutes after the given instant
	}
	for _, tc := range cases {
		t.Run(tc.spec, func(t *testing.T) {
			expr := mustParse(t, tc.spec, zone)
			got := expr.Next(after)
			assert.True(t, tc.want.Equal(got), "Next(%s): got %s want %s", after, got, tc.want)
		})
	}
}

func TestParser_NextIsReadInTheScheduleZone(t *testing.T) {
	// 01:00 in Paris after the clocks went back is 00:00Z.
	expr := mustParse(t, "0 1 * * *", "Europe/Paris")

	got := expr.Next(utc(2026, time.October, 27, 12, 0))

	assert.True(t, utc(2026, time.October, 28, 0, 0).Equal(got), "got %s", got)
	assert.Equal(t, time.UTC, got.Location(), "an instant is answered in UTC")

	// New York is on EDT (UTC-4) until 1 November.
	ny := mustParse(t, "0 1 * * *", "America/New_York")
	got = ny.Next(utc(2026, time.October, 27, 12, 0))
	assert.True(t, utc(2026, time.October, 28, 5, 0).Equal(got), "got %s", got)
}

func TestParser_NextIsStrictlyAfter(t *testing.T) {
	expr := mustParse(t, "0 * * * *", "UTC")

	got := expr.Next(utc(2026, time.October, 28, 3, 0))

	assert.True(t, utc(2026, time.October, 28, 4, 0).Equal(got), "got %s", got)
}

func TestParser_RefusesACronTZPrefix(t *testing.T) {
	p := newParser(t)
	for _, spec := range []string{
		"CRON_TZ=Europe/Paris 0 1 * * *",
		"TZ=Europe/Paris 0 1 * * *",
		"  CRON_TZ=UTC 0 1 * * *",
		"CRON_TZ=UTC @daily",
	} {
		expr, err := p.Parse(spec, "Europe/Paris")
		require.Error(t, err, "parse %q", spec)
		assert.Nil(t, expr)
		assert.Contains(t, err.Error(), "timezone", "the message says where the zone goes: %v", err)

		_, err = p.Describe(spec)
		require.Error(t, err, "describe %q", spec)
	}
}

func TestParser_RefusesAnUnknownZone(t *testing.T) {
	p := newParser(t)
	for _, zone := range []string{"Mars/Olympus_Mons", "Europe/Atlantis", "", "Local", "../etc/passwd", "europe/paris "} {
		expr, err := p.Parse("0 1 * * *", zone)
		require.Error(t, err, "zone %q", zone)
		assert.Nil(t, expr)
		assert.Contains(t, err.Error(), "timezone", "zone %q: %v", zone, err)
	}
}

func TestParser_AcceptsAnyIANAZone(t *testing.T) {
	for _, zone := range []string{"UTC", "Europe/Paris", "America/New_York", "Asia/Kolkata", "Australia/Lord_Howe"} {
		mustParse(t, "0 1 * * *", zone)
	}
}

func TestParser_RefusesACronItCannotRead(t *testing.T) {
	p := newParser(t)
	for _, spec := range []string{
		"",
		"   ",
		"61 * * * *",
		"* * *",
		"0 0 1 1 * 2030", // a year field is not standard
		"0 0 0 1 1 *",    // a seconds field is not standard
		"@sometimes",     // an unknown descriptor
		"@every soon",    // a bad duration
		"0 25 * * *",     // an hour out of range
		"0 0 1 13 *",     // a month out of range
		"not a cron at all",
	} {
		expr, err := p.Parse(spec, "UTC")
		require.Error(t, err, "parse %q", spec)
		assert.Nil(t, expr)
	}
}

func TestParser_RefusesACronThatNeverFires(t *testing.T) {
	p := newParser(t)
	for _, spec := range []string{"0 0 30 2 *", "0 0 31 4 *"} {
		expr, err := p.Parse(spec, "Europe/Paris")
		require.Error(t, err, "parse %q", spec)
		assert.Nil(t, expr)
		assert.Contains(t, err.Error(), "never fires")
	}
	// A leap day does fire.
	mustParse(t, "0 0 29 2 *", "Europe/Paris")
}

func TestParser_DescribesInWords(t *testing.T) {
	p := newParser(t)

	got, err := p.Describe("0 1 * * *")

	require.NoError(t, err)
	assert.Contains(t, got, "01:00")
	assert.Equal(t, "At 01:00", got)
}

func TestParser_DescribesDescriptorsAndIntervals(t *testing.T) {
	p := newParser(t)
	cases := map[string]string{
		"@daily":       "At 00:00",
		"@midnight":    "At 00:00",
		"@hourly":      "Every hour",
		"@weekly":      "At 00:00, only on Sunday",
		"@monthly":     "At 00:00, on day 1 of the month",
		"@yearly":      "At 00:00, on day 1 of the month, only in January",
		"@annually":    "At 00:00, on day 1 of the month, only in January",
		"@every 90m":   "Every 1h30m0s",
		"@every 1s":    "Every 1s",
		"*/15 * * * *": "Every 15 minutes",
		"0 9 * * 1-5":  "At 09:00, Monday through Friday",
	}
	for spec, want := range cases {
		t.Run(spec, func(t *testing.T) {
			got, err := p.Describe(spec)
			require.NoError(t, err)
			assert.Equal(t, want, got)
		})
	}
}

func TestParser_DescribesAQuestionMarkAsAnyValue(t *testing.T) {
	// robfig reads `?` as `*` in every field; the words library panics on some
	// of those positions.
	p := newParser(t)
	cases := map[string]string{
		"0 1 ? * ?":    "At 01:00",
		"? ? * * *":    "Every minute",
		"0 ? * * *":    "Every hour",
		"0 0 * ? *":    "At 00:00",
		"0 0 1 ? ?":    "At 00:00, on day 1 of the month",
		"? */1 13 ? ?": "Every minute, on day 13 of the month",
	}
	for spec, want := range cases {
		got, err := p.Describe(spec)
		require.NoError(t, err, "describe %q", spec)
		assert.Equal(t, want, got, "describe %q", spec)
	}
}

func TestParser_DescribeRefusesWhatTheWordsLibraryCannotRead(t *testing.T) {
	// robfig accepts a step wider than the field, the words library does not;
	// the cron still fires, so Parse accepts it and only the words are missing.
	p := newParser(t)
	const spec = "0 9 * * */9"
	mustParse(t, spec, "UTC")

	got, err := p.Describe(spec)

	require.Error(t, err)
	assert.Empty(t, got)
	assert.Contains(t, err.Error(), "cannot be put in words")
}

func TestParser_DescribeRefusesWhatParseRefuses(t *testing.T) {
	p := newParser(t)
	for _, spec := range []string{"", "61 * * * *", "0 0 1 1 * 2030", "0 0 0 1 1 *", "@sometimes", "@every soon"} {
		got, err := p.Describe(spec)
		require.Error(t, err, "describe %q", spec)
		assert.Empty(t, got)
	}
}

func TestParser_DescribeNeverPanicsOnAnythingParseAccepts(t *testing.T) {
	p := newParser(t)
	for _, spec := range []string{
		"* * * * *", "*/5 * * * *", "0-59/7 3 * * *", "0 0 L * *", "0 0 1,15 * *", "0 0 * jan-mar mon",
		"5,10,15 8-17 * * 1,3,5", "0 0 29 2 *", "59 23 31 12 *", "0 0 * * sun", "0 */2 * * *", "30 2 * * *",
		"1-5 * * * *", "* 2 * * *", "0 0 1-31/2 * *", "0 0 * 1-12/3 *", "0 0 ? * ?",
	} {
		if _, err := p.Parse(spec, "UTC"); err != nil {
			continue
		}
		assert.NotPanics(t, func() {
			got, err := p.Describe(spec)
			if err == nil {
				assert.NotEmpty(t, got, "describe %q", spec)
			}
		}, "describe %q", spec)
	}
}
