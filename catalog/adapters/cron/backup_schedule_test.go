package croninfra

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The parser accepts the five-field specs and the @ descriptors the chart
// renders, and refuses the ones the backup cannot honour: a timezone prefix
// (the schedule runs in UTC), an @every under a minute, a schedule no date
// satisfies, and a spec robfig/cron does not read.
func TestBackupScheduleParser_Parse(t *testing.T) {
	cases := []struct {
		spec   string
		refuse bool
	}{
		{"0 2 * * *", false},
		{"30 3 * * 1", false},
		{"*/15 * * * *", false},
		{"@daily", false},
		{"@hourly", false},
		{"@every 6h", false},
		{"@every 1m", false},
		{"  0 2 * * *  ", false},
		{"0 0 29 2 *", false},
		{"", true},
		{"   ", true},
		{"nightly", true},
		{"0 2 * *", true},
		{"0 2 * * * *", true},
		{"61 2 * * *", true},
		{"@nightly", true},
		{"CRON_TZ=UTC 0 2 * * *", true},
		{"TZ=UTC 0 2 * * *", true},
		{"TZ=Europe/Paris @daily", true},
		{" CRON_TZ=UTC 0 2 * * *", true},
		{"@every 30s", true},
		{"@every 0s", true},
		{"0 0 30 2 *", true},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%q", c.spec), func(t *testing.T) {
			schedule, err := BackupScheduleParser{}.Parse(c.spec)
			if c.refuse {
				require.Error(t, err)
				assert.Nil(t, schedule)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, schedule)
		})
	}
}

// A parsed schedule fires in UTC: the slot after a time is the schedule's
// next UTC slot, whatever zone the time carries.
func TestBackupScheduleParser_NextIsInUTC(t *testing.T) {
	schedule, err := BackupScheduleParser{}.Parse("0 2 * * *")
	require.NoError(t, err)

	paris := time.FixedZone("CET", 3600)
	after := time.Date(2026, time.November, 1, 3, 30, 0, 0, paris) // 02:30 UTC
	got := schedule.Next(after)

	assert.Equal(t, time.Date(2026, time.November, 2, 2, 0, 0, 0, time.UTC), got)
	assert.Equal(t, time.UTC, got.Location())
}
