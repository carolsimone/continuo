package schedulesconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/state/adapters/schedulesconfig"
	"github.com/carolsimone/continuo/state/internal/scheduler"
)

func TestCrons_GivesTheEntriesOfTheLoadedFile(t *testing.T) {
	crons := schedulesconfig.New(&scheduler.SchedulesConfig{
		Timezone: "Europe/Rome",
		Schedules: []scheduler.ScheduleEntry{
			{Name: "daily", Cron: "0 6 * * *", Description: "the morning load"},
		},
	})

	assert.Equal(t, "Europe/Rome", crons.Timezone())
	got, ok := crons.CronOf("daily")
	assert.True(t, ok)
	assert.Equal(t, "0 6 * * *", got.Expression)
	assert.Equal(t, "the morning load", got.Description)
	_, ok = crons.CronOf("adhoc")
	assert.False(t, ok, "a schedule the file does not name has no cron")
}

func TestCrons_ANilConfigDeclaresNoSchedule(t *testing.T) {
	crons := schedulesconfig.New(nil)

	assert.Empty(t, crons.Timezone())
	_, ok := crons.CronOf("daily")
	assert.False(t, ok)
}
