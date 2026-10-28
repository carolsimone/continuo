package schedule

import (
	"io"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scheduleCommand returns the named subcommand of "continuo schedule".
func scheduleCommand(t *testing.T, name string) *cobra.Command {
	t.Helper()
	for _, sub := range NewCommand(&config.Config{}, io.Discard, io.Discard).Commands() {
		if sub.Name() == name {
			return sub
		}
	}
	require.FailNow(t, "no schedule command named "+name)
	return nil
}

// A schedule is a name the live release carries or that has a cron; no catalog
// of schedules exists, so no help text may send the reader to one.
func TestScheduleHelpNamesNoCatalog(t *testing.T) {
	for _, sub := range NewCommand(&config.Config{}, io.Discard, io.Discard).Commands() {
		for _, text := range []string{sub.Short, sub.Long, sub.Example} {
			assert.NotContains(t, strings.ToLower(text), "catalog", "%s help", sub.CommandPath())
		}
	}
}

// The commands that start a run of a schedule by name say what not_found means
// for a schedule: no model of the live release carries its tag and it has no
// cron.
func TestRunStartingScheduleCommandsSayWhenAScheduleIsNotFound(t *testing.T) {
	const want = `not_found  (exit 3)  no model of the live release has the schedule's tag and the schedule has no cron (see "schedule list")`
	for _, name := range []string{"trigger", "test", "build"} {
		assert.Contains(t, scheduleCommand(t, name).Long, want, "schedule %s", name)
	}
}

// maintenance mode refuses a pause (exit 7), so a pause is not for maintenance.
func TestPauseHelpDoesNotSuggestPausingForMaintenance(t *testing.T) {
	assert.NotContains(t, scheduleCommand(t, "pause").Long, "during maintenance")
}

// The chat passes the cron as one value; the help says the five fields are one
// argument and that a shell needs it quoted, in the Long, the Example and the
// usage error.
func TestSetCronHelpSaysTheCronIsOneArgument(t *testing.T) {
	setCron := scheduleCommand(t, "set-cron")
	long := strings.Join(strings.Fields(setCron.Long), " ")

	assert.Contains(t, long, "Pass the five fields as ONE argument (in a shell, quote it)")
	assert.Contains(t, setCron.Example, "ONE argument")
}

// set-cron re-sends what it did not set, and its help says which fields.
func TestSetCronHelpSaysWhatItResends(t *testing.T) {
	long := strings.Join(strings.Fields(scheduleCommand(t, "set-cron").Long), " ")

	assert.Contains(t, long, "re-sends the rest of the schedule as listed: its current paused flag and its description")
	assert.Contains(t, long, "sends the version it read")
}

// The commands that save a schedule by name say that the server refuses a name
// longer than 50 characters, the longest a run of the schedule can store, as a
// usage error.
func TestScheduleEditCommandsNameTheNameLimit(t *testing.T) {
	for _, name := range []string{"set-cron", "pause", "resume"} {
		assert.Contains(t, scheduleCommand(t, name).Long, "a name longer than 50 characters", "%s help", name)
	}
}

// A first cron is saved with no version, so an edit that saved between the read
// and the save, such as a pause, is overwritten by it; set-cron does not claim
// that a concurrent pause is never overwritten without saying so.
func TestSetCronHelpSaysWhatItsFirstCronOverwrites(t *testing.T) {
	long := strings.Join(strings.Fields(scheduleCommand(t, "set-cron").Long), " ")
	assert.NotContains(t, long, "so a concurrent pause is never overwritten;")
	assert.Contains(t, long, "A first cron is saved with no version: it overwrites an edit")
}
