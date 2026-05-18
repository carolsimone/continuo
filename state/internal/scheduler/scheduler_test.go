package scheduler_test

import (
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/state/internal/scheduler"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadSchedulesConfig_Valid(t *testing.T) {
	yaml := `
timezone: Europe/Paris
schedules:
  - name: daily
    cron: "0 1 * * *"
    description: "Runs daily"
  - name: hourly
    cron: "0 * * * *"
    description: "Runs hourly"
`
	f, err := os.CreateTemp("", "schedules-*.yaml")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	_, err = f.WriteString(yaml)
	require.NoError(t, err)
	f.Close()

	cfg, err := scheduler.LoadSchedulesConfig(f.Name())
	require.NoError(t, err)
	assert.Equal(t, "Europe/Paris", cfg.Timezone)
	assert.Len(t, cfg.Schedules, 2)
	assert.Equal(t, "daily", cfg.Schedules[0].Name)
	assert.Equal(t, "0 1 * * *", cfg.Schedules[0].Cron)
	assert.Equal(t, "hourly", cfg.Schedules[1].Name)
}

func TestLoadSchedulesConfig_FileMissing(t *testing.T) {
	_, err := scheduler.LoadSchedulesConfig("/nonexistent/path.yaml")
	require.Error(t, err)
}

func TestLoadSchedulesConfig_Malformed(t *testing.T) {
	f, err := os.CreateTemp("", "schedules-*.yaml")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	_, err = f.WriteString("not: valid: yaml: [")
	require.NoError(t, err)
	f.Close()

	_, err = scheduler.LoadSchedulesConfig(f.Name())
	require.Error(t, err)
}

func TestNewCronSchedulerWithConfig_DynamicSchedules(t *testing.T) {
	cfg := &scheduler.SchedulesConfig{
		Timezone: "UTC",
		Schedules: []scheduler.ScheduleEntry{
			{Name: "test-schedule", Cron: "* * * * * *"}, // every second
		},
		WithSeconds: true,
	}

	activate := svchandlers.NewActivateScheduleHandler(testLogger())
	// The UoW factory panics if called — the constructor test does not fire cron
	// ticks so no activation occurs during this test.
	uowFactory := func() uow.UnitOfWork {
		panic("uow must not be called during constructor test")
	}
	s, err := scheduler.NewCronSchedulerWithConfig(activate, uowFactory, testLogger(), cfg)
	require.NoError(t, err)
	require.NotNil(t, s)
}

func TestNewCronSchedulerWithConfig_InvalidTimezone(t *testing.T) {
	cfg := &scheduler.SchedulesConfig{
		Timezone:  "Not/ATimezone",
		Schedules: []scheduler.ScheduleEntry{{Name: "x", Cron: "0 * * * *"}},
	}
	activate := svchandlers.NewActivateScheduleHandler(testLogger())
	_, err := scheduler.NewCronSchedulerWithConfig(activate, nil, testLogger(), cfg)
	require.Error(t, err)
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, nil))
}
