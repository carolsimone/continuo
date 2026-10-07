package scheduler

import (
	"bytes"
	"log/slog"
	"testing"

	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/stretchr/testify/require"
)

// A cron fire during maintenance creates no run: activation never opens a unit
// of work, and the skip is logged with the schedule's name.
func TestActivateSchedule_SkippedDuringMaintenance(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	opened := 0
	cfg := &SchedulesConfig{Timezone: "UTC", Schedules: []ScheduleEntry{{Name: "daily", Cron: "0 1 * * *"}}}
	s, err := NewCronSchedulerWithConfig(svchandlers.NewActivateScheduleHandler(logger),
		func() uow.UnitOfWork { opened++; return nil }, logger, cfg, true)
	require.NoError(t, err)

	s.activateSchedule("daily")

	require.Zero(t, opened, "a cron fire during maintenance must not open a unit of work")
	require.Contains(t, buf.String(), "maintenance")
	require.Contains(t, buf.String(), "daily")
}
