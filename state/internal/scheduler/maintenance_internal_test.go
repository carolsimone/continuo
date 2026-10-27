package scheduler

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/repository"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/runstart"
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
	s, err := NewCronSchedulerWithConfig(svchandlers.NewStartRunHandler(defaultPlanner, logger),
		func() uow.UnitOfWork { opened++; return nil }, logger, cfg, true)
	require.NoError(t, err)

	s.startScheduledRun("daily")

	require.Zero(t, opened, "a cron fire during maintenance must not open a unit of work")
	require.Contains(t, buf.String(), "maintenance")
	require.Contains(t, buf.String(), "daily")
}

// defaultPlanner plans runs with the task defaults state's main uses.
var defaultPlanner = runstart.Planner{Tasks: run.TaskDefaults{MaxRetries: 2, Timeout: 24 * time.Hour}}

// askedCatalog records the tenant of each schedule existence check and holds
// no schedule.
type askedCatalog struct {
	repository.ScheduleCatalogRepository
	tenants []string
}

func (c *askedCatalog) ExistsActive(_ context.Context, tenantID, _ string) (bool, error) {
	c.tenants = append(c.tenants, tenantID)
	return false, nil
}

// The schedules file is the default tenant's: a cron fire starts a run of the
// default tenant's schedule.
func TestStartScheduledRun_StartsARunOfTheDefaultTenant(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	catalog := &askedCatalog{}
	u := &uow.FakeUnitOfWork{}
	u.SetCatalogRepo(catalog)
	cfg := &SchedulesConfig{Timezone: "UTC", Schedules: []ScheduleEntry{{Name: "daily", Cron: "0 1 * * *"}}}
	s, err := NewCronSchedulerWithConfig(svchandlers.NewStartRunHandler(defaultPlanner, logger),
		func() uow.UnitOfWork { return u }, logger, cfg, false)
	require.NoError(t, err)

	s.startScheduledRun("daily")

	require.Equal(t, []string{pkgevents.DefaultTenantID}, catalog.tenants)
	require.Contains(t, buf.String(), "tenant_id="+pkgevents.DefaultTenantID)
}
