package runreads

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/service/ports"
)

// ScheduleView is one schedule of a tenant: its name, the cron the install
// declares for it, and its last run.
type ScheduleView struct {
	Name string
	// Cron and Description are empty when the install declares no cron for the
	// schedule; Timezone is the one the install's crons are read in.
	Cron        string
	Description string
	Timezone    string
	// LastRun is the schedule's most recent run, nil when it never ran. A
	// schedule has an active run when LastRun is active.
	LastRun *repository.LastRunSummary
}

// HasActiveRun reports whether a pending, running or cancelling run holds the
// schedule.
func (v ScheduleView) HasActiveRun() bool { return v.LastRun != nil && v.LastRun.IsActive }

// Schedules reads the schedules of the tenant each read is given.
type Schedules struct {
	catalog  repository.ActiveScheduleReader
	lastRuns repository.LastRunReader
	crons    ports.ScheduleCrons
}

// NewSchedules returns a Schedules over the catalog, the last-run read and the
// install's cron configuration; crons may be nil when the install declares
// none.
func NewSchedules(catalog repository.ActiveScheduleReader, lastRuns repository.LastRunReader, crons ports.ScheduleCrons) *Schedules {
	return &Schedules{catalog: catalog, lastRuns: lastRuns, crons: crons}
}

// List returns every active schedule of tenantID's catalog in catalog order
// (by name), each with its cron and last run.
func (s *Schedules) List(ctx context.Context, tenantID string) ([]ScheduleView, error) {
	names, err := s.catalog.ListActive(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list active schedules: %w", err)
	}
	lastRuns, err := s.lastRuns.GetLastRunPerSchedule(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("get last runs: %w", err)
	}

	out := make([]ScheduleView, 0, len(names))
	for _, name := range names {
		v := ScheduleView{Name: name}
		if s.crons != nil {
			v.Timezone = s.crons.Timezone()
			if c, ok := s.crons.CronOf(name); ok {
				v.Cron, v.Description = c.Expression, c.Description
			}
		}
		if last, ok := lastRuns[name]; ok {
			v.LastRun = &last
		}
		out = append(out, v)
	}
	return out, nil
}
