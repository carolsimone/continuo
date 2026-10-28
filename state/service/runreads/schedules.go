package runreads

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/schedule"
)

// ScheduleView is one schedule of a tenant: its name, its cron, and its last
// run.
type ScheduleView struct {
	Name string
	// Cron, Description and Timezone are the schedule's row: its cron, what it
	// is for, and the zone its cron is read in. All three are empty when the
	// schedule has no row.
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
	rows     repository.ScheduleRepository
}

// NewSchedules returns a Schedules over the catalog, the last-run read and the
// schedule rows.
func NewSchedules(catalog repository.ActiveScheduleReader, lastRuns repository.LastRunReader, rows repository.ScheduleRepository) *Schedules {
	return &Schedules{catalog: catalog, lastRuns: lastRuns, rows: rows}
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
	rows, err := s.rows.List(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list the schedule rows: %w", err)
	}
	byName := make(map[string]*schedule.Schedule, len(rows))
	for _, row := range rows {
		byName[row.Name()] = row
	}

	out := make([]ScheduleView, 0, len(names))
	for _, name := range names {
		v := ScheduleView{Name: name}
		if row, ok := byName[name]; ok {
			v.Cron, v.Description, v.Timezone = row.Cron(), row.Description(), row.Timezone()
		}
		if last, ok := lastRuns[name]; ok {
			v.LastRun = &last
		}
		out = append(out, v)
	}
	return out, nil
}
