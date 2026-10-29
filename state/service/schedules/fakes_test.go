package schedules_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/state/adapters/cron"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/schedules"
	"github.com/carolsimone/continuo/state/service/uow"
)

const (
	tenantA = "default"
	tenantB = "acme"
)

// now is the instant the services under test run at: 2026-10-28 12:00 UTC,
// after Europe/Paris went back to UTC+1.
var now = time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)

var errBoom = errors.New("boom")

type fixedClock struct{ at time.Time }

func (c fixedClock) Now() time.Time { return c.at }

// storedRow is one row of memorySchedules, as the table stores it.
type storedRow struct {
	cron, timezone, description string
	paused                      bool
	nextFireAt                  *time.Time
	version                     int64
	updatedBy                   string
	updatedAt                   time.Time
}

// memorySchedules is the shared uow.FakeScheduleRepository, which follows the
// Postgres adapter's contract and rebuilds each schedule with the real cron
// parser, with the two helpers these tests use to plant and read rows.
type memorySchedules struct{ *uow.FakeScheduleRepository }

var _ repository.ScheduleRepository = memorySchedules{}

func newMemorySchedules(parser ports.CronParser) memorySchedules {
	return memorySchedules{uow.NewFakeScheduleRepository(parser, nil)}
}

// put stores a row as it stands, bypassing the aggregate, as a row written by
// hand would be.
func (m memorySchedules) put(tenant, name string, r storedRow) {
	m.Put(uow.StoredSchedule{
		TenantID: tenant, Name: name, Cron: r.cron, Timezone: r.timezone, Description: r.description,
		Paused: r.paused, NextFireAt: r.nextFireAt, Version: r.version, UpdatedBy: r.updatedBy, UpdatedAt: r.updatedAt,
	})
}

func (m memorySchedules) row(tenant, name string) (storedRow, bool) {
	r, ok := m.Row(tenant, name)
	if !ok {
		return storedRow{}, false
	}
	return storedRow{
		cron: r.Cron, timezone: r.Timezone, description: r.Description, paused: r.Paused, nextFireAt: r.NextFireAt,
		version: r.Version, updatedBy: r.UpdatedBy, updatedAt: r.UpdatedAt,
	}, true
}

// lastRuns is an in-memory repository.LastRunReader: the last run of each
// schedule name, per tenant.
type lastRuns struct {
	byTenant map[string]map[string]repository.LastRunSummary
	failWith error
}

var _ repository.LastRunReader = (*lastRuns)(nil)

func (l *lastRuns) GetLastRunPerSchedule(_ context.Context, tenantID string) (map[string]repository.LastRunSummary, error) {
	if l.failWith != nil {
		return nil, l.failWith
	}
	return l.byTenant[tenantID], nil
}

func (l *lastRuns) put(tenant string, last repository.LastRunSummary) {
	if l.byTenant[tenant] == nil {
		l.byTenant[tenant] = map[string]repository.LastRunSummary{}
	}
	l.byTenant[tenant][last.ScheduleName] = last
}

// harness is a schedules.Service over in-memory ports, running at now.
type harness struct {
	parser   ports.CronParser
	rows     memorySchedules
	models   *uow.FakeLiveScheduleModels
	lastRuns *lastRuns
	svc      *schedules.Service
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	parser, err := cron.NewParser()
	require.NoError(t, err)
	h := &harness{
		parser:   parser,
		rows:     newMemorySchedules(parser),
		models:   &uow.FakeLiveScheduleModels{Counts: map[string]map[string]int{}},
		lastRuns: &lastRuns{byTenant: map[string]map[string]repository.LastRunSummary{}},
	}
	h.svc = schedules.New(h.rows, h.models, h.lastRuns, parser, fixedClock{at: now})
	return h
}

// liveModels sets how many models of tenant's live release carry each name.
func (h *harness) liveModels(tenant string, counts map[string]int) {
	h.models.Counts[tenant] = counts
}

// fires returns the first n fires of cron in timezone strictly after `after`,
// as the parser counts them.
func (h *harness) fires(t *testing.T, cronText, timezone string, after time.Time, n int) []time.Time {
	t.Helper()
	expr, err := h.parser.Parse(cronText, timezone)
	require.NoError(t, err)
	out := make([]time.Time, 0, n)
	for at := after; len(out) < n; {
		at = expr.Next(at)
		out = append(out, at)
	}
	return out
}

// parseError returns the parser's refusal of cron in timezone.
func (h *harness) parseError(t *testing.T, cronText, timezone string) string {
	t.Helper()
	_, err := h.parser.Parse(cronText, timezone)
	require.Error(t, err)
	return err.Error()
}

func activeRun(name string) repository.LastRunSummary {
	return repository.LastRunSummary{ScheduleName: name, RunID: uuid.New(), Status: run.RunStatusRunning, CreatedAt: now, IsActive: true}
}
