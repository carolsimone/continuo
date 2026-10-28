package schedules_test

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/state/adapters/cron"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/schedule"
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

type rowKey struct{ tenant, name string }

// memorySchedules is an in-memory repository.ScheduleRepository that stores
// rows as the table does and rebuilds each schedule with the real cron parser,
// so a planted row whose cron no longer parses behaves as the adapter's.
type memorySchedules struct {
	parser ports.CronParser
	rows   map[rowKey]storedRow

	// gets and saves count the calls of Get and Save, failed ones included.
	gets, saves int
	// failWith, when set, fails every call.
	failWith error
	// failInvalidWith, when set, fails ListStoredCronInvalid alone.
	failInvalidWith error
	// afterGet, when set, runs after each Get has read its row: a test uses it
	// to change the row as a concurrent edit would.
	afterGet func()
}

var _ repository.ScheduleRepository = (*memorySchedules)(nil)

func newMemorySchedules(parser ports.CronParser) *memorySchedules {
	return &memorySchedules{parser: parser, rows: map[rowKey]storedRow{}}
}

// put stores a row as it stands, bypassing the aggregate, as a row written by
// hand would be.
func (m *memorySchedules) put(tenant, name string, row storedRow) {
	m.rows[rowKey{tenant, name}] = row
}

func (m *memorySchedules) row(tenant, name string) (storedRow, bool) {
	r, ok := m.rows[rowKey{tenant, name}]
	return r, ok
}

func (m *memorySchedules) hydrate(k rowKey, r storedRow) (*schedule.Schedule, error) {
	expr, err := m.parser.Parse(r.cron, r.timezone)
	if err != nil {
		return nil, &schedule.StoredCronInvalidError{
			TenantID: k.tenant, Name: k.name, Cron: r.cron, Timezone: r.timezone, Version: r.version, Reason: err,
		}
	}
	return schedule.Rehydrate(k.tenant, k.name, r.cron, r.timezone, r.description, r.paused, r.nextFireAt,
		r.version, r.updatedBy, r.updatedAt, expr), nil
}

func (m *memorySchedules) Exists(_ context.Context, tenantID, name string) (bool, error) {
	if m.failWith != nil {
		return false, m.failWith
	}
	_, ok := m.rows[rowKey{tenantID, name}]
	return ok, nil
}

func (m *memorySchedules) Get(_ context.Context, tenantID, name string) (*schedule.Schedule, bool, error) {
	m.gets++
	if m.failWith != nil {
		return nil, false, m.failWith
	}
	k := rowKey{tenantID, name}
	r, ok := m.rows[k]
	if m.afterGet != nil {
		defer m.afterGet()
	}
	if !ok {
		return nil, false, nil
	}
	s, err := m.hydrate(k, r)
	if err != nil {
		return nil, false, err
	}
	return s, true, nil
}

// GetForUpdate reads the row as Get does, without running afterGet: the fake
// holds no transaction, so there is no row lock to take.
func (m *memorySchedules) GetForUpdate(_ context.Context, tenantID, name string) (*schedule.Schedule, bool, error) {
	if m.failWith != nil {
		return nil, false, m.failWith
	}
	k := rowKey{tenantID, name}
	r, ok := m.rows[k]
	if !ok {
		return nil, false, nil
	}
	s, err := m.hydrate(k, r)
	if err != nil {
		return nil, false, err
	}
	return s, true, nil
}

func (m *memorySchedules) List(_ context.Context, tenantID string) ([]*schedule.Schedule, error) {
	if m.failWith != nil {
		return nil, m.failWith
	}
	var out []*schedule.Schedule
	for k, r := range m.rows {
		if k.tenant != tenantID {
			continue
		}
		s, err := m.hydrate(k, r)
		if errors.Is(err, schedule.ErrStoredCronInvalid) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out, nil
}

func (m *memorySchedules) ListStoredCronInvalid(_ context.Context, tenantID string) ([]string, error) {
	if m.failWith != nil {
		return nil, m.failWith
	}
	if m.failInvalidWith != nil {
		return nil, m.failInvalidWith
	}
	names := []string{}
	for k, r := range m.rows {
		if k.tenant != tenantID {
			continue
		}
		if _, err := m.parser.Parse(r.cron, r.timezone); err != nil {
			names = append(names, k.name)
		}
	}
	sort.Strings(names)
	return names, nil
}

// Save writes s while the stored row is at expectedVersion, as the adapter
// does: 0 names no row, so it only inserts, and a row it finds is stale.
func (m *memorySchedules) Save(_ context.Context, s *schedule.Schedule, expectedVersion int64) error {
	m.saves++
	if m.failWith != nil {
		return m.failWith
	}
	k := rowKey{s.TenantID(), s.Name()}
	r, ok := m.rows[k]
	if ok != (expectedVersion > 0) || (ok && r.version != expectedVersion) {
		return schedule.ErrStaleVersion
	}
	var next *time.Time
	if at, ok := s.NextFireAt(); ok {
		next = &at
	}
	m.rows[k] = storedRow{
		cron: s.Cron(), timezone: s.Timezone(), description: s.Description(), paused: s.Paused(), nextFireAt: next,
		version: s.Version(), updatedBy: s.UpdatedBy(), updatedAt: s.UpdatedAt(),
	}
	return nil
}

func (m *memorySchedules) Delete(_ context.Context, tenantID, name string) (bool, error) {
	if m.failWith != nil {
		return false, m.failWith
	}
	k := rowKey{tenantID, name}
	_, ok := m.rows[k]
	delete(m.rows, k)
	return ok, nil
}

func (m *memorySchedules) LockDue(context.Context, time.Time, int) ([]*schedule.Schedule, error) {
	return nil, uow.ErrNotFaked
}

func (m *memorySchedules) EarliestFire(context.Context) (time.Time, bool, error) {
	return time.Time{}, false, uow.ErrNotFaked
}

// Count counts the rows of every tenant that are not paused and those that
// are.
func (m *memorySchedules) Count(context.Context) (scheduled, paused int, err error) {
	if m.failWith != nil {
		return 0, 0, m.failWith
	}
	for _, r := range m.rows {
		if r.paused {
			paused++
		} else {
			scheduled++
		}
	}
	return scheduled, paused, nil
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
	rows     *memorySchedules
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
