package uow

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/schedule"
	ports "github.com/carolsimone/continuo/state/service/ports"
)

// ScheduleKey names a row of the schedules table: its tenant and its name.
type ScheduleKey struct{ TenantID, Name string }

// StoredSchedule is a row of the schedules table as FakeScheduleRepository
// holds it: the text of the cron and timezone, not an expression parsed from
// them, so a row the parser refuses can be stored.
type StoredSchedule struct {
	TenantID    string
	Name        string
	Cron        string
	Timezone    string
	Description string
	Paused      bool
	NextFireAt  *time.Time
	Version     int64
	UpdatedBy   string
	UpdatedAt   time.Time
	// Expression, when set, is what the row's schedule is rebuilt with in
	// place of parsing Cron and Timezone, which then always read: a test that
	// needs an expression of its own sets it.
	Expression schedule.Expression
}

// FakeScheduleRepository is an in-memory repository.ScheduleRepository that
// follows the Postgres adapter's contract: a row stores the text of its cron,
// each load rebuilds the schedule from that text with the cron parser, and
// what a caller changes on a schedule stays its own until it saves it. A row
// the parser refuses fails Get and LoadForUpdate with a
// *schedule.StoredCronInvalidError, is named apart by ListWithUnreadable, is
// left out of ListDue and is counted unparsable; Save at a row's version
// repairs it. LoadForUpdate needs an open transaction, as reported by the
// function the repository is built with. It holds no lock beyond its own mutex.
type FakeScheduleRepository struct {
	parser ports.CronParser
	inTx   func() bool

	mu      sync.Mutex
	rows    map[ScheduleKey]*StoredSchedule
	gets    int
	saves   int
	listDue int
	changed chan<- struct{}

	// FailWith, when set, fails every call with it.
	FailWith error
	// AfterGet, when set, runs after each Get has read its row and released
	// the repository: a test uses it to change the row as a concurrent edit
	// would.
	AfterGet func()
}

var _ repository.ScheduleRepository = (*FakeScheduleRepository)(nil)

// NewFakeScheduleRepository returns an empty repository that rebuilds each
// schedule with parser (a row with an Expression of its own needs none) and
// tells inTx whether the unit of work that owns it has a transaction open; a
// nil inTx means none is.
func NewFakeScheduleRepository(parser ports.CronParser, inTx func() bool) *FakeScheduleRepository {
	if inTx == nil {
		inTx = func() bool { return false }
	}
	return &FakeScheduleRepository{parser: parser, inTx: inTx, rows: map[ScheduleKey]*StoredSchedule{}}
}

// Put stores row as it is, replacing the row of its tenant and name.
func (f *FakeScheduleRepository) Put(row StoredSchedule) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows[ScheduleKey{row.TenantID, row.Name}] = copyRow(&row)
}

// Row returns a copy of the stored row of tenantID's schedule name.
func (f *FakeScheduleRepository) Row(tenantID, name string) (StoredSchedule, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.rows[ScheduleKey{tenantID, name}]
	if !ok {
		return StoredSchedule{}, false
	}
	return *copyRow(row), true
}

// Update changes the stored row of tenantID's schedule name with edit, as a
// write that bypasses the repository would, and reports whether there is one.
func (f *FakeScheduleRepository) Update(tenantID, name string, edit func(*StoredSchedule)) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.rows[ScheduleKey{tenantID, name}]
	if ok {
		edit(row)
	}
	return ok
}

// NotifyOnChange makes every save and delete send on ch without blocking, as
// the schedules table's trigger notifies its channel when a change commits.
func (f *FakeScheduleRepository) NotifyOnChange(ch chan<- struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.changed = ch
}

// GetCalls is how many times Get was called, failed calls included.
func (f *FakeScheduleRepository) GetCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gets
}

// SaveCalls is how many times Save was called, refused calls included.
func (f *FakeScheduleRepository) SaveCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.saves
}

// ListDueCalls is how many times ListDue was called.
func (f *FakeScheduleRepository) ListDueCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listDue
}

func copyRow(row *StoredSchedule) *StoredSchedule {
	c := *row
	if row.NextFireAt != nil {
		next := *row.NextFireAt
		c.NextFireAt = &next
	}
	return &c
}

// parse rebuilds the expression of a row: its own, else the parser's reading of
// its cron and timezone.
func (f *FakeScheduleRepository) parse(row *StoredSchedule) (schedule.Expression, error) {
	if row.Expression != nil {
		return row.Expression, nil
	}
	if f.parser == nil {
		return nil, fmt.Errorf("read schedule %q of tenant %q: the repository has no cron parser", row.Name, row.TenantID)
	}
	expr, err := f.parser.Parse(row.Cron, row.Timezone)
	if err != nil {
		return nil, &schedule.StoredCronInvalidError{
			TenantID: row.TenantID, Name: row.Name, Cron: row.Cron, Timezone: row.Timezone,
			Version: row.Version, Reason: err,
		}
	}
	return expr, nil
}

// readable reports whether the row's cron and timezone parse.
func (f *FakeScheduleRepository) readable(row *StoredSchedule) bool {
	_, err := f.parse(row)
	return err == nil
}

// hydrate rebuilds the schedule a row stores, or the error parse returns.
func (f *FakeScheduleRepository) hydrate(row *StoredSchedule) (*schedule.Schedule, error) {
	expr, err := f.parse(row)
	if err != nil {
		return nil, err
	}
	return schedule.Rehydrate(row.TenantID, row.Name, row.Cron, row.Timezone, row.Description,
		row.Paused, row.NextFireAt, row.Version, row.UpdatedBy, row.UpdatedAt, expr), nil
}

func (f *FakeScheduleRepository) notify() {
	if f.changed == nil {
		return
	}
	select {
	case f.changed <- struct{}{}:
	default:
	}
}

// Get returns the schedule, or a *schedule.StoredCronInvalidError for a row
// whose cron or timezone no longer parses.
func (f *FakeScheduleRepository) Get(_ context.Context, tenantID, name string) (*schedule.Schedule, bool, error) {
	f.mu.Lock()
	f.gets++
	after := f.AfterGet
	s, ok, err := f.get(tenantID, name)
	f.mu.Unlock()
	if after != nil {
		after()
	}
	return s, ok, err
}

func (f *FakeScheduleRepository) get(tenantID, name string) (*schedule.Schedule, bool, error) {
	if f.FailWith != nil {
		return nil, false, f.FailWith
	}
	row, ok := f.rows[ScheduleKey{tenantID, name}]
	if !ok {
		return nil, false, nil
	}
	s, err := f.hydrate(row)
	if err != nil {
		return nil, false, err
	}
	return s, true, nil
}

// Exists reports whether there is a row, whatever its cron.
func (f *FakeScheduleRepository) Exists(_ context.Context, tenantID, name string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.FailWith != nil {
		return false, f.FailWith
	}
	_, ok := f.rows[ScheduleKey{tenantID, name}]
	return ok, nil
}

// LoadForUpdate is Get inside a transaction; outside one it is an error.
func (f *FakeScheduleRepository) LoadForUpdate(_ context.Context, tenantID, name string) (*schedule.Schedule, bool, error) {
	if !f.inTx() {
		return nil, false, fmt.Errorf("lock schedule %q of tenant %q: LoadForUpdate requires an active transaction", name, tenantID)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.get(tenantID, name)
}

// ListWithUnreadable returns the tenant's schedules that parse, ordered by
// name, and apart from them the names of those that do not.
func (f *FakeScheduleRepository) ListWithUnreadable(_ context.Context, tenantID string) ([]*schedule.Schedule, []string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.FailWith != nil {
		return nil, nil, f.FailWith
	}
	var rows []*StoredSchedule
	for k, row := range f.rows {
		if k.TenantID == tenantID {
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	schedules := make([]*schedule.Schedule, 0, len(rows))
	unreadable := []string{}
	for _, row := range rows {
		s, err := f.hydrate(row)
		var invalid *schedule.StoredCronInvalidError
		switch {
		case errors.As(err, &invalid):
			unreadable = append(unreadable, row.Name)
		case err != nil:
			return nil, nil, err
		default:
			schedules = append(schedules, s)
		}
	}
	return schedules, unreadable, nil
}

// Save writes s while the stored row is at expectedVersion, as the adapter
// does: above 0 it replaces that row, 0 only inserts. Any other case is
// schedule.ErrStaleVersion and writes nothing.
func (f *FakeScheduleRepository) Save(_ context.Context, s *schedule.Schedule, expectedVersion int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saves++
	if f.FailWith != nil {
		return f.FailWith
	}
	k := ScheduleKey{s.TenantID(), s.Name()}
	stored, exists := f.rows[k]
	switch {
	case expectedVersion > 0 && (!exists || stored.Version != expectedVersion):
		return fmt.Errorf("save schedule %q of tenant %q at version %d: %w", s.Name(), s.TenantID(), expectedVersion, schedule.ErrStaleVersion)
	case expectedVersion <= 0 && exists:
		return fmt.Errorf("create schedule %q of tenant %q: it has a row already: %w", s.Name(), s.TenantID(), schedule.ErrStaleVersion)
	}
	row := &StoredSchedule{
		TenantID: s.TenantID(), Name: s.Name(), Cron: s.Cron(), Timezone: s.Timezone(), Description: s.Description(),
		Paused: s.Paused(), Version: s.Version(), UpdatedBy: s.UpdatedBy(), UpdatedAt: s.UpdatedAt(),
	}
	if next, ok := s.NextFireAt(); ok {
		row.NextFireAt = &next
	}
	if exists {
		row.Expression = stored.Expression
	}
	f.rows[k] = row
	f.notify()
	return nil
}

// Delete removes the row, whatever its cron, and reports whether there was one.
func (f *FakeScheduleRepository) Delete(_ context.Context, tenantID, name string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.FailWith != nil {
		return false, f.FailWith
	}
	k := ScheduleKey{tenantID, name}
	_, ok := f.rows[k]
	delete(f.rows, k)
	if ok {
		f.notify()
	}
	return ok, nil
}

// ListDue returns, oldest next fire first, up to limit schedules of any tenant
// that are not paused and whose next fire is at or before now. A row that
// does not parse is left out and takes none of limit's places.
func (f *FakeScheduleRepository) ListDue(_ context.Context, now time.Time, limit int) ([]*schedule.Schedule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listDue++
	if f.FailWith != nil {
		return nil, f.FailWith
	}
	var due []*StoredSchedule
	for _, row := range f.rows {
		if row.NextFireAt != nil && !row.NextFireAt.After(now) && !row.Paused && f.readable(row) {
			due = append(due, row)
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if !due[i].NextFireAt.Equal(*due[j].NextFireAt) {
			return due[i].NextFireAt.Before(*due[j].NextFireAt)
		}
		if due[i].TenantID != due[j].TenantID {
			return due[i].TenantID < due[j].TenantID
		}
		return due[i].Name < due[j].Name
	})
	if len(due) > limit {
		due = due[:limit]
	}
	out := make([]*schedule.Schedule, 0, len(due))
	for _, row := range due {
		s, err := f.hydrate(row)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

// EarliestFireAfter returns the earliest next fire strictly after `after` of
// the rows that are not paused, whether or not their cron parses.
func (f *FakeScheduleRepository) EarliestFireAfter(_ context.Context, after time.Time) (time.Time, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.FailWith != nil {
		return time.Time{}, false, f.FailWith
	}
	var earliest *time.Time
	for _, row := range f.rows {
		if row.NextFireAt != nil && !row.Paused && row.NextFireAt.After(after) && (earliest == nil || row.NextFireAt.Before(*earliest)) {
			earliest = row.NextFireAt
		}
	}
	if earliest == nil {
		return time.Time{}, false, nil
	}
	return *earliest, true, nil
}

// Count counts each row once: unparsable when its cron or timezone does not
// parse, else paused, else scheduled.
func (f *FakeScheduleRepository) Count(context.Context) (scheduled, paused, unparsable int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.FailWith != nil {
		return 0, 0, 0, f.FailWith
	}
	for _, row := range f.rows {
		switch {
		case !f.readable(row):
			unparsable++
		case row.Paused:
			paused++
		default:
			scheduled++
		}
	}
	return scheduled, paused, unparsable, nil
}
