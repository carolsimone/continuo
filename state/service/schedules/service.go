// Package schedules serves the crons of a tenant's schedules. An operator
// creates, replaces and removes the cron of a schedule name, previews a cron
// before saving it, and lists every schedule name the tenant's live release
// has or that has a cron. Which models a schedule runs comes from their dbt
// tag; a cron says only when it runs on its own. Each call names the tenant it
// acts for and sees only that tenant's schedules.
package schedules

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/schedule"
	"github.com/carolsimone/continuo/state/service/ports"
)

// upcomingFires is how many fire times Upsert and Preview return.
const upcomingFires = 3

// lastWriteWinsTries is how many times Upsert reads and saves an edit that
// names no expected version while other edits of the schedule keep saving
// between its read and its save.
const lastWriteWinsTries = 3

// ErrInvalid marks an input the service refuses before it reads or writes
// anything: an empty schedule name, a negative expected version, or a cron or
// timezone the cron parser refuses. The error's message is the reason alone,
// fit to show the operator; for a refusal of the parser it is the parser's
// own message.
var ErrInvalid = errors.New("invalid schedule input")

// invalidInput is an ErrInvalid whose message is its reason.
type invalidInput struct{ reason error }

func (e invalidInput) Error() string        { return e.reason.Error() }
func (e invalidInput) Is(target error) bool { return target == ErrInvalid }
func (e invalidInput) Unwrap() error        { return e.reason }

func invalid(reason error) error { return invalidInput{reason: reason} }

// errNameRequired is the reason an empty schedule name is refused.
var errNameRequired = errors.New("schedule_name is required")

// UpsertInput is an operator's edit of one schedule: the cron it runs on, read
// in the IANA timezone, what it is for, and whether it is paused.
type UpsertInput struct {
	Name        string
	Cron        string
	Timezone    string
	Description string
	Paused      bool
	// ExpectedVersion is the version the operator read before editing. Above
	// 0 the edit is saved only while the schedule is still at it; with 0 the
	// last write wins.
	ExpectedVersion int64
}

// Summary is one schedule name of a tenant: its cron, when it has one, how
// many models of the live release carry the name, and its last run.
type Summary struct {
	Name string
	// Schedule is the name's cron; nil when it has none, so it runs only when
	// triggered. A row whose stored cron no longer parses is shown as none.
	Schedule *schedule.Schedule
	// Words is the cron in English words; empty when the name has no cron or
	// the words library cannot read it.
	Words string
	// ModelCount is how many models of the live release carry the name. A
	// name with a cron and no models records a failed run at each fire.
	ModelCount int
	// LastRun is the name's most recent run; nil when it never ran.
	LastRun *repository.LastRunSummary
}

// HasActiveRun reports whether a pending, running or cancelling run holds the
// schedule.
func (s Summary) HasActiveRun() bool { return s.LastRun != nil && s.LastRun.IsActive }

// Service edits, previews and lists the schedules of the tenant each call
// names.
type Service struct {
	schedules repository.ScheduleRepository
	models    repository.LiveScheduleModels
	lastRuns  repository.LastRunReader
	parser    ports.CronParser
	clock     ports.Clock
}

// New returns a Service that stores schedules in schedules, checks a name
// against models, reads last runs from lastRuns, reads crons with parser and
// counts fires from clock's now.
func New(
	schedules repository.ScheduleRepository,
	models repository.LiveScheduleModels,
	lastRuns repository.LastRunReader,
	parser ports.CronParser,
	clock ports.Clock,
) *Service {
	return &Service{schedules: schedules, models: models, lastRuns: lastRuns, parser: parser, clock: clock}
}

// Upsert creates or replaces the cron of tenantID's schedule in.Name, as an
// edit made now by `by`, and returns the schedule as saved with its next three
// fires: none while it is paused. The next fire counts from now, and the
// version goes up by one from the stored row's.
//
// The edit is saved at the version of the row it read, so the stored version
// only ever goes up. With in.ExpectedVersion 0, an edit whose save finds that
// another edit saved first reads the row again and saves over it, up to three
// times in all: the last write wins.
//
// It returns ErrInvalid for an input it refuses, schedule.ErrStaleVersion when
// in.ExpectedVersion is above 0 and the schedule is not at that version (or
// has no row), or is 0 and other edits saved first three times, and
// schedule.ErrUnknownName when the name has no row and no model of the live
// release carries it. A name that has a row is edited whatever the live
// release holds, and a row whose stored cron no longer parses is saved over.
func (s *Service) Upsert(ctx context.Context, tenantID string, in UpsertInput, by string) (*schedule.Schedule, []time.Time, error) {
	if in.Name == "" {
		return nil, nil, invalid(errNameRequired)
	}
	if in.ExpectedVersion < 0 {
		return nil, nil, invalid(errors.New("expected_version must not be negative"))
	}
	cron := strings.TrimSpace(in.Cron)
	expr, err := s.parser.Parse(cron, in.Timezone)
	if err != nil {
		return nil, nil, invalid(err)
	}
	for range lastWriteWinsTries {
		var saved *schedule.Schedule
		saved, err = s.edit(ctx, tenantID, in, cron, expr, by)
		if err == nil {
			return saved, upcoming(saved, expr), nil
		}
		if in.ExpectedVersion > 0 || !errors.Is(err, schedule.ErrStaleVersion) {
			break
		}
	}
	return nil, nil, err
}

// edit reads tenantID's schedule in.Name, applies the edit to it and saves it
// at the version it read: the stored row's, or 0 when there is none. A save
// that finds the row at another version is schedule.ErrStaleVersion.
func (s *Service) edit(ctx context.Context, tenantID string, in UpsertInput, cron string, expr schedule.Expression, by string) (*schedule.Schedule, error) {
	stored, found, err := s.schedules.Get(ctx, tenantID, in.Name)
	var unreadable *schedule.StoredCronInvalidError
	switch {
	case errors.As(err, &unreadable):
		found = true
	case err != nil:
		return nil, fmt.Errorf("read schedule %q: %w", in.Name, err)
	}
	var storedVersion int64
	switch {
	case unreadable != nil:
		storedVersion = unreadable.Version
	case found:
		storedVersion = stored.Version()
	}
	// The expected version is compared with the row this edit builds on, so
	// the save below always raises the version the operator read.
	if in.ExpectedVersion > 0 && in.ExpectedVersion != storedVersion {
		return nil, fmt.Errorf("schedule %q is at version %d, not %d: %w",
			in.Name, storedVersion, in.ExpectedVersion, schedule.ErrStaleVersion)
	}
	if !found {
		models, err := s.models.CountOf(ctx, tenantID, in.Name)
		if err != nil {
			return nil, fmt.Errorf("count the live release's models of schedule %q: %w", in.Name, err)
		}
		if models == 0 {
			return nil, fmt.Errorf("schedule %q: %w", in.Name, schedule.ErrUnknownName)
		}
	}

	now := s.clock.Now()
	var saved *schedule.Schedule
	switch {
	case !found:
		saved = schedule.New(tenantID, in.Name, cron, in.Timezone, in.Description, in.Paused, expr, by, now)
	case unreadable != nil:
		// The stored row cannot be rebuilt, so the edit starts from its
		// version alone.
		saved = schedule.Rehydrate(tenantID, in.Name, cron, in.Timezone, in.Description, in.Paused,
			nil, storedVersion, by, now, expr)
		saved.Replace(cron, in.Timezone, in.Description, in.Paused, expr, by, now)
	default:
		saved = stored
		saved.Replace(cron, in.Timezone, in.Description, in.Paused, expr, by, now)
	}
	if err := s.schedules.Save(ctx, saved, storedVersion); err != nil {
		return nil, fmt.Errorf("save schedule %q: %w", in.Name, err)
	}
	return saved, nil
}

// Delete removes the cron of tenantID's schedule name, which then runs only
// when triggered. A schedule with no cron is left as it is, and a run of it in
// progress goes on. It returns ErrInvalid for an empty name.
func (s *Service) Delete(ctx context.Context, tenantID, name string) error {
	if name == "" {
		return invalid(errNameRequired)
	}
	if _, err := s.schedules.Delete(ctx, tenantID, name); err != nil {
		return fmt.Errorf("delete schedule %q: %w", name, err)
	}
	return nil
}

// Preview reads cron in timezone as Upsert would, and returns it in words with
// its next three fires from now, without writing anything. The words are empty
// when the words library cannot read a cron the parser fires. A cron or
// timezone the parser refuses is ErrInvalid.
func (s *Service) Preview(_ context.Context, cron, timezone string) (words string, next []time.Time, err error) {
	cron = strings.TrimSpace(cron)
	expr, err := s.parser.Parse(cron, timezone)
	if err != nil {
		return "", nil, invalid(err)
	}
	return s.words(cron), fires(expr, s.clock.Now(), upcomingFires), nil
}

// List returns one Summary per schedule name that tenantID's live release has
// or that has a cron, ordered by name. A schedule whose stored cron no longer
// parses is listed with no cron, whether or not the live release has its name,
// so the operator sees it and can save a new cron over it or remove it.
func (s *Service) List(ctx context.Context, tenantID string) ([]Summary, error) {
	models, err := s.models.CountByScheduleName(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("count the live release's models per schedule: %w", err)
	}
	rows, err := s.schedules.List(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list schedules: %w", err)
	}
	unreadable, err := s.schedules.ListStoredCronInvalid(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list the schedules whose stored cron no longer parses: %w", err)
	}
	lastRuns, err := s.lastRuns.GetLastRunPerSchedule(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("get last runs: %w", err)
	}

	byName := make(map[string]*Summary, len(models)+len(rows))
	for name, count := range models {
		byName[name] = &Summary{Name: name, ModelCount: count}
	}
	for _, row := range rows {
		sum, ok := byName[row.Name()]
		if !ok {
			sum = &Summary{Name: row.Name()}
			byName[row.Name()] = sum
		}
		sum.Schedule = row
		sum.Words = s.words(row.Cron())
	}
	for _, name := range unreadable {
		if _, ok := byName[name]; !ok {
			byName[name] = &Summary{Name: name}
		}
	}
	out := make([]Summary, 0, len(byName))
	for name, sum := range byName {
		if last, ok := lastRuns[name]; ok {
			sum.LastRun = &last
		}
		out = append(out, *sum)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// words returns cron in English words, or nothing when the words library
// cannot read it.
func (s *Service) words(cron string) string {
	words, err := s.parser.Describe(cron)
	if err != nil {
		return ""
	}
	return words
}

// upcoming returns the saved schedule's next fire and the fires after it, up
// to upcomingFires: none while it has no next fire.
func upcoming(saved *schedule.Schedule, expr schedule.Expression) []time.Time {
	next, ok := saved.NextFireAt()
	if !ok {
		return nil
	}
	return append([]time.Time{next}, fires(expr, next, upcomingFires-1)...)
}

// fires returns up to n fires of expr strictly after `after`, fewer when it
// fires no more.
func fires(expr schedule.Expression, after time.Time, n int) []time.Time {
	out := make([]time.Time, 0, n)
	for at := after; len(out) < n; {
		at = expr.Next(at)
		if at.IsZero() {
			break
		}
		out = append(out, at)
	}
	return out
}
