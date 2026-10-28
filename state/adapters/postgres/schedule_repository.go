package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/schedule"
	ports "github.com/carolsimone/continuo/state/service/ports"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// SchedulesChannel is the Postgres channel the schedules table's statement
// trigger notifies once a transaction that changed schedules commits.
const SchedulesChannel = "schedules"

// ScheduleRepository implements repository.ScheduleRepository over schedules.
// Each method but ListDue is one statement; ListDue runs two or three. Every
// statement runs in the unit of work's transaction when one is open, else on
// the autocommit db, and under READ COMMITTED each sees the rows committed when
// it starts, so the statements of one ListDue can see different rows, inside a
// transaction as outside one. It rebuilds the expression of
// each schedule it loads by parsing the stored cron with parser. A stored cron
// that no longer parses fails Get and LoadForUpdate; List and ListDue log it to
// logger and return the other schedules.
type ScheduleRepository struct {
	db     *sqlx.DB
	tx     *sqlx.Tx
	parser ports.CronParser
	logger *slog.Logger
}

var _ repository.ScheduleRepository = (*ScheduleRepository)(nil)

// NewScheduleRepository binds the repository to db and tx (nil outside a
// transaction). parser reads the cron of each schedule the repository loads;
// logger records the schedules it leaves out (slog.Default when nil).
func NewScheduleRepository(db *sqlx.DB, tx *sqlx.Tx, parser ports.CronParser, logger *slog.Logger) *ScheduleRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &ScheduleRepository{db: db, tx: tx, parser: parser, logger: logger}
}

func (r *ScheduleRepository) conn() sqlx.ExtContext {
	if r.tx != nil {
		return r.tx
	}
	return r.db
}

// scheduleColumns lists the columns of a schedules row, in the order of
// scheduleRow's fields.
const scheduleColumns = `tenant_id, name, cron, timezone, description, paused, next_fire_at, version, updated_by, updated_at`

type scheduleRow struct {
	TenantID    string       `db:"tenant_id"`
	Name        string       `db:"name"`
	Cron        string       `db:"cron"`
	Timezone    string       `db:"timezone"`
	Description string       `db:"description"`
	Paused      bool         `db:"paused"`
	NextFireAt  sql.NullTime `db:"next_fire_at"`
	Version     int64        `db:"version"`
	UpdatedBy   string       `db:"updated_by"`
	UpdatedAt   time.Time    `db:"updated_at"`
}

// hydrate rebuilds the schedule a row stores. A cron or timezone that no longer
// parses is a *schedule.StoredCronInvalidError that names the schedule and the
// row's version; a repository with no parser is a plain error, which no row
// causes.
func (r *ScheduleRepository) hydrate(row scheduleRow) (*schedule.Schedule, error) {
	if r.parser == nil {
		return nil, fmt.Errorf("read schedule %q of tenant %q: the repository has no cron parser", row.Name, row.TenantID)
	}
	expr, err := r.parser.Parse(row.Cron, row.Timezone)
	if err != nil {
		return nil, &schedule.StoredCronInvalidError{
			TenantID: row.TenantID, Name: row.Name, Cron: row.Cron, Timezone: row.Timezone,
			Version: row.Version, Reason: err,
		}
	}
	var nextFireAt *time.Time
	if row.NextFireAt.Valid {
		next := row.NextFireAt.Time.UTC()
		nextFireAt = &next
	}
	return schedule.Rehydrate(row.TenantID, row.Name, row.Cron, row.Timezone, row.Description,
		row.Paused, nextFireAt, row.Version, row.UpdatedBy, row.UpdatedAt.UTC(), expr), nil
}

// hydrateAll rebuilds the schedules the rows store. It logs and leaves out each
// row whose stored cron or timezone no longer parses, so one bad row does not
// hide the others; any other error ends it.
func (r *ScheduleRepository) hydrateAll(rows []scheduleRow) ([]*schedule.Schedule, error) {
	out := make([]*schedule.Schedule, 0, len(rows))
	for _, row := range rows {
		s, err := r.hydrate(row)
		if errors.Is(err, schedule.ErrStoredCronInvalid) {
			r.logger.Error("Left out a schedule whose stored cron or timezone no longer parses",
				"tenant_id", row.TenantID, "schedule_name", row.Name, "cron", row.Cron, "timezone", row.Timezone, "error", err)
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

// Get reads the tenant's schedule of that name. A stored cron or timezone that
// no longer parses is a *schedule.StoredCronInvalidError.
func (r *ScheduleRepository) Get(ctx context.Context, tenantID, name string) (*schedule.Schedule, bool, error) {
	var row scheduleRow
	err := sqlx.GetContext(ctx, r.conn(), &row,
		`SELECT `+scheduleColumns+` FROM schedules WHERE tenant_id = $1 AND name = $2`, tenantID, name)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get schedule %q of tenant %q: %w", name, tenantID, err)
	}
	s, err := r.hydrate(row)
	if err != nil {
		return nil, false, err
	}
	return s, true, nil
}

// Exists reads whether the tenant has a schedule of that name, without
// parsing its stored cron.
func (r *ScheduleRepository) Exists(ctx context.Context, tenantID, name string) (bool, error) {
	var exists bool
	if err := sqlx.GetContext(ctx, r.conn(), &exists,
		`SELECT EXISTS (SELECT 1 FROM schedules WHERE tenant_id = $1 AND name = $2)`, tenantID, name); err != nil {
		return false, fmt.Errorf("read whether tenant %q has schedule %q: %w", tenantID, name, err)
	}
	return exists, nil
}

// LoadForUpdate reads the tenant's schedule of that name with FOR UPDATE in the
// bound transaction, so the row stays locked until it ends. A stored cron or
// timezone that no longer parses is a *schedule.StoredCronInvalidError, and the
// row stays locked.
func (r *ScheduleRepository) LoadForUpdate(ctx context.Context, tenantID, name string) (*schedule.Schedule, bool, error) {
	if r.tx == nil {
		return nil, false, fmt.Errorf("lock schedule %q of tenant %q: LoadForUpdate requires an active transaction", name, tenantID)
	}
	var row scheduleRow
	err := r.tx.GetContext(ctx, &row,
		`SELECT `+scheduleColumns+` FROM schedules WHERE tenant_id = $1 AND name = $2 FOR UPDATE`, tenantID, name)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("lock schedule %q of tenant %q: %w", name, tenantID, err)
	}
	s, err := r.hydrate(row)
	if err != nil {
		return nil, false, err
	}
	return s, true, nil
}

// List reads the tenant's schedules, ordered by name byte by byte (the "C"
// collation, Go's string order), leaving out and logging each whose stored
// cron or timezone no longer parses.
func (r *ScheduleRepository) List(ctx context.Context, tenantID string) ([]*schedule.Schedule, error) {
	var rows []scheduleRow
	if err := sqlx.SelectContext(ctx, r.conn(), &rows,
		`SELECT `+scheduleColumns+` FROM schedules WHERE tenant_id = $1 ORDER BY name COLLATE "C"`, tenantID); err != nil {
		return nil, fmt.Errorf("list schedules of tenant %q: %w", tenantID, err)
	}
	return r.hydrateAll(rows)
}

// ListStoredCronInvalid reads the cron and timezone of each of the tenant's
// schedules, ordered by name as List orders them, and names the ones the
// parser refuses. A repository with no parser cannot tell, so it is an error.
func (r *ScheduleRepository) ListStoredCronInvalid(ctx context.Context, tenantID string) ([]string, error) {
	if r.parser == nil {
		return nil, fmt.Errorf("list the unparsable schedules of tenant %q: the repository has no cron parser", tenantID)
	}
	var rows []struct {
		Name     string `db:"name"`
		Cron     string `db:"cron"`
		Timezone string `db:"timezone"`
	}
	if err := sqlx.SelectContext(ctx, r.conn(), &rows,
		`SELECT name, cron, timezone FROM schedules WHERE tenant_id = $1 ORDER BY name COLLATE "C"`, tenantID); err != nil {
		return nil, fmt.Errorf("list the unparsable schedules of tenant %q: %w", tenantID, err)
	}
	names := []string{}
	for _, row := range rows {
		if _, err := r.parser.Parse(row.Cron, row.Timezone); err != nil {
			names = append(names, row.Name)
		}
	}
	return names, nil
}

// Save writes s as it stands, in one statement. With expectedVersion above 0 it
// is an UPDATE of the row at that version, which matches nothing when the row
// has another version or is gone. With 0 it is an INSERT that does nothing on a
// conflict of the primary key: when another transaction is inserting the row it
// waits for that transaction, and a row that exists by then is left as it is.
// So of concurrent creators of one name one writes, the others are stale, and
// none sees the key conflict. A save that matches no row is
// schedule.ErrStaleVersion and leaves the transaction usable.
func (r *ScheduleRepository) Save(ctx context.Context, s *schedule.Schedule, expectedVersion int64) error {
	nextFireAt := sql.NullTime{}
	if next, ok := s.NextFireAt(); ok {
		nextFireAt = sql.NullTime{Time: next, Valid: true}
	}
	var (
		res sql.Result
		err error
	)
	if expectedVersion > 0 {
		res, err = r.conn().ExecContext(ctx, `
			UPDATE schedules
			   SET cron = $3, timezone = $4, description = $5, paused = $6, next_fire_at = $7,
			       version = $8, updated_by = $9, updated_at = $10
			 WHERE tenant_id = $1 AND name = $2 AND version = $11`,
			s.TenantID(), s.Name(), s.Cron(), s.Timezone(), s.Description(), s.Paused(), nextFireAt,
			s.Version(), s.UpdatedBy(), s.UpdatedAt(), expectedVersion)
	} else {
		res, err = r.conn().ExecContext(ctx, `
			INSERT INTO schedules (`+scheduleColumns+`)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (tenant_id, name) DO NOTHING`,
			s.TenantID(), s.Name(), s.Cron(), s.Timezone(), s.Description(), s.Paused(), nextFireAt,
			s.Version(), s.UpdatedBy(), s.UpdatedAt())
	}
	if err != nil {
		return fmt.Errorf("save schedule %q of tenant %q: %w", s.Name(), s.TenantID(), err)
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("save schedule %q of tenant %q: %w", s.Name(), s.TenantID(), err)
	}
	switch {
	case changed > 0:
		return nil
	case expectedVersion > 0:
		return fmt.Errorf("save schedule %q of tenant %q at version %d: %w",
			s.Name(), s.TenantID(), expectedVersion, schedule.ErrStaleVersion)
	default:
		return fmt.Errorf("create schedule %q of tenant %q: it has a row already: %w",
			s.Name(), s.TenantID(), schedule.ErrStaleVersion)
	}
}

// Delete removes the tenant's schedule of that name.
func (r *ScheduleRepository) Delete(ctx context.Context, tenantID, name string) (bool, error) {
	res, err := r.conn().ExecContext(ctx, `DELETE FROM schedules WHERE tenant_id = $1 AND name = $2`, tenantID, name)
	if err != nil {
		return false, fmt.Errorf("delete schedule %q of tenant %q: %w", name, tenantID, err)
	}
	deleted, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("delete schedule %q of tenant %q: %w", name, tenantID, err)
	}
	return deleted > 0, nil
}

// dueRows is the condition of the rows due at $1: not paused, with a next fire
// at or before it. A paused row is never due, whatever its next_fire_at holds.
const dueRows = `next_fire_at IS NOT NULL AND next_fire_at <= $1 AND NOT paused`

// maxNamedUnreadableDue is how many of the due rows whose stored cron or
// timezone no longer parses one ListDue logs one by one, oldest first; it counts
// the rest in one more line.
const maxNamedUnreadableDue = 10

// unreadablePair is a stored cron and timezone the parser refuses, with its
// reason.
type unreadablePair struct {
	cron, timezone string
	reason         error
}

// pairColumns returns the crons and the timezones of the pairs, pair by pair:
// the two arrays a statement unnests together to match a row's cron and
// timezone.
func pairColumns(pairs []unreadablePair) (crons, zones []string) {
	crons = make([]string, 0, len(pairs))
	zones = make([]string, 0, len(pairs))
	for _, pair := range pairs {
		crons = append(crons, pair.cron)
		zones = append(zones, pair.timezone)
	}
	return crons, zones
}

// ListDue selects the due schedules of every tenant whose stored cron and
// timezone parse, with FOR UPDATE SKIP LOCKED, so a row another transaction
// holds at that moment is passed over. Outside a transaction, as the cron loop
// calls it, the lock ends with the statement; inside one, the rows it returns
// stay locked to that transaction.
//
// A row's expression is a function of its cron and timezone alone, so ListDue
// first reads the distinct pairs of the due rows, without a lock, and parses
// each once. The select of the due rows then leaves out every row of a pair the
// parser refuses: such a row takes none of the limit's places, however many
// there are and wherever they sort, and is neither locked nor returned. ListDue
// logs the first maxNamedUnreadableDue of them by name and the others by count.
// A row whose cron another transaction changed between the two reads to one
// that does not parse is logged and left out of the result like any row that
// does not parse.
func (r *ScheduleRepository) ListDue(ctx context.Context, now time.Time, limit int) ([]*schedule.Schedule, error) {
	if r.parser == nil {
		return nil, errors.New("list the due schedules: the repository has no cron parser")
	}
	unreadable, err := r.unreadableDuePairs(ctx, now)
	if err != nil {
		return nil, err
	}
	if len(unreadable) > 0 {
		if err := r.logUnreadableDue(ctx, now, unreadable); err != nil {
			return nil, err
		}
	}
	crons, zones := pairColumns(unreadable)
	var rows []scheduleRow
	if err := sqlx.SelectContext(ctx, r.conn(), &rows, `
		SELECT `+scheduleColumns+`
		  FROM schedules
		 WHERE `+dueRows+`
		   AND NOT EXISTS (SELECT 1 FROM unnest($3::text[], $4::text[]) AS unreadable (cron, timezone)
		                    WHERE unreadable.cron = schedules.cron AND unreadable.timezone = schedules.timezone)
		 ORDER BY next_fire_at, tenant_id, name
		 LIMIT $2
		   FOR UPDATE SKIP LOCKED`, now, limit, pq.Array(crons), pq.Array(zones)); err != nil {
		return nil, fmt.Errorf("list the due schedules: %w", err)
	}
	return r.hydrateAll(rows)
}

// unreadableDuePairs reads the distinct crons and timezones of the rows due at
// now and returns the pairs the parser refuses.
func (r *ScheduleRepository) unreadableDuePairs(ctx context.Context, now time.Time) ([]unreadablePair, error) {
	var pairs []struct {
		Cron     string `db:"cron"`
		Timezone string `db:"timezone"`
	}
	if err := sqlx.SelectContext(ctx, r.conn(), &pairs,
		`SELECT DISTINCT cron, timezone FROM schedules WHERE `+dueRows, now); err != nil {
		return nil, fmt.Errorf("read the crons of the due schedules: %w", err)
	}
	var unreadable []unreadablePair
	for _, pair := range pairs {
		if _, err := r.parser.Parse(pair.Cron, pair.Timezone); err != nil {
			unreadable = append(unreadable, unreadablePair{cron: pair.Cron, timezone: pair.Timezone, reason: err})
		}
	}
	return unreadable, nil
}

// logUnreadableDue logs the rows due at now whose cron and timezone are one of
// the unreadable pairs, oldest next fire first: each of the first
// maxNamedUnreadableDue at ERROR with its tenant, name, cron, timezone and the
// parser's reason, then, when there are more, one ERROR line with how many.
func (r *ScheduleRepository) logUnreadableDue(ctx context.Context, now time.Time, unreadable []unreadablePair) error {
	crons, zones := pairColumns(unreadable)
	var rows []struct {
		TenantID string `db:"tenant_id"`
		Name     string `db:"name"`
		Cron     string `db:"cron"`
		Timezone string `db:"timezone"`
		Total    int    `db:"total"`
	}
	if err := sqlx.SelectContext(ctx, r.conn(), &rows, `
		SELECT tenant_id, name, cron, timezone, count(*) OVER () AS total
		  FROM schedules
		 WHERE `+dueRows+`
		   AND EXISTS (SELECT 1 FROM unnest($2::text[], $3::text[]) AS unreadable (cron, timezone)
		                WHERE unreadable.cron = schedules.cron AND unreadable.timezone = schedules.timezone)
		 ORDER BY next_fire_at, tenant_id, name
		 LIMIT $4`, now, pq.Array(crons), pq.Array(zones), maxNamedUnreadableDue); err != nil {
		return fmt.Errorf("read the due schedules whose stored cron no longer parses: %w", err)
	}
	reasons := make(map[[2]string]error, len(unreadable))
	for _, pair := range unreadable {
		reasons[[2]string{pair.cron, pair.timezone}] = pair.reason
	}
	for _, row := range rows {
		r.logger.Error("Left out a due schedule whose stored cron or timezone no longer parses",
			"tenant_id", row.TenantID, "schedule_name", row.Name, "cron", row.Cron, "timezone", row.Timezone,
			"error", reasons[[2]string{row.Cron, row.Timezone}])
	}
	if len(rows) > 0 && rows[0].Total > len(rows) {
		r.logger.Error("Left out more due schedules whose stored cron or timezone no longer parses; ListSchedules lists each with no cron",
			"more", rows[0].Total-len(rows), "left_out", rows[0].Total)
	}
	return nil
}

// EarliestFireAfter reads the earliest next fire after `after` among the
// schedules of every tenant that are not paused. next_fire_at holds
// microseconds, and Postgres rounds a finer parameter to the nearest one, which
// would leave out a fire less than half a microsecond after `after`. So `after`
// is truncated to the microsecond first: no stored microsecond lies between the
// two, and the comparison stays exactly "strictly after".
func (r *ScheduleRepository) EarliestFireAfter(ctx context.Context, after time.Time) (time.Time, bool, error) {
	var earliest sql.NullTime
	if err := sqlx.GetContext(ctx, r.conn(), &earliest,
		`SELECT min(next_fire_at) FROM schedules WHERE next_fire_at > $1 AND NOT paused`, after.Truncate(time.Microsecond)); err != nil {
		return time.Time{}, false, fmt.Errorf("read the earliest schedule fire: %w", err)
	}
	if !earliest.Valid {
		return time.Time{}, false, nil
	}
	return earliest.Time.UTC(), true, nil
}

// Count counts the schedules of every tenant that are not paused and those
// that are, in one scan.
func (r *ScheduleRepository) Count(ctx context.Context) (scheduled, paused int, err error) {
	var counts struct {
		Scheduled int `db:"scheduled"`
		Paused    int `db:"paused"`
	}
	if err := sqlx.GetContext(ctx, r.conn(), &counts, `
		SELECT count(*) FILTER (WHERE NOT paused) AS scheduled,
		       count(*) FILTER (WHERE paused)     AS paused
		  FROM schedules`); err != nil {
		return 0, 0, fmt.Errorf("count the schedules: %w", err)
	}
	return counts.Scheduled, counts.Paused, nil
}
