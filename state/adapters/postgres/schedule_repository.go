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
)

// ScheduleRepository implements repository.ScheduleRepository over schedules.
// Each method is one statement, which runs in the unit of work's transaction
// when one is open, else on the autocommit db. It rebuilds the expression of
// each schedule it loads by parsing the stored cron with parser. A stored cron
// that no longer parses fails Get; List and LockDue log it to logger and
// return the other schedules.
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

// List reads the tenant's schedules, ordered by name, leaving out and logging
// each whose stored cron or timezone no longer parses.
func (r *ScheduleRepository) List(ctx context.Context, tenantID string) ([]*schedule.Schedule, error) {
	var rows []scheduleRow
	if err := sqlx.SelectContext(ctx, r.conn(), &rows,
		`SELECT `+scheduleColumns+` FROM schedules WHERE tenant_id = $1 ORDER BY name`, tenantID); err != nil {
		return nil, fmt.Errorf("list schedules of tenant %q: %w", tenantID, err)
	}
	return r.hydrateAll(rows)
}

// ListStoredCronInvalid reads the cron and timezone of each of the tenant's
// schedules, ordered by name, and names the ones the parser refuses. A
// repository with no parser cannot tell, so it is an error.
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
// has another version or is gone. With 0 it is an upsert on the primary key: an
// INSERT that, when the row exists or another transaction is inserting it, waits
// for that transaction and then overwrites the row, so concurrent creators of
// one name end as one row and none sees the key conflict.
func (r *ScheduleRepository) Save(ctx context.Context, s *schedule.Schedule, expectedVersion int64) error {
	nextFireAt := sql.NullTime{}
	if next, ok := s.NextFireAt(); ok {
		nextFireAt = sql.NullTime{Time: next, Valid: true}
	}
	if expectedVersion > 0 {
		res, err := r.conn().ExecContext(ctx, `
			UPDATE schedules
			   SET cron = $3, timezone = $4, description = $5, paused = $6, next_fire_at = $7,
			       version = $8, updated_by = $9, updated_at = $10
			 WHERE tenant_id = $1 AND name = $2 AND version = $11`,
			s.TenantID(), s.Name(), s.Cron(), s.Timezone(), s.Description(), s.Paused(), nextFireAt,
			s.Version(), s.UpdatedBy(), s.UpdatedAt(), expectedVersion)
		if err != nil {
			return fmt.Errorf("save schedule %q of tenant %q: %w", s.Name(), s.TenantID(), err)
		}
		changed, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("save schedule %q of tenant %q: %w", s.Name(), s.TenantID(), err)
		}
		if changed == 0 {
			return fmt.Errorf("save schedule %q of tenant %q at version %d: %w",
				s.Name(), s.TenantID(), expectedVersion, schedule.ErrStaleVersion)
		}
		return nil
	}
	if _, err := r.conn().ExecContext(ctx, `
		INSERT INTO schedules (`+scheduleColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (tenant_id, name) DO UPDATE
		   SET cron = EXCLUDED.cron, timezone = EXCLUDED.timezone, description = EXCLUDED.description,
		       paused = EXCLUDED.paused, next_fire_at = EXCLUDED.next_fire_at, version = EXCLUDED.version,
		       updated_by = EXCLUDED.updated_by, updated_at = EXCLUDED.updated_at`,
		s.TenantID(), s.Name(), s.Cron(), s.Timezone(), s.Description(), s.Paused(), nextFireAt,
		s.Version(), s.UpdatedBy(), s.UpdatedAt()); err != nil {
		return fmt.Errorf("save schedule %q of tenant %q: %w", s.Name(), s.TenantID(), err)
	}
	return nil
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

// LockDue selects the due schedules of every tenant with FOR UPDATE SKIP
// LOCKED, so the rows stay locked to the open transaction and a row another
// transaction holds is passed over. Outside a transaction the lock ends with
// the statement. A due row whose stored cron or timezone no longer parses is
// locked like the others, then logged and left out of the result.
func (r *ScheduleRepository) LockDue(ctx context.Context, now time.Time, limit int) ([]*schedule.Schedule, error) {
	var rows []scheduleRow
	if err := sqlx.SelectContext(ctx, r.conn(), &rows, `
		SELECT `+scheduleColumns+`
		  FROM schedules
		 WHERE next_fire_at IS NOT NULL AND next_fire_at <= $1
		 ORDER BY next_fire_at, tenant_id, name
		 LIMIT $2
		   FOR UPDATE SKIP LOCKED`, now, limit); err != nil {
		return nil, fmt.Errorf("lock the due schedules: %w", err)
	}
	return r.hydrateAll(rows)
}

// EarliestFire reads the earliest next fire among the schedules of every tenant.
func (r *ScheduleRepository) EarliestFire(ctx context.Context) (time.Time, bool, error) {
	var earliest sql.NullTime
	if err := sqlx.GetContext(ctx, r.conn(), &earliest, `SELECT min(next_fire_at) FROM schedules`); err != nil {
		return time.Time{}, false, fmt.Errorf("read the earliest schedule fire: %w", err)
	}
	if !earliest.Valid {
		return time.Time{}, false, nil
	}
	return earliest.Time.UTC(), true, nil
}
