//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/run-controller/adapters/postgres"
	"github.com/carolsimone/continuo/run-controller/domain/repository/repositorytest"
)

// planted is the repository the contract suite drives, bound to a transaction
// that is rolled back when the test ends, with a way to plant rows through it.
type plantedSchedules struct {
	*postgres.ScheduleRepository
	tx *sqlx.Tx
}

func (p plantedSchedules) Plant(t *testing.T, row repositorytest.PlantedSchedule) {
	t.Helper()
	var next sql.NullTime
	if row.NextFireAt != nil {
		next = sql.NullTime{Time: *row.NextFireAt, Valid: true}
	}
	_, err := p.tx.Exec(`
		INSERT INTO schedules (tenant_id, name, cron, timezone, paused, next_fire_at, version, updated_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'contract')`,
		row.TenantID, row.Name, row.Cron, row.Timezone, row.Paused, next, row.Version)
	require.NoError(t, err)
}

// The Postgres adapter holds the behaviour every ScheduleRepository shares. Each
// test runs in a REPEATABLE READ transaction that is rolled back, so its rows
// are seen by no other session and the counts it reads do not move under it.
func TestScheduleRepository_HoldsTheScheduleContract(t *testing.T) {
	db := newTestDB(t)
	parser := scheduleParser(t)
	repositorytest.RunScheduleContract(t, parser, func(t *testing.T) repositorytest.ScheduleStore {
		tx, err := db.BeginTxx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		return plantedSchedules{ScheduleRepository: newScheduleRepo(db, tx, parser), tx: tx}
	})
}
