package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
)

// durationTask inserts a task of runID at service.raw.table with operation
// and status, inherited from inheritedFrom when it is not nil.
func durationTask(t *testing.T, db *sqlx.DB, runID uuid.UUID, service, table, operation, status string, inheritedFrom *uuid.UUID) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.Exec(`
		INSERT INTO task_tracker (task_id, schedule_id, service_name, schema_name, table_name, job_name, status, max_retries, node_key, operation, inherited_from_task_id)
		VALUES ($1, $2, $3, 'raw', $4::text, $5, $6, 2, 'raw.' || $4::text, $7, $8)`,
		id, runID, service, table, "job-"+id.String()[:8], status, operation, inheritedFrom)
	require.NoError(t, err)
	return id
}

// execution inserts an execution of taskID created at, taking seconds, with
// errorMessage when it is not empty.
func execution(t *testing.T, db *sqlx.DB, taskID uuid.UUID, at time.Time, seconds *float64, errorMessage string) {
	t.Helper()
	var msg *string
	if errorMessage != "" {
		msg = &errorMessage
	}
	_, err := db.Exec(`
		INSERT INTO task_execution (task_id, created_at, started_at, completed_at, execution_time_seconds, error_message)
		VALUES ($1, $2, $2, $2, $3, $4)`, taskID, at, seconds, msg)
	require.NoError(t, err)
}

func seconds(s float64) *float64 { return &s }

// A node's last successful duration is its latest execution without an error
// and with a positive duration, of a succeeded task that is not a test. A
// node without one is absent.
func TestRunReadRepository_LastSuccessDurations(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	svc := "dur-" + uuid.NewString()[:8]
	older := readRun(t, db, "durations", nil, nil, "succeeded", true)
	newer := readRun(t, db, "durations", nil, nil, "failed", true)
	t0 := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)

	// raw.orders succeeded twice; the newer success is the answer, whatever
	// failed or recorded no duration after it.
	first := durationTask(t, db, older, svc, "orders", "run", "succeeded", nil)
	execution(t, db, first, t0, seconds(10.5), "")
	second := durationTask(t, db, newer, svc, "orders", "build", "succeeded", nil)
	execution(t, db, second, t0.Add(time.Minute), seconds(99), "boom")
	execution(t, db, second, t0.Add(2*time.Minute), seconds(20.25), "")
	execution(t, db, second, t0.Add(3*time.Minute), seconds(0), "")
	execution(t, db, second, t0.Add(4*time.Minute), nil, "")
	// raw.tested succeeded only as a test.
	tested := durationTask(t, db, older, svc, "tested", "test", "succeeded", nil)
	execution(t, db, tested, t0, seconds(3), "")
	// raw.broken never succeeded.
	broken := durationTask(t, db, newer, svc, "broken", "run", "failed", nil)
	execution(t, db, broken, t0, seconds(7), "")
	// raw.carried succeeded only by being inherited, which records no
	// execution.
	durationTask(t, db, newer, svc, "carried", "run", "succeeded", &first)

	node := func(table string) run.NodeID {
		return run.NodeID{ServiceName: svc, SchemaName: "raw", TableName: table}
	}
	got, err := postgres.NewRunReadRepository(db).LastSuccessDurations(ctx, []run.NodeID{
		node("orders"), node("tested"), node("broken"), node("carried"), node("absent"), node("orders"),
	})

	require.NoError(t, err)
	assert.Equal(t, map[run.NodeID]time.Duration{node("orders"): 20250 * time.Millisecond}, got)
}

func TestRunReadRepository_LastSuccessDurationsOfNoNode(t *testing.T) {
	got, err := postgres.NewRunReadRepository(newTestDB(t)).LastSuccessDurations(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}
