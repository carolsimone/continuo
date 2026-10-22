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

// attemptOf inserts attempt number attempt of taskID with status, finished at
// and taking seconds (nil when it recorded no duration).
func attemptOf(t *testing.T, db *sqlx.DB, taskID uuid.UUID, attempt int, status string, at time.Time, seconds *float64) {
	t.Helper()
	insertAttemptRow(t, db, storedAttempt{
		TaskID: taskID, Attempt: attempt, Status: status,
		StartedAt: &at, FinishedAt: &at, ExecutionSeconds: seconds,
	})
}

func seconds(s float64) *float64 { return &s }

// A node's last successful duration is the duration of its latest succeeded
// attempt with a positive duration, of a succeeded task that is not a test. A
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
	attemptOf(t, db, first, 0, "succeeded", t0, seconds(10.5))
	second := durationTask(t, db, newer, svc, "orders", "build", "succeeded", nil)
	attemptOf(t, db, second, 0, "failed", t0.Add(time.Minute), seconds(99))
	attemptOf(t, db, second, 1, "succeeded", t0.Add(2*time.Minute), seconds(20.25))
	attemptOf(t, db, second, 2, "succeeded", t0.Add(3*time.Minute), seconds(0))
	attemptOf(t, db, second, 3, "succeeded", t0.Add(4*time.Minute), nil)
	// raw.tested succeeded only as a test.
	tested := durationTask(t, db, older, svc, "tested", "test", "succeeded", nil)
	attemptOf(t, db, tested, 0, "succeeded", t0, seconds(3))
	// raw.broken never succeeded.
	broken := durationTask(t, db, newer, svc, "broken", "run", "failed", nil)
	attemptOf(t, db, broken, 0, "failed", t0, seconds(7))
	// raw.carried succeeded only by being inherited, which records no
	// attempt.
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

// A task that failed first and succeeded on a retry answers with the
// succeeded attempt's duration, not the failed attempt's.
func TestRunReadRepository_LastSuccessDurationsReadsTheSucceededAttempt(t *testing.T) {
	db := newTestDB(t)
	svc := "dur-" + uuid.NewString()[:8]
	r := readRun(t, db, "durations", nil, nil, "succeeded", true)
	t0 := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	task := durationTask(t, db, r, svc, "orders", "run", "succeeded", nil)
	attemptOf(t, db, task, 0, "failed", t0, seconds(30))
	attemptOf(t, db, task, 1, "succeeded", t0.Add(time.Minute), seconds(12))

	got, err := postgres.NewRunReadRepository(db).LastSuccessDurations(context.Background(),
		[]run.NodeID{{ServiceName: svc, SchemaName: "raw", TableName: "orders"}})

	require.NoError(t, err)
	assert.Equal(t, map[run.NodeID]time.Duration{{ServiceName: svc, SchemaName: "raw", TableName: "orders"}: 12 * time.Second}, got)
}

func TestRunReadRepository_LastSuccessDurationsOfNoNode(t *testing.T) {
	got, err := postgres.NewRunReadRepository(newTestDB(t)).LastSuccessDurations(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}
