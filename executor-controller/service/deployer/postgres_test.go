//go:build integration

package deployer_test

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/service/deployer"
	executortest "github.com/carolsimone/continuo/executor-controller/test"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupPostgres(t *testing.T) (*sqlx.DB, func()) {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER": "testuser", "POSTGRES_PASSWORD": "testpass", "POSTGRES_DB": "testdb",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req, Started: true,
	})
	require.NoError(t, err)
	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "5432")
	require.NoError(t, err)
	if host == "localhost" {
		host = "127.0.0.1"
	}
	connStr := "host=" + host + " port=" + port.Port() + " user=testuser password=testpass dbname=testdb sslmode=disable"
	var db *sqlx.DB
	for i := 0; i < 10; i++ {
		if db, err = sqlx.Connect("postgres", connStr); err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.NoError(t, err)
	require.NoError(t, executortest.ApplyMigrations(db.DB))
	return db, func() {
		_ = db.Close()
		_ = container.Terminate(ctx)
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func newPending(t *testing.T, db *sqlx.DB, nextAttempt time.Time) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.Exec(
		`INSERT INTO executor_deployments (id, task_id, schedule_id, job_params, next_attempt_at)
		 VALUES ($1, $2, $3, '{}'::jsonb, $4)`,
		id, uuid.New(), uuid.New(), nextAttempt)
	require.NoError(t, err)
	return id
}

func TestRepo_GetDueBatch_OnlyDueRowsOldestFirst(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	past1 := newPending(t, db, time.Now().Add(-2*time.Minute))
	past2 := newPending(t, db, time.Now().Add(-1*time.Minute))
	_ = newPending(t, db, time.Now().Add(10*time.Minute)) // future: must be excluded

	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	txRepo := deployer.NewPostgresRepository(tx, testLogger())

	rows, err := txRepo.GetDueBatch(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, rows, 2, "future-dated row excluded")
	assert.Equal(t, past1, rows[0].ID, "oldest next_attempt_at first")
	assert.Equal(t, past2, rows[1].ID)
}

func TestRepo_MarkDeployed(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	repo := deployer.NewPostgresRepository(db, testLogger())
	id := newPending(t, db, time.Now())

	require.NoError(t, repo.MarkDeployed(context.Background(), id))

	var status string
	var deployedAt *time.Time
	require.NoError(t, db.QueryRow(`SELECT status, deployed_at FROM executor_deployments WHERE id=$1`, id).Scan(&status, &deployedAt))
	assert.Equal(t, "deployed", status)
	assert.NotNil(t, deployedAt)
}

func TestRepo_RescheduleBumpsRetryAndDelays(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	repo := deployer.NewPostgresRepository(db, testLogger())
	id := newPending(t, db, time.Now())

	next := time.Now().Add(20 * time.Second).UTC()
	require.NoError(t, repo.Reschedule(context.Background(), id, next, "boom"))

	var status, errMsg string
	var rc int
	var na time.Time
	require.NoError(t, db.QueryRow(
		`SELECT status, retry_count, error_message, next_attempt_at FROM executor_deployments WHERE id=$1`, id).
		Scan(&status, &rc, &errMsg, &na))
	assert.Equal(t, "pending", status)
	assert.Equal(t, 1, rc)
	assert.Equal(t, "boom", errMsg)
	assert.WithinDuration(t, next, na, time.Second)
}

func TestRepo_MarkFailed(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	repo := deployer.NewPostgresRepository(db, testLogger())
	id := newPending(t, db, time.Now())

	require.NoError(t, repo.MarkFailed(context.Background(), id, "exhausted"))

	var status, errMsg string
	require.NoError(t, db.QueryRow(`SELECT status, error_message FROM executor_deployments WHERE id=$1`, id).Scan(&status, &errMsg))
	assert.Equal(t, "failed", status)
	assert.Equal(t, "exhausted", errMsg)
}

func TestRepo_GetDueBatch_SkipLockedDisjoint(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	for i := 0; i < 10; i++ {
		newPending(t, db, time.Now().Add(-time.Minute))
	}

	const workers = 5
	seen := make([][]uuid.UUID, workers)

	// Each goroutine opens a transaction, issues its SELECT (locking rows), signals
	// it has selected, then waits for all others to select before committing.
	// This guarantees all transactions are concurrently open during the SELECT phase,
	// which is the condition FOR UPDATE SKIP LOCKED is designed for.
	var (
		selectedWg sync.WaitGroup // counts down once each goroutine has selected
		commitWg   sync.WaitGroup // counts down once each goroutine is ready to let main proceed
		startWg    sync.WaitGroup // used as a start barrier so all goroutines select together
	)
	startWg.Add(1) // released after all goroutines are ready

	type result struct {
		ids []uuid.UUID
		err error
	}
	results := make([]result, workers)

	selectedWg.Add(workers)
	commitWg.Add(workers)
	var txs []*sqlx.Tx

	var txMu sync.Mutex
	txs = make([]*sqlx.Tx, workers)
	for w := 0; w < workers; w++ {
		wCopy := w
		go func() {
			tx, err := db.BeginTxx(context.Background(), nil)
			if err != nil {
				results[wCopy].err = err
				selectedWg.Done()
				commitWg.Done()
				return
			}
			txMu.Lock()
			txs[wCopy] = tx
			txMu.Unlock()

			// Wait for all goroutines to reach this point before issuing SELECT.
			startWg.Wait()

			repo := deployer.NewPostgresRepository(tx, testLogger())
			rows, err := repo.GetDueBatch(context.Background(), 2)
			results[wCopy].err = err
			for _, r := range rows {
				results[wCopy].ids = append(results[wCopy].ids, r.ID)
			}
			// Signal that this goroutine has selected its rows.
			selectedWg.Done()
			// Wait for all goroutines to finish selecting before committing.
			selectedWg.Wait()
			_ = tx.Commit()
			commitWg.Done()
		}()
	}

	// Release all goroutines to SELECT simultaneously.
	startWg.Done()
	// Wait for all goroutines to commit.
	commitWg.Wait()

	for w, res := range results {
		require.NoError(t, res.err, "worker %d", w)
		seen[w] = res.ids
	}

	all := map[uuid.UUID]int{}
	for _, ids := range seen {
		for _, id := range ids {
			all[id]++
		}
	}
	for id, n := range all {
		assert.Equal(t, 1, n, "row %s claimed by exactly one worker", id)
	}
}
