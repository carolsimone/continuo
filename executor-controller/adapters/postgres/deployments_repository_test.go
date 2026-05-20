//go:build integration

package postgres_test

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
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

func validCmd() command.DeployTask {
	return command.DeployTask{
		TaskID: uuid.New().String(), ScheduleID: uuid.New().String(),
		ScheduleName: "daily", ServiceName: "dbt", SchemaName: "public", TableName: "orders",
		JobName: "dbt-public-orders", NodeType: "dbt-model", ImageTag: "sha-abc",
		TaskRetryCount: 0, TaskMaxRetries: 2,
	}
}

// seedDue inserts a raw pending row (empty job_params) with a chosen due time.
func seedDue(t *testing.T, db *sqlx.DB, nextAttempt time.Time) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.Exec(
		`INSERT INTO executor_deployments (id, task_id, schedule_id, job_params, next_attempt_at)
		 VALUES ($1, $2, $3, '{}'::jsonb, $4)`,
		id, uuid.New(), uuid.New(), nextAttempt)
	require.NoError(t, err)
	return id
}

func TestRepo_Add_PersistsPendingAggregate(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	repo := postgres.NewDeploymentsRepository(db, testLogger())

	now := time.Now()
	cmd := validCmd()
	dep := model.NewDeployment(cmd, nil, now)
	require.NoError(t, repo.Add(context.Background(), dep))

	var (
		status     string
		maxRetries int
		taskID     uuid.UUID
		jobParams  []byte
		nextAt     time.Time
	)
	require.NoError(t, db.QueryRow(
		`SELECT status, max_retries, task_id, job_params, next_attempt_at FROM executor_deployments WHERE id=$1`, dep.ID(),
	).Scan(&status, &maxRetries, &taskID, &jobParams, &nextAt))
	assert.Equal(t, "pending", status)
	assert.Equal(t, 3, maxRetries)
	assert.Equal(t, cmd.TaskID, taskID.String(), "task_id column populated from the command identity")
	assert.Contains(t, string(jobParams), "dbt-public-orders", "command serialized into job_params")
	assert.False(t, nextAt.IsZero())
}

func TestRepo_GetDueBatch_OnlyDueRowsOldestFirst(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	past1 := seedDue(t, db, time.Now().Add(-2*time.Minute))
	past2 := seedDue(t, db, time.Now().Add(-1*time.Minute))
	_ = seedDue(t, db, time.Now().Add(10*time.Minute)) // future: excluded

	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	repo := postgres.NewDeploymentsRepository(tx, testLogger())

	deployments, err := repo.GetDueBatch(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, deployments, 2, "future-dated row excluded")
	assert.Equal(t, past1, deployments[0].ID(), "oldest next_attempt_at first")
	assert.Equal(t, past2, deployments[1].ID())
}

func TestRepo_Save_MarkDeployed(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	repo := postgres.NewDeploymentsRepository(db, testLogger())

	now := time.Now()
	dep := model.NewDeployment(validCmd(), nil, now)
	require.NoError(t, repo.Add(context.Background(), dep))

	require.NoError(t, dep.MarkDeployed(now))
	require.NoError(t, repo.Save(context.Background(), dep))

	var status string
	var deployedAt *time.Time
	require.NoError(t, db.QueryRow(`SELECT status, deployed_at FROM executor_deployments WHERE id=$1`, dep.ID()).Scan(&status, &deployedAt))
	assert.Equal(t, "deployed", status)
	assert.NotNil(t, deployedAt)
}

func TestRepo_Save_RescheduleAndFail(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	backoff := model.BackoffPolicy{Base: 20 * time.Second, Cap: 2 * time.Minute}

	now := time.Now()
	dep := model.NewDeployment(validCmd(), nil, now) // maxRetries 3
	require.NoError(t, repo.Add(context.Background(), dep))

	// Transient failure → reschedule (stays pending, retry_count 1).
	require.False(t, dep.RegisterFailure(now, false, "boom", backoff))
	require.NoError(t, repo.Save(context.Background(), dep))

	var status, errMsg string
	var rc int
	var nextAt time.Time
	require.NoError(t, db.QueryRow(
		`SELECT status, retry_count, error_message, next_attempt_at FROM executor_deployments WHERE id=$1`, dep.ID()).
		Scan(&status, &rc, &errMsg, &nextAt))
	assert.Equal(t, "pending", status)
	assert.Equal(t, 1, rc)
	assert.Equal(t, "boom", errMsg)
	assert.True(t, nextAt.After(now))

	// Permanent failure → terminal.
	require.True(t, dep.RegisterFailure(now, true, "fatal", backoff))
	require.NoError(t, repo.Save(context.Background(), dep))
	require.NoError(t, db.QueryRow(`SELECT status, error_message FROM executor_deployments WHERE id=$1`, dep.ID()).Scan(&status, &errMsg))
	assert.Equal(t, "failed", status)
	assert.Equal(t, "fatal", errMsg)
}

func TestRepo_GetDueBatch_CorruptJobParamsRecoversIdentity(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	taskID := uuid.New()
	scheduleID := uuid.New()
	// Valid JSONB but a JSON string — cannot unmarshal into DeployTask.
	_, err := db.Exec(
		`INSERT INTO executor_deployments (id, task_id, schedule_id, job_params, next_attempt_at)
		 VALUES ($1, $2, $3, '"corrupt"'::jsonb, NOW() - interval '1 minute')`,
		uuid.New(), taskID, scheduleID)
	require.NoError(t, err)

	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	repo := postgres.NewDeploymentsRepository(tx, testLogger())

	deployments, err := repo.GetDueBatch(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, deployments, 1)

	dep := deployments[0]
	assert.False(t, dep.IsDeployable(), "corrupt payload yields an undeployable aggregate")
	assert.Equal(t, taskID.String(), dep.Command().TaskID, "identity recovered from the task_id column")
	assert.Equal(t, scheduleID.String(), dep.Command().ScheduleID)
}

func TestRepo_GetDueBatch_SkipLockedDisjoint(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	for i := 0; i < 10; i++ {
		seedDue(t, db, time.Now().Add(-time.Minute))
	}

	const workers = 5
	seen := make([][]uuid.UUID, workers)

	var (
		selectedWg sync.WaitGroup
		commitWg   sync.WaitGroup
		startWg    sync.WaitGroup
	)
	startWg.Add(1)

	type result struct {
		ids []uuid.UUID
		err error
	}
	results := make([]result, workers)

	selectedWg.Add(workers)
	commitWg.Add(workers)
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
			startWg.Wait()
			repo := postgres.NewDeploymentsRepository(tx, testLogger())
			deployments, err := repo.GetDueBatch(context.Background(), 2)
			results[wCopy].err = err
			for _, d := range deployments {
				results[wCopy].ids = append(results[wCopy].ids, d.ID())
			}
			selectedWg.Done()
			selectedWg.Wait()
			_ = tx.Commit()
			commitWg.Done()
		}()
	}

	startWg.Done()
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
