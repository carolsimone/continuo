//go:build integration

package redis_test

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	executorredis "github.com/carolsimone/continuo/executor-controller/adapters/redis"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	executortest "github.com/carolsimone/continuo/executor-controller/test"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const testStreamQuery = "query.model:v1"

// setupPostgres spins a Postgres testcontainer, applies executor's Flyway
// migrations, and returns a connected *sqlx.DB plus a cleanup func.
func setupPostgres(t *testing.T) (*sqlx.DB, func()) {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "testuser",
			"POSTGRES_PASSWORD": "testpass",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "failed to start postgres container")

	host, err := container.Host(ctx)
	require.NoError(t, err, "failed to get container host")

	port, err := container.MappedPort(ctx, "5432")
	require.NoError(t, err, "failed to get container port")

	// Force IPv4 for macOS/colima compatibility.
	if host == "localhost" {
		host = "127.0.0.1"
	}

	connStr := "host=" + host + " port=" + port.Port() + " user=testuser password=testpass dbname=testdb sslmode=disable"

	var db *sqlx.DB
	for i := 0; i < 10; i++ {
		db, err = sqlx.Connect("postgres", connStr)
		if err == nil {
			break
		}
		t.Logf("connection attempt %d/10 failed, retrying...", i+1)
		time.Sleep(500 * time.Millisecond)
	}
	require.NoError(t, err, "failed to connect to postgres after retries")

	require.NoError(t, executortest.ApplyMigrations(db.DB), "failed to apply executor migrations")

	cleanup := func() {
		_ = db.Close()
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}
	return db, cleanup
}

// buildBinding constructs a QueryModelBinding backed by the given *sqlx.DB
// and returns it together with the logger used internally.
func buildBinding(db *sqlx.DB) (func(ctx context.Context, msg goredis.XMessage) error, *slog.Logger) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	uowFactory := func() uow.UnitOfWork {
		return uow.NewPostgresUnitOfWork(db, logger)
	}
	handler := handlers.NewQueryModelHandler(logger)
	return executorredis.NewQueryModelBinding(uowFactory, handler, logger), logger
}

// queryModelXMessage builds a goredis.XMessage fixture for query.model:v1.
// node_type is "dbt-model" (hyphen) per the wire format used by the orchestrator.
func queryModelXMessage(t *testing.T, msgID string, taskID, scheduleID uuid.UUID) goredis.XMessage {
	t.Helper()
	return goredis.XMessage{ID: msgID, Values: map[string]interface{}{
		"task_id":       taskID.String(),
		"schedule_id":   scheduleID.String(),
		"schedule_name": "daily",
		"service_name":  "dbt",
		"schema_name":   "public",
		"table_name":    "orders",
		"job_name":      "dbt-public-orders",
		"node_type":     "dbt-model",
		"image_tag":     "sha-abc",
	}}
}

// countRows executes a COUNT(*) query and returns the result.
func countRows(t *testing.T, db *sqlx.DB, query string, args ...interface{}) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(context.Background(), query, args...).Scan(&n))
	return n
}

func TestQueryModelBinding_SingleMessageHappyPath(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	binding, _ := buildBinding(db)
	msg := queryModelXMessage(t, "1-0", uuid.New(), uuid.New())

	require.NoError(t, binding(context.Background(), msg))

	assert.Equal(t, 1, countRows(t, db, `SELECT COUNT(*) FROM deployment_outbox`))
	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM message_processing WHERE stream_name = $1`, testStreamQuery))
}

func TestQueryModelBinding_ConcurrentDedup(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	binding, _ := buildBinding(db)
	msg := queryModelXMessage(t, "2-0", uuid.New(), uuid.New())

	const goroutines = 10
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- binding(context.Background(), msg)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	assert.Equal(t, 1, countRows(t, db, `SELECT COUNT(*) FROM deployment_outbox`),
		"exactly one outbox row even with %d concurrent handlers", goroutines)
	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM message_processing WHERE message_id = $1 AND stream_name = $2`,
		msg.ID, testStreamQuery))
}

func TestQueryModelBinding_CancelledScheduleDropsMessage(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	scheduleID := uuid.New()
	_, err := db.ExecContext(context.Background(),
		`INSERT INTO cancelled_schedules (schedule_id) VALUES ($1)`, scheduleID)
	require.NoError(t, err)

	binding, _ := buildBinding(db)
	msg := queryModelXMessage(t, "3-0", uuid.New(), scheduleID)

	require.NoError(t, binding(context.Background(), msg))

	assert.Equal(t, 0, countRows(t, db, `SELECT COUNT(*) FROM deployment_outbox`),
		"no outbox row written when schedule is cancelled")
	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM message_processing WHERE message_id = $1`, msg.ID),
		"dedup row IS created so future redeliveries skip")
}
