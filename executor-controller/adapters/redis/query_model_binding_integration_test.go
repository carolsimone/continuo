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
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

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
		return postgres.NewPostgresUnitOfWork(db, logger)
	}
	handler := handlers.NewQueryModelHandler(logger)
	return executorredis.NewQueryModelBinding(uowFactory, handler, logger), logger
}

// queryModelXMessage builds a goredis.XMessage fixture for query.model:v1.
// node_type is "dbt-model" (hyphen) per the wire format used by the orchestrator.
// outbox_entry_id is set to a fresh UUID so the test exercises the
// application-level idempotency layer; pass a specific value for the
// publisher-retry regression test.
func queryModelXMessage(t *testing.T, msgID string, taskID, scheduleID uuid.UUID) goredis.XMessage {
	t.Helper()
	return queryModelXMessageWithOutboxID(t, msgID, taskID, scheduleID, uuid.New())
}

func queryModelXMessageWithOutboxID(
	t *testing.T, msgID string, taskID, scheduleID, outboxEntryID uuid.UUID,
) goredis.XMessage {
	t.Helper()
	return goredis.XMessage{ID: msgID, Values: map[string]interface{}{
		"outbox_entry_id": outboxEntryID.String(),
		"task_id":         taskID.String(),
		"schedule_id":     scheduleID.String(),
		"schedule_name":   "daily",
		"service_name":    "dbt",
		"schema_name":     "public",
		"table_name":      "orders",
		"job_name":        "dbt-public-orders",
		"node_type":       "dbt-model",
		"image_tag":       "sha-abc",
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

	assert.Equal(t, 1, countRows(t, db, `SELECT COUNT(*) FROM executor_deployments`))
	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM message_processing WHERE stream_name = $1`, streams.QueryModelV1))
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

	assert.Equal(t, 1, countRows(t, db, `SELECT COUNT(*) FROM executor_deployments`),
		"exactly one deployment row even with %d concurrent handlers", goroutines)
	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM message_processing WHERE message_id = $1 AND stream_name = $2`,
		msg.ID, streams.QueryModelV1))
}

func TestQueryModelBinding_PublisherRetrySameOutboxEntryIDDedups(t *testing.T) {
	// When the orchestrator's outbox processor sees an ambiguous XADD (succeeded
	// but error returned) and re-publishes, Redis assigns a new msg.ID to the
	// second delivery. The shared (msg.ID, stream_name) primary key would treat
	// the two deliveries as distinct, but the partial unique index on
	// message_processing.outbox_entry_id catches them as the same upstream event.
	// The second delivery is deduped at the message_processing layer: only one
	// executor_deployments row and one message_processing row are written.
	db, cleanup := setupPostgres(t)
	defer cleanup()

	binding, _ := buildBinding(db)
	outboxEntryID := uuid.New()
	taskID := uuid.New()
	scheduleID := uuid.New()

	// First delivery: msg.ID "10-0".
	msg1 := queryModelXMessageWithOutboxID(t, "10-0", taskID, scheduleID, outboxEntryID)
	require.NoError(t, binding(context.Background(), msg1))

	// Second delivery: same outbox_entry_id, different msg.ID. The
	// outbox_entry_id partial unique index on message_processing deduplicates
	// it — the handler is skipped and no second deployment row is written.
	msg2 := queryModelXMessageWithOutboxID(t, "11-0", taskID, scheduleID, outboxEntryID)
	require.NoError(t, binding(context.Background(), msg2))

	assert.Equal(t, 1, countRows(t, db, `SELECT COUNT(*) FROM executor_deployments`),
		"outbox_entry_id dedup prevents a second deployment row for the same upstream event")
	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM message_processing WHERE stream_name = $1`, streams.QueryModelV1),
		"outbox_entry_id dedup also prevents a second message_processing row")
}

func TestQueryModelBinding_NoOutboxEntryIDStillDedupsByMessageID(t *testing.T) {
	// When the inbound message has no outbox_entry_id, dedup falls back entirely
	// to (msg.ID, stream_name) in message_processing. A repeated delivery with
	// the same msg.ID produces only one deployment row.
	db, cleanup := setupPostgres(t)
	defer cleanup()

	binding, _ := buildBinding(db)
	msg := goredis.XMessage{ID: "12-0", Values: map[string]interface{}{
		"task_id":       uuid.New().String(),
		"schedule_id":   uuid.New().String(),
		"schedule_name": "daily",
		"service_name":  "dbt",
		"schema_name":   "public",
		"table_name":    "orders",
		"job_name":      "dbt-public-orders",
		"node_type":     "dbt-model",
		"image_tag":     "sha-abc",
	}}

	require.NoError(t, binding(context.Background(), msg))
	require.NoError(t, binding(context.Background(), msg))

	assert.Equal(t, 1, countRows(t, db, `SELECT COUNT(*) FROM executor_deployments`),
		"second delivery with same msg.ID is caught by shared dedup")
	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM message_processing WHERE message_id = $1`, msg.ID))
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

	assert.Equal(t, 0, countRows(t, db, `SELECT COUNT(*) FROM executor_deployments`),
		"no deployment row written when schedule is cancelled")
	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM message_processing WHERE message_id = $1`, msg.ID),
		"dedup row IS created so future redeliveries skip")
}
