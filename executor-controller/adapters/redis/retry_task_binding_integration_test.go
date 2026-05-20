//go:build integration

package redis_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"testing"

	executorredis "github.com/carolsimone/continuo/executor-controller/adapters/redis"
	"github.com/carolsimone/continuo/executor-controller/service/deployer"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// retryTaskXMessage builds a goredis.XMessage fixture for retry.task:v1.
// node_type is "dbt-model" (hyphen) per the wire format used by the orchestrator.
// task_retry_count is "2" and max_retries is "5".
func retryTaskXMessage(t *testing.T, msgID string, taskID, scheduleID uuid.UUID) goredis.XMessage {
	t.Helper()
	return goredis.XMessage{ID: msgID, Values: map[string]interface{}{
		"task_id":          taskID.String(),
		"schedule_id":      scheduleID.String(),
		"schedule_name":    "daily",
		"service_name":     "dbt",
		"schema_name":      "public",
		"table_name":       "orders",
		"job_name":         "dbt-public-orders",
		"node_type":        "dbt-model",
		"image_tag":        "sha-abc",
		"task_retry_count": "2",
		"max_retries":      "5",
	}}
}

// buildRetryBinding constructs a RetryTaskBinding backed by the given *sqlx.DB
// and returns it together with the logger used internally.
func buildRetryBinding(db *sqlx.DB) (func(ctx context.Context, msg goredis.XMessage) error, *slog.Logger) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	uowFactory := func() uow.UnitOfWork {
		return uow.NewPostgresUnitOfWork(db, logger)
	}
	handler := handlers.NewRetryTaskHandler(logger)
	return executorredis.NewRetryTaskBinding(uowFactory, handler, logger), logger
}

// readDeployJobParams scans the JSONB job_params from the single executor_deployments row
// and unmarshals it into a DeployJob for field assertions.
func readDeployJobParams(t *testing.T, db *sqlx.DB) deployer.DeployJob {
	t.Helper()
	var raw []byte
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT job_params FROM executor_deployments LIMIT 1`).Scan(&raw))
	var d deployer.DeployJob
	require.NoError(t, json.Unmarshal(raw, &d))
	return d
}

func TestRetryTaskBinding_SingleMessageHappyPath(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	binding, _ := buildRetryBinding(db)
	msg := retryTaskXMessage(t, "1-0", uuid.New(), uuid.New())

	require.NoError(t, binding(context.Background(), msg))

	assert.Equal(t, 1, countRows(t, db, `SELECT COUNT(*) FROM executor_deployments`))
	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM message_processing WHERE stream_name = $1`, streams.RetryTaskV1))

	// Verify the retry fields are stored faithfully inside the JSONB job_params.
	d := readDeployJobParams(t, db)
	assert.Equal(t, 2, d.TaskRetryCount)
	assert.Equal(t, 5, d.TaskMaxRetries)
}

func TestRetryTaskBinding_ConcurrentDedup(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	binding, _ := buildRetryBinding(db)
	msg := retryTaskXMessage(t, "2-0", uuid.New(), uuid.New())

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
		msg.ID, streams.RetryTaskV1))
}

func TestRetryTaskBinding_CrossStreamIsolation(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	// Use the same message ID on both streams to confirm dedup is scoped by
	// stream_name — this is the V21-fix regression guard.
	sharedMsgID := "42-0"
	taskID := uuid.New()
	scheduleID := uuid.New()

	queryBinding, _ := buildBinding(db)
	retryBinding, _ := buildRetryBinding(db)

	queryMsg := queryModelXMessage(t, sharedMsgID, taskID, scheduleID)
	retryMsg := retryTaskXMessage(t, sharedMsgID, taskID, scheduleID)

	require.NoError(t, queryBinding(context.Background(), queryMsg))
	require.NoError(t, retryBinding(context.Background(), retryMsg))

	// Both streams must produce their own independent deployment row.
	assert.Equal(t, 2, countRows(t, db, `SELECT COUNT(*) FROM executor_deployments`),
		"each stream produces its own deployment row; no false-dedup across streams")

	// Each stream must produce exactly one dedup row keyed to its own stream_name.
	assert.Equal(t, 2, countRows(t, db,
		`SELECT COUNT(*) FROM message_processing WHERE message_id = $1`, sharedMsgID),
		"two message_processing rows: one per stream")
	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM message_processing WHERE message_id = $1 AND stream_name = $2`,
		sharedMsgID, streams.QueryModelV1))
	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM message_processing WHERE message_id = $1 AND stream_name = $2`,
		sharedMsgID, streams.RetryTaskV1))
}
