package test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	grpcAdapter "github.com/carolsimone/continuo/startup-controller/adapters/grpc"
	"github.com/carolsimone/continuo/startup-controller/adapters/postgres"
	"github.com/carolsimone/continuo/startup-controller/adapters/redis"
	"github.com/carolsimone/continuo/startup-controller/domain/command"
	"github.com/carolsimone/continuo/startup-controller/service/handlers"
	"github.com/carolsimone/continuo/startup-controller/service/messagebus"
	"github.com/carolsimone/continuo/startup-controller/service/uow"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test configuration
const (
	testScheduleName    = "hourly"
	redisStream         = "scheduler.started:v1"
	outputStream        = "query.model:v1"
	initializeRunStream = "initialize.run:v1"
	consumerGroup       = "test_consumers"
)

// getEnvOrDefault returns the value of an environment variable or a default value
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// testFixture holds all test dependencies
type testFixture struct {
	ctx         context.Context
	cancel      context.CancelFunc
	logger      *slog.Logger
	pgDB        *sqlx.DB
	redisClient *goredis.Client
	stateClient *grpcAdapter.StateClient
	outboxRepo  postgres.OutboxRepository
	unitOfWork  uow.UnitOfWork
	messageBus  *messagebus.MessageBus
	producers   map[string]*redis.Producer
	outboxProc  *handlers.OutboxProcessor
	scheduleID  uuid.UUID
}

// setupTestFixture initializes all test dependencies
func setupTestFixture(t *testing.T, redisDB int) *testFixture {
	ctx, cancel := context.WithCancel(context.Background())

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Get connection hosts
	pgHost := getEnvOrDefault("POSTGRES_HOST", "postgres")
	redisHost := getEnvOrDefault("REDIS_HOST", "redis")
	stateHost := getEnvOrDefault("STATE_HOST", "state")

	// Connect to PostgreSQL
	pgDatabase := getEnvOrDefault("POSTGRES_DB", "continuo_startup")
	pgPassword := getEnvOrDefault("POSTGRES_PASSWORD", "continuo")
	pgConnStr := fmt.Sprintf("host=%s port=5432 dbname=%s user=continuo_svc password=%s sslmode=disable", pgHost, pgDatabase, pgPassword)
	pgDB, err := sqlx.Connect("postgres", pgConnStr)
	require.NoError(t, err, "Failed to connect to PostgreSQL")

	// Connect to Redis with isolated DB per test
	redisPassword := getEnvOrDefault("REDIS_PASSWORD", "")
	redisClient := goredis.NewClient(&goredis.Options{
		Addr:     fmt.Sprintf("%s:6379", redisHost),
		Password: redisPassword,
		DB:       redisDB,
	})
	require.NoError(t, redisClient.Ping(ctx).Err(), "Failed to connect to Redis")

	// Connect to state service
	stateAddr := fmt.Sprintf("%s:50051", stateHost)
	stateClient, err := grpcAdapter.NewStateClient(stateAddr, logger)
	require.NoError(t, err, "Failed to connect to state service")

	// Initialize repositories
	outboxRepo := postgres.NewOutboxRepository(pgDB, logger)

	// Initialize Unit of Work
	unitOfWork := uow.NewPostgresUnitOfWork(pgDB, logger)

	// Create schedule ID for this test
	scheduleID := uuid.New()

	// Create handler (now emits events instead of calling graph gRPC)
	initHandler := handlers.NewInitializeSchedulerHandler(
		unitOfWork,
		stateClient,
		initializeRunStream,
		logger,
	)

	// Create command handlers map
	commandHandlers := map[string]messagebus.CommandHandler{
		"command.InitializeScheduler": func(ctx context.Context, cmd command.Command) error {
			return initHandler.Handle(ctx, cmd.(command.InitializeScheduler))
		},
	}

	// Create MessageBus
	messageBus := messagebus.NewMessageBus(
		unitOfWork,
		commandHandlers,
		map[string][]messagebus.EventHandler{},
		logger,
	)

	// Flush Redis streams at setup
	redisClient.Del(ctx, redisStream)
	redisClient.Del(ctx, outputStream)
	redisClient.Del(ctx, initializeRunStream)

	// Create Redis producers keyed by stream name
	queryModelProducer := redis.NewProducer(redisClient, outputStream, logger)
	initRunProducer := redis.NewProducer(redisClient, initializeRunStream, logger)
	producers := map[string]*redis.Producer{
		outputStream:        queryModelProducer,
		initializeRunStream: initRunProducer,
	}

	// Create outbox processor
	outboxProc := handlers.NewOutboxProcessor(outboxRepo, producers, logger)

	return &testFixture{
		ctx:         ctx,
		cancel:      cancel,
		logger:      logger,
		pgDB:        pgDB,
		redisClient: redisClient,
		stateClient: stateClient,
		outboxRepo:  outboxRepo,
		unitOfWork:  unitOfWork,
		messageBus:  messageBus,
		producers:   producers,
		outboxProc:  outboxProc,
		scheduleID:  scheduleID,
	}
}

// teardownTestFixture cleans up test dependencies
func teardownTestFixture(t *testing.T, f *testFixture) {
	f.cancel()

	// Clean up PostgreSQL test data
	cleanupPostgres(t, f)

	// Clean up Redis test data
	cleanupRedis(t, f)

	// Close connections
	f.pgDB.Close()
	f.redisClient.Close()
	f.stateClient.Close()
}

// cleanupPostgres removes all test data from PostgreSQL
func cleanupPostgres(t *testing.T, f *testFixture) {
	_, err := f.pgDB.Exec("DELETE FROM startup_outbox WHERE aggregate_id = $1", f.scheduleID)
	if err != nil {
		t.Logf("Warning: Failed to cleanup startup_outbox: %v", err)
	}
}

// cleanupRedis removes all test data from Redis
func cleanupRedis(t *testing.T, f *testFixture) {
	f.redisClient.Del(f.ctx, redisStream)
	f.redisClient.Del(f.ctx, outputStream)
	f.redisClient.Del(f.ctx, initializeRunStream)

	// Delete consumer group if it exists
	f.redisClient.XGroupDestroy(f.ctx, redisStream, consumerGroup)
}

// setupSchedulerInStateService creates a scheduler entry in state service
func setupSchedulerInStateService(t *testing.T, f *testFixture) {
	err := f.stateClient.CreateScheduler(f.ctx, f.scheduleID, testScheduleName)
	require.NoError(t, err, "Failed to create scheduler in state service")
	t.Logf("Scheduler %s created for schedule_name %s", f.scheduleID, testScheduleName)
}

// TestE2E_StartupController_EmitsInitializeRunEvent tests the event-driven flow.
// InitializeScheduler now emits an initialize.run:v1 event instead of calling graph gRPC.
func TestE2E_StartupController_EmitsInitializeRunEvent(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	f := setupTestFixture(t, 1)
	defer teardownTestFixture(t, f)

	// Setup: Create scheduler in state service
	setupSchedulerInStateService(t, f)

	// Step 1: Process the InitializeScheduler command
	t.Log("Processing scheduler.started event")
	cmd := command.InitializeScheduler{
		RunnerID:     f.scheduleID,
		ScheduleName: testScheduleName,
	}

	err := f.messageBus.Handle(f.ctx, cmd)
	require.NoError(t, err, "Failed to handle InitializeScheduler command")

	// Step 2: Verify outbox entry was created with initialize.run:v1 target
	t.Log("Verifying outbox entry")
	time.Sleep(100 * time.Millisecond)

	var outboxEntries []struct {
		ID         string `db:"id"`
		Payload    []byte `db:"payload"`
		Status     string `db:"status"`
		StreamName string `db:"stream_name"`
		EventType  string `db:"event_type"`
	}

	err = f.pgDB.Select(&outboxEntries, "SELECT id, payload, status, stream_name, event_type FROM startup_outbox WHERE aggregate_id = $1 ORDER BY created_at", f.scheduleID)
	require.NoError(t, err, "Failed to query outbox")

	require.Len(t, outboxEntries, 1, "Expected exactly 1 outbox entry")
	entry := outboxEntries[0]
	assert.Equal(t, "pending", entry.Status)
	assert.Equal(t, initializeRunStream, entry.StreamName)
	assert.Equal(t, "run_initialization_requested", entry.EventType)

	// Verify payload contents
	var payload map[string]string
	err = json.Unmarshal(entry.Payload, &payload)
	require.NoError(t, err, "Failed to unmarshal payload")
	assert.Equal(t, f.scheduleID.String(), payload["run_id"])
	assert.Equal(t, testScheduleName, payload["schedule_name"])

	// Step 3: Run outbox processor to publish to Redis
	t.Log("Running outbox processor")
	err = f.outboxProc.ProcessBatch(f.ctx)
	require.NoError(t, err, "Failed to process outbox batch")

	// Step 4: Verify event was published to initialize.run:v1 stream
	t.Log("Verifying event in initialize.run:v1 stream")
	time.Sleep(100 * time.Millisecond)

	messages, err := f.redisClient.XRange(f.ctx, initializeRunStream, "-", "+").Result()
	require.NoError(t, err, "Failed to read from initialize.run:v1 stream")

	require.Len(t, messages, 1, "Expected 1 message in initialize.run:v1 stream")
	msg := messages[0]
	assert.NotEmpty(t, msg.Values["outbox_entry_id"])
	assert.Equal(t, "run_initialization_requested", msg.Values["event_type"])

	// Verify payload is valid JSON
	payloadStr, ok := msg.Values["payload"].(string)
	require.True(t, ok, "payload should be a string")
	var parsedPayload map[string]string
	require.NoError(t, json.Unmarshal([]byte(payloadStr), &parsedPayload))
	assert.Equal(t, f.scheduleID.String(), parsedPayload["run_id"])
	assert.Equal(t, testScheduleName, parsedPayload["schedule_name"])

	t.Log("Test passed: startup-controller emits initialize.run:v1 event via outbox")
}

// TestE2E_OutboxPattern_CrashRecovery tests outbox pattern with interruption
func TestE2E_OutboxPattern_CrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	f := setupTestFixture(t, 2)
	defer teardownTestFixture(t, f)

	// Setup: Create scheduler in state service
	setupSchedulerInStateService(t, f)

	// Step 1: Process the command to populate outbox
	t.Log("Processing scheduler.started event to populate outbox")
	cmd := command.InitializeScheduler{
		RunnerID:     f.scheduleID,
		ScheduleName: testScheduleName,
	}

	err := f.messageBus.Handle(f.ctx, cmd)
	require.NoError(t, err, "Failed to handle InitializeScheduler command")

	// Step 2: Verify outbox entry exists and is pending
	var pendingCount int
	err = f.pgDB.Get(&pendingCount, "SELECT COUNT(*) FROM startup_outbox WHERE aggregate_id = $1 AND status = 'pending'", f.scheduleID)
	require.NoError(t, err, "Failed to count pending outbox entries")
	assert.Equal(t, 1, pendingCount, "Expected 1 pending outbox entry")

	// Step 3: Simulate crash before outbox processing completes
	t.Log("Simulating crash - creating fresh outbox processor")
	newOutboxProc := handlers.NewOutboxProcessor(f.outboxRepo, f.producers, f.logger)

	err = newOutboxProc.ProcessBatch(f.ctx)
	require.NoError(t, err, "Failed to process outbox batch after recovery")

	// Step 4: Verify all entries are now processed
	var processedCount int
	err = f.pgDB.Get(&processedCount, "SELECT COUNT(*) FROM startup_outbox WHERE aggregate_id = $1 AND status = 'processed'", f.scheduleID)
	require.NoError(t, err)
	assert.Equal(t, 1, processedCount, "Expected 1 processed entry")

	var stillPendingCount int
	err = f.pgDB.Get(&stillPendingCount, "SELECT COUNT(*) FROM startup_outbox WHERE aggregate_id = $1 AND status = 'pending'", f.scheduleID)
	require.NoError(t, err)
	assert.Equal(t, 0, stillPendingCount, "Expected no pending entries")

	// Step 5: Verify event in Redis stream
	messages, err := f.redisClient.XRange(f.ctx, initializeRunStream, "-", "+").Result()
	require.NoError(t, err, "Failed to read from initialize.run:v1 stream")
	assert.Len(t, messages, 1, "Expected 1 message in initialize.run:v1 stream after recovery")

	t.Log("Test passed: outbox pattern recovered from crash and published missing message")
}
