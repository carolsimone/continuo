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
	graphv1 "github.com/carolsimone/continuo/graph/api/graph/v1"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Test configuration
const (
	testScheduleName = "hourly"
	redisStream      = "scheduler.started:v1"
	outputStream     = "query.model:v1"
	consumerGroup    = "test_consumers"
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
	ctx                 context.Context
	cancel              context.CancelFunc
	logger              *slog.Logger
	pgDB                *sqlx.DB
	redisClient         *goredis.Client
	stateClient         *grpcAdapter.StateClient
	graphServiceClient  graphv1.GraphServiceClient
	graphConn           *grpc.ClientConn
	graphAdapter        *grpcAdapter.GraphClient
	outboxRepo          postgres.OutboxRepository
	unitOfWork          uow.UnitOfWork
	messageBus          *messagebus.MessageBus
	producer            *redis.Producer
	outboxProc          *handlers.OutboxProcessor
	scheduleID          uuid.UUID
}

// setupTestFixture initializes all test dependencies
func setupTestFixture(t *testing.T, redisDB int) *testFixture {
	ctx, cancel := context.WithCancel(context.Background())

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Get connection hosts (use service names for docker-compose, allow override for host testing)
	pgHost := getEnvOrDefault("POSTGRES_HOST", "postgres")
	redisHost := getEnvOrDefault("REDIS_HOST", "redis")
	stateHost := getEnvOrDefault("STATE_HOST", "state")

	// Connect to PostgreSQL
	pgDatabase := getEnvOrDefault("POSTGRES_DB", "continuo_startup")
	pgConnStr := fmt.Sprintf("host=%s port=5432 dbname=%s user=continuo_svc password=runner sslmode=disable", pgHost, pgDatabase)
	pgDB, err := sqlx.Connect("postgres", pgConnStr)
	require.NoError(t, err, "Failed to connect to PostgreSQL")

	// Connect to Redis with isolated DB per test
	redisClient := goredis.NewClient(&goredis.Options{
		Addr: fmt.Sprintf("%s:6379", redisHost),
		DB:   redisDB, // Each test uses its own Redis DB for isolation
	})
	require.NoError(t, redisClient.Ping(ctx).Err(), "Failed to connect to Redis")

	// Connect to state service
	stateAddr := fmt.Sprintf("%s:50051", stateHost)
	stateClient, err := grpcAdapter.NewStateClient(stateAddr, logger)
	require.NoError(t, err, "Failed to connect to state service")

	// Connect to graph service
	graphAddr := fmt.Sprintf("%s:50052", getEnvOrDefault("GRAPH_HOST", "graph"))
	graphConn, err := grpc.NewClient(graphAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "Failed to connect to graph service")
	graphServiceClient := graphv1.NewGraphServiceClient(graphConn)

	// Create graph client adapter for the handler
	graphClientAdapter, err := grpcAdapter.NewGraphClient(graphAddr, logger)
	require.NoError(t, err, "Failed to create graph client adapter")

	// Initialize repositories
	outboxRepo := postgres.NewOutboxRepository(pgDB, logger)

	// Initialize Unit of Work
	unitOfWork := uow.NewPostgresUnitOfWork(pgDB, logger)

	// Create schedule ID for this test
	scheduleID := uuid.New()

	// Create handler
	initHandler := handlers.NewInitializeSchedulerHandler(
		unitOfWork,
		graphClientAdapter,
		stateClient,
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

	// Flush Redis streams at setup to ensure clean state regardless of prior runs
	redisClient.Del(ctx, redisStream)
	redisClient.Del(ctx, outputStream)

	// Create Redis producer
	producer := redis.NewProducer(redisClient, outputStream, logger)

	// Create outbox processor
	outboxProc := handlers.NewOutboxProcessor(outboxRepo, producer, logger)

	return &testFixture{
		ctx:                ctx,
		cancel:             cancel,
		logger:             logger,
		pgDB:               pgDB,
		redisClient:        redisClient,
		stateClient:        stateClient,
		graphServiceClient: graphServiceClient,
		graphConn:          graphConn,
		graphAdapter:       graphClientAdapter,
		outboxRepo:         outboxRepo,
		unitOfWork:         unitOfWork,
		messageBus:         messageBus,
		producer:           producer,
		outboxProc:         outboxProc,
		scheduleID:         scheduleID,
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
	f.graphConn.Close()
	f.graphAdapter.Close()
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

	// Delete consumer group if it exists
	f.redisClient.XGroupDestroy(f.ctx, redisStream, consumerGroup)
}

// setupGraphTestData creates test nodes in the graph service via gRPC
func setupGraphTestData(t *testing.T, f *testFixture) []string {
	// Create 3 root nodes (no upstream dependencies)
	rootNodes := []struct {
		serviceName string
		schema      string
		tableName   string
		nodeType    string
	}{
		{"analytics", "public", "users", "dbt-model"},
		{"analytics", "public", "orders", "dbt-model"},
		{"analytics", "public", "products", "dbt-model"},
	}

	// Create 2 downstream nodes (with dependencies)
	downstreamNodes := []struct {
		serviceName string
		schema      string
		tableName   string
		dependsOn   string
		nodeType    string
	}{
		{"analytics", "public", "user_stats", "users", "dbt-model"},
		{"analytics", "public", "order_stats", "orders", "dbt-model"},
	}

	// Create root nodes
	for _, node := range rootNodes {
		_, err := f.graphServiceClient.CreateNode(f.ctx, &graphv1.CreateNodeRequest{
			TableName:    node.tableName,
			SchemaName:   node.schema,
			ServiceName:  node.serviceName,
			ScheduleName: testScheduleName,
			NodeType:     node.nodeType,
			Criticality:  graphv1.Criticality_CRITICALITY_CORE,
			Owner:        "test_owner",
		})
		require.NoError(t, err, "Failed to create root node: %s", node.tableName)
	}

	// Create downstream nodes with dependencies
	for _, node := range downstreamNodes {
		_, err := f.graphServiceClient.CreateNode(f.ctx, &graphv1.CreateNodeRequest{
			TableName:    node.tableName,
			SchemaName:   node.schema,
			ServiceName:  node.serviceName,
			ScheduleName: testScheduleName,
			NodeType:     node.nodeType,
			Criticality:  graphv1.Criticality_CRITICALITY_CORE,
			Owner:        "test_owner",
			UpstreamDependencies: []*graphv1.UpstreamDependency{
				{
					TableName:   node.dependsOn,
					SchemaName:  node.schema,
					ServiceName: node.serviceName,
				},
			},
		})
		require.NoError(t, err, "Failed to create downstream node: %s", node.tableName)
	}

	// Return only root node table names
	var rootNodeNames []string
	for _, node := range rootNodes {
		rootNodeNames = append(rootNodeNames, node.tableName)
	}

	t.Logf("Created %d root nodes and %d downstream nodes in graph service", len(rootNodes), len(downstreamNodes))

	return rootNodeNames
}

// setupSchedulerInStateService creates a scheduler entry in state service
func setupSchedulerInStateService(t *testing.T, f *testFixture) {
	err := f.stateClient.CreateScheduler(f.ctx, f.scheduleID, testScheduleName)
	require.NoError(t, err, "Failed to create scheduler in state service")
	t.Logf("Scheduler %s created for schedule_name %s", f.scheduleID, testScheduleName)
}

// TestE2E_StartupController_ConsumeFromStream tests the main flow
func TestE2E_StartupController_ConsumeFromStream(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	f := setupTestFixture(t, 1) // Use Redis DB 1 for test isolation
	defer teardownTestFixture(t, f)

	// Setup: Add test data to graph service
	expectedRootNodes := setupGraphTestData(t, f)

	// Setup: Create scheduler in state service
	setupSchedulerInStateService(t, f)

	// Step 1: Publish scheduler.started event to Redis
	t.Log("Publishing scheduler.started event to Redis")
	event := map[string]interface{}{
		"runner_id":     f.scheduleID.String(),
		"schedule_name": testScheduleName,
	}

	_, err := f.redisClient.XAdd(f.ctx, &goredis.XAddArgs{
		Stream: redisStream,
		Values: event,
	}).Result()
	require.NoError(t, err, "Failed to publish event to Redis")

	// Step 2: Manually trigger the handler (simulating consumer processing)
	t.Log("Processing scheduler.started event")
	cmd := command.InitializeScheduler{
		RunnerID:     f.scheduleID,
		ScheduleName: testScheduleName,
	}

	err = f.messageBus.Handle(f.ctx, cmd)
	require.NoError(t, err, "Failed to handle InitializeScheduler command")

	// Step 3: Verify outbox entries were created
	t.Log("Verifying outbox entries")
	time.Sleep(100 * time.Millisecond) // Give it a moment

	var outboxEntries []struct {
		ID      string `db:"id"`
		Payload []byte `db:"payload"`
		Status  string `db:"status"`
	}

	err = f.pgDB.Select(&outboxEntries, "SELECT id, payload, status FROM startup_outbox WHERE aggregate_id = $1 ORDER BY created_at", f.scheduleID)
	require.NoError(t, err, "Failed to query outbox")

	assert.Equal(t, len(expectedRootNodes), len(outboxEntries), "Expected %d outbox entries", len(expectedRootNodes))

	// Verify all entries are pending
	for _, entry := range outboxEntries {
		assert.Equal(t, "pending", entry.Status, "Outbox entry should be pending")

		// Verify payload contains correct data
		var payload struct {
			ScheduleID   string `json:"schedule_id"`
			ScheduleName string `json:"schedule_name"`
			TableName    string `json:"table_name"`
			TaskID       string `json:"task_id"`
			JobName      string `json:"job_name"`
		}
		err = json.Unmarshal(entry.Payload, &payload)
		require.NoError(t, err, "Failed to unmarshal payload")

		assert.Equal(t, f.scheduleID.String(), payload.ScheduleID)
		assert.Equal(t, testScheduleName, payload.ScheduleName)
		assert.Contains(t, expectedRootNodes, payload.TableName, "Table name should be one of the root nodes")
		assert.NotEmpty(t, payload.TaskID, "TaskID should not be empty")
		assert.NotEmpty(t, payload.JobName, "JobName should not be empty")
		assert.Regexp(t, `^[a-z0-9-]+$`, payload.JobName, "JobName should be k8s compliant")
	}

	// Step 4: Run outbox processor to publish to Redis
	t.Log("Running outbox processor")
	err = f.outboxProc.ProcessBatch(f.ctx)
	require.NoError(t, err, "Failed to process outbox batch")

	// Step 5: Verify events were published to output stream
	t.Log("Verifying events in output stream")
	time.Sleep(100 * time.Millisecond)

	messages, err := f.redisClient.XRange(f.ctx, outputStream, "-", "+").Result()
	require.NoError(t, err, "Failed to read from output stream")

	assert.Equal(t, len(expectedRootNodes), len(messages), "Expected %d messages in output stream", len(expectedRootNodes))

	// Verify message content
	for _, msg := range messages {
		assert.Equal(t, f.scheduleID.String(), msg.Values["schedule_id"])
		assert.Equal(t, testScheduleName, msg.Values["schedule_name"])
		assert.Contains(t, expectedRootNodes, msg.Values["table_name"], "Table name should be one of the root nodes")
		assert.NotEmpty(t, msg.Values["task_id"], "TaskID should be present in Redis message")
		assert.NotEmpty(t, msg.Values["job_name"], "JobName should be present in Redis message")
		assert.NotEmpty(t, msg.Values["outbox_entry_id"], "outbox_entry_id should be present in Redis message")
	}

	// Step 6: Verify all outbox entries marked as processed
	t.Log("Verifying outbox entries marked as processed")
	err = f.pgDB.Select(&outboxEntries, "SELECT id, status FROM startup_outbox WHERE aggregate_id = $1", f.scheduleID)
	require.NoError(t, err, "Failed to query outbox")

	for _, entry := range outboxEntries {
		assert.Equal(t, "processed", entry.Status, "Outbox entry should be marked as processed")
	}

	t.Log("✅ Test passed: startup-controller successfully consumed from stream and published events")
}

// TestE2E_OutboxPattern_CrashRecovery tests outbox pattern with interruption
func TestE2E_OutboxPattern_CrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	f := setupTestFixture(t, 2) // Use Redis DB 2 for test isolation
	defer teardownTestFixture(t, f)

	// Setup: Add test data to graph service
	expectedRootNodes := setupGraphTestData(t, f)

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

	// Step 2: Verify outbox entries exist and are pending
	var pendingCount int
	err = f.pgDB.Get(&pendingCount, "SELECT COUNT(*) FROM startup_outbox WHERE aggregate_id = $1 AND status = 'pending'", f.scheduleID)
	require.NoError(t, err, "Failed to count pending outbox entries")

	assert.Equal(t, len(expectedRootNodes), pendingCount, "Expected %d pending outbox entries", len(expectedRootNodes))

	t.Logf("✅ Created %d pending outbox entries", pendingCount)

	// Step 3: Process ONLY HALF of the outbox entries (simulate partial success before crash)
	t.Log("Processing half of outbox entries (simulating partial processing before crash)")

	var outboxIDs []string
	err = f.pgDB.Select(&outboxIDs, "SELECT id FROM startup_outbox WHERE aggregate_id = $1 AND status = 'pending' ORDER BY created_at LIMIT $2", f.scheduleID, len(expectedRootNodes)/2)
	require.NoError(t, err, "Failed to query outbox IDs")

	// Manually process half of the entries
	for _, outboxID := range outboxIDs {
		outboxUUID, _ := uuid.Parse(outboxID)

		// Get the entry
		var entry struct {
			Payload []byte `db:"payload"`
		}
		err = f.pgDB.Get(&entry, "SELECT payload FROM startup_outbox WHERE id = $1", outboxUUID)
		require.NoError(t, err)

		// Unmarshal and publish
		var payload map[string]interface{}
		json.Unmarshal(entry.Payload, &payload)

		_, err = f.producer.Publish(f.ctx, payload)
		require.NoError(t, err, "Failed to publish to Redis")

		// Mark as processed
		err = f.outboxRepo.MarkProcessed(f.ctx, outboxUUID)
		require.NoError(t, err, "Failed to mark as processed")
	}

	t.Logf("✅ Processed %d entries, simulating crash before completing rest", len(outboxIDs))

	// Step 4: Verify partial state (some processed, some pending)
	var processedCount, stillPendingCount int
	err = f.pgDB.Get(&processedCount, "SELECT COUNT(*) FROM startup_outbox WHERE aggregate_id = $1 AND status = 'processed'", f.scheduleID)
	require.NoError(t, err)

	err = f.pgDB.Get(&stillPendingCount, "SELECT COUNT(*) FROM startup_outbox WHERE aggregate_id = $1 AND status = 'pending'", f.scheduleID)
	require.NoError(t, err)

	assert.Equal(t, len(outboxIDs), processedCount, "Expected %d processed entries", len(outboxIDs))
	assert.Equal(t, len(expectedRootNodes)-len(outboxIDs), stillPendingCount, "Expected %d pending entries", len(expectedRootNodes)-len(outboxIDs))

	t.Logf("✅ Before recovery: %d processed, %d pending", processedCount, stillPendingCount)

	// Step 5: SIMULATE SERVICE RESTART - run outbox processor again
	t.Log("Simulating service restart - running outbox processor for recovery")

	// Create a new outbox processor (simulating fresh instance after restart)
	newOutboxProc := handlers.NewOutboxProcessor(f.outboxRepo, f.producer, f.logger)

	err = newOutboxProc.ProcessBatch(f.ctx)
	require.NoError(t, err, "Failed to process outbox batch after recovery")

	// Step 6: Verify ALL entries are now processed
	t.Log("Verifying all outbox entries are processed after recovery")
	time.Sleep(100 * time.Millisecond)

	err = f.pgDB.Get(&processedCount, "SELECT COUNT(*) FROM startup_outbox WHERE aggregate_id = $1 AND status = 'processed'", f.scheduleID)
	require.NoError(t, err)

	err = f.pgDB.Get(&stillPendingCount, "SELECT COUNT(*) FROM startup_outbox WHERE aggregate_id = $1 AND status = 'pending'", f.scheduleID)
	require.NoError(t, err)

	assert.Equal(t, len(expectedRootNodes), processedCount, "Expected all %d entries to be processed", len(expectedRootNodes))
	assert.Equal(t, 0, stillPendingCount, "Expected no pending entries")

	t.Logf("✅ After recovery: %d processed, %d pending", processedCount, stillPendingCount)

	// Step 7: Verify ALL events are in Redis output stream
	t.Log("Verifying all events in output stream after recovery")

	messages, err := f.redisClient.XRange(f.ctx, outputStream, "-", "+").Result()
	require.NoError(t, err, "Failed to read from output stream")

	assert.Equal(t, len(expectedRootNodes), len(messages), "Expected all %d messages in output stream", len(expectedRootNodes))

	// Verify no duplicate messages
	seenTables := make(map[string]bool)
	for _, msg := range messages {
		tableName := msg.Values["table_name"].(string)
		assert.False(t, seenTables[tableName], "Duplicate message found for table: %s", tableName)
		seenTables[tableName] = true
	}

	t.Log("✅ Test passed: outbox pattern successfully recovered from crash and published all missing messages")
}
