package test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/carolsimone/continuo/state/internal/scheduler"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
)

// schedulerStartedEvent holds the fields read back from the Redis stream.
type schedulerStartedEvent struct {
	RunnerID     string
	ScheduleName string
}

// setupRedis starts a Redis testcontainer
func setupRedis(t *testing.T) (*goredis.Client, func()) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "redis:7.2-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}

	redisContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "Failed to start Redis container")

	host, err := redisContainer.Host(ctx)
	require.NoError(t, err)

	port, err := redisContainer.MappedPort(ctx, "6379")
	require.NoError(t, err)

	addr := fmt.Sprintf("%s:%s", host, port.Port())
	t.Logf("Redis available at: %s", addr)

	client := goredis.NewClient(&goredis.Options{Addr: addr})

	// Test connection with retries
	var pingErr error
	for i := 0; i < 10; i++ {
		pingErr = client.Ping(ctx).Err()
		if pingErr == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.NoError(t, pingErr, "Failed to connect to Redis")

	cleanup := func() {
		client.Close()
		if err := redisContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate Redis container: %v", err)
		}
	}

	return client, cleanup
}

// outboxSchema returns the DDL for the state_outbox table used in e2e tests.
const outboxSchema = `
CREATE TABLE IF NOT EXISTS state_outbox (
	id             UUID PRIMARY KEY,
	aggregate_type VARCHAR(100) NOT NULL,
	aggregate_id   UUID NOT NULL,
	event_type     VARCHAR(100) NOT NULL,
	payload        JSONB NOT NULL,
	stream_name    VARCHAR(200) NOT NULL,
	status         VARCHAR(20) NOT NULL DEFAULT 'pending',
	max_retries    INTEGER NOT NULL DEFAULT 3,
	retry_count    INTEGER NOT NULL DEFAULT 0,
	created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`

// TestSchedulerActivation_E2E tests the full scheduler activation flow using the
// transactional outbox pattern: ScheduleActivationService writes atomically to
// scheduler_tracker + state_outbox, then OutboxProcessor publishes to Redis.
func TestSchedulerActivation_E2E(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Setup PostgreSQL
	t.Log("Setting up PostgreSQL...")
	db, cleanupDB := setupPostgres(t)
	defer cleanupDB()

	// Create the outbox table (setupPostgres only creates scheduler/task tables)
	_, err := db.Exec(outboxSchema)
	require.NoError(t, err, "Failed to create state_outbox table")

	// Setup Redis
	t.Log("Setting up Redis...")
	redisClient, cleanupRedis := setupRedis(t)
	defer cleanupRedis()

	// Initialize repositories
	repo := postgres.NewSchedulerTrackerRepository(db, logger)
	outboxRepo := postgres.NewOutboxRepository(db, logger)

	// Initialize activator and activation service (transactional outbox)
	streamName := "scheduler.started:v1"
	activator := scheduler.NewScheduleActivator(repo, logger)
	catalogRepo := postgres.NewScheduleCatalogRepository(db, logger)
	activationService := scheduler.NewScheduleActivationService(
		db,
		activator,
		catalogRepo,
		repo,
		outboxRepo,
		streamName,
		logger,
	)

	// Start outbox processor in background — picks up pending outbox entries and
	// publishes them to Redis
	outboxProcessor := svchandlers.NewOutboxProcessor(outboxRepo, redisClient, logger)
	go func() {
		if err := outboxProcessor.Run(ctx); err != nil {
			logger.Error("Outbox processor error", "error", err)
		}
	}()

	// Initialize cron scheduler with 30-second intervals for testing
	cfg := &scheduler.SchedulesConfig{
		Timezone: "UTC",
		Schedules: []scheduler.ScheduleEntry{
			{Name: "daily", Cron: "*/30 * * * * *"},
			{Name: "hourly", Cron: "*/30 * * * * *"},
		},
		WithSeconds: true,
	}
	cronScheduler, err := scheduler.NewCronSchedulerWithConfig(activationService, logger, cfg)
	require.NoError(t, err, "Failed to create cron scheduler")

	// Start cron scheduler
	err = cronScheduler.Start()
	require.NoError(t, err, "Failed to start cron scheduler")
	defer cronScheduler.Stop(ctx)

	t.Log("Cron scheduler started with 30-second intervals")

	// Wait for first activation (up to 35 seconds)
	t.Log("Waiting for first activation (max 35 seconds)...")
	time.Sleep(35 * time.Second)

	// Test 1: Verify scheduler_tracker record exists
	t.Log("Verifying scheduler_tracker records...")
	var trackers []model.SchedulerTracker
	err = db.Select(&trackers, `
		SELECT schedule_id, schedule_name, status, created_at
		FROM scheduler_tracker
		ORDER BY created_at DESC
	`)
	require.NoError(t, err, "Failed to query scheduler_tracker")
	require.NotEmpty(t, trackers, "Expected at least one scheduler_tracker record")

	// Should have 2 records (daily and hourly)
	assert.GreaterOrEqual(t, len(trackers), 2, "Expected at least 2 scheduler records (daily and hourly)")

	dailyTracker := findTrackerByName(trackers, "daily")
	hourlyTracker := findTrackerByName(trackers, "hourly")

	require.NotNil(t, dailyTracker, "Expected to find daily schedule tracker")
	require.NotNil(t, hourlyTracker, "Expected to find hourly schedule tracker")

	assert.Equal(t, model.SchedulerStatusPending, dailyTracker.Status)
	assert.Equal(t, model.SchedulerStatusPending, hourlyTracker.Status)

	t.Logf("Found daily tracker: %s (status: %s)", dailyTracker.ScheduleID, dailyTracker.Status)
	t.Logf("Found hourly tracker: %s (status: %s)", hourlyTracker.ScheduleID, hourlyTracker.Status)

	// Test 2: Verify Redis stream events exist (published by OutboxProcessor)
	t.Log("Verifying Redis stream events...")

	// Allow outbox processor a moment to publish any remaining entries
	time.Sleep(2 * time.Second)

	// Read all messages from stream
	readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readCancel()

	messages, err := redisClient.XRead(readCtx, &goredis.XReadArgs{
		Streams: []string{streamName, "0"},
		Count:   10,
		Block:   1 * time.Second,
	}).Result()
	require.NoError(t, err, "Failed to read from Redis stream")

	// Collect events
	var events []schedulerStartedEvent
	for _, stream := range messages {
		for _, msg := range stream.Messages {
			runnerID, ok := msg.Values["runner_id"].(string)
			require.True(t, ok, "runner_id field missing or not a string in Redis message %v", msg.Values)
			scheduleName, ok := msg.Values["schedule_name"].(string)
			require.True(t, ok, "schedule_name field missing or not a string in Redis message %v", msg.Values)
			event := schedulerStartedEvent{
				RunnerID:     runnerID,
				ScheduleName: scheduleName,
			}
			events = append(events, event)
			t.Logf("Read Redis event: runner_id=%s, schedule_name=%s", event.RunnerID, event.ScheduleName)
		}
	}

	// Verify at least one event published
	require.NotEmpty(t, events, "No events found in Redis stream")
	assert.GreaterOrEqual(t, len(events), 2, "Expected at least 2 Redis events")

	// Verify runner_ids match scheduler_tracker schedule_ids
	dailyEvent := findEventByName(events, "daily")
	hourlyEvent := findEventByName(events, "hourly")

	require.NotNil(t, dailyEvent, "Expected to find daily event in Redis")
	require.NotNil(t, hourlyEvent, "Expected to find hourly event in Redis")

	assert.Equal(t, dailyTracker.ScheduleID.String(), dailyEvent.RunnerID,
		"Daily tracker schedule_id should match Redis event runner_id")
	assert.Equal(t, hourlyTracker.ScheduleID.String(), hourlyEvent.RunnerID,
		"Hourly tracker schedule_id should match Redis event runner_id")

	t.Log("E2E test passed: scheduler_tracker records match Redis events")

	// Test 3: Verify duplicate run prevention
	t.Log("Testing duplicate run prevention...")

	// Wait for another 30 seconds - should NOT create new records because status is PENDING
	time.Sleep(32 * time.Second)

	var trackersAfter []model.SchedulerTracker
	err = db.Select(&trackersAfter, `
		SELECT schedule_id, schedule_name, status, created_at
		FROM scheduler_tracker
		ORDER BY created_at DESC
	`)
	require.NoError(t, err, "Failed to query scheduler_tracker after second activation")

	// Should still have only 2 records (blocked by PENDING status)
	assert.Equal(t, len(trackers), len(trackersAfter),
		"Should not create new records when previous run is PENDING")

	t.Log("Duplicate run prevention working correctly")
}

// findTrackerByName finds a scheduler tracker by schedule_name
func findTrackerByName(trackers []model.SchedulerTracker, name string) *model.SchedulerTracker {
	for _, tracker := range trackers {
		if tracker.ScheduleName == name {
			return &tracker
		}
	}
	return nil
}

// findEventByName finds a Redis event by schedule_name
func findEventByName(events []schedulerStartedEvent, name string) *schedulerStartedEvent {
	for _, event := range events {
		if event.ScheduleName == name {
			return &event
		}
	}
	return nil
}
