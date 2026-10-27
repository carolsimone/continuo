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

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	statepublisher "github.com/carolsimone/continuo/state/adapters/publisher"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/internal/scheduler"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	ports "github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/uow"
)

// dispatchEvent holds the fields of a task.dispatch:v1 dispatch read back from
// the Redis stream.
type dispatchEvent struct {
	RunID        string
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

// TestSchedulerActivation_E2E tests the full scheduler activation flow using the
// transactional outbox pattern: StartRunHandler writes the run, its
// tasks and the dispatch of each ready task atomically to runs,
// tasks and state_outbox, then OutboxProcessor publishes the dispatches
// to Redis.
func TestSchedulerActivation_E2E(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Setup PostgreSQL. setupPostgres applies the full Flyway migration set
	// from db/migration/state/V*.sql, including message_processing and
	// state_outbox, so no additional DDL is needed here.
	t.Log("Setting up PostgreSQL...")
	db, cleanupDB := setupPostgres(t)
	defer cleanupDB()

	// Setup Redis
	t.Log("Setting up Redis...")
	redisClient, cleanupRedis := setupRedis(t)
	defer cleanupRedis()

	// Initialize repositories
	schedulerRepo := postgres.NewRunTableRepository(db, logger)
	taskRepo := postgres.NewTaskRepository(db, pkgevents.DefaultTenantID, logger)
	catalogRepo := postgres.NewScheduleCatalogRepository(db, logger)

	// Seed schedule_catalog rows so the activation policy can verify existence.
	// removed_at IS NULL means active. The e2e test uses "daily" and "hourly".
	_, err := db.Exec(`
		INSERT INTO schedule_catalog (schedule_name, first_seen_at, last_seen_at, removed_at, service_metadata)
		VALUES ('daily',  NOW(), NOW(), NULL, '{}'),
		       ('hourly', NOW(), NOW(), NULL, '{}')
		ON CONFLICT (schedule_name) DO NOTHING
	`)
	require.NoError(t, err, "Failed to seed schedule_catalog")
	promoteSchedules(t, db, "daily", "hourly")

	// Wire the UoW factory. Each cron tick gets a fresh UoW.
	clk := ports.SystemClock{}
	uowFactory := func() uow.UnitOfWork {
		return postgres.NewPostgresUnitOfWork(db, schedulerRepo, taskRepo, catalogRepo, clk, logger)
	}

	// Initialize activation handler
	activateHandler := svchandlers.NewStartRunHandler(defaultPlanner, logger)

	// Start outbox processor in background — picks up pending outbox entries and
	// publishes them to Redis.
	redisPub := statepublisher.NewOutboxPublisher(redisClient, logger)
	outboxProcessor := pkgoutbox.NewProcessor(
		db,
		postgres.OutboxTable,
		redisPub,
		nil,
		logger,
		pkgoutbox.ProcessorConfig{Tick: 500 * time.Millisecond, BatchSize: 10},
	)
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
	cronScheduler, err := scheduler.NewCronSchedulerWithConfig(activateHandler, uowFactory, logger, cfg, false)
	require.NoError(t, err, "Failed to create cron scheduler")

	// Start cron scheduler
	err = cronScheduler.Start()
	require.NoError(t, err, "Failed to start cron scheduler")
	defer cronScheduler.Stop(ctx)

	t.Log("Cron scheduler started with 30-second intervals")

	// Wait for first activation (up to 35 seconds)
	t.Log("Waiting for first activation (max 35 seconds)...")
	time.Sleep(35 * time.Second)

	// Test 1: Verify run record exists
	t.Log("Verifying run records...")
	var runRows []postgres.RunRow
	err = db.Select(&runRows, `
		SELECT run_id, schedule_name, status, created_at
		FROM runs
		ORDER BY created_at DESC
	`)
	require.NoError(t, err, "Failed to query runs")
	require.NotEmpty(t, runRows, "Expected at least one run record")

	// Should have 2 records (daily and hourly)
	assert.GreaterOrEqual(t, len(runRows), 2, "Expected at least 2 scheduler records (daily and hourly)")

	dailyRun := findRunByName(runRows, "daily")
	hourlyRun := findRunByName(runRows, "hourly")

	require.NotNil(t, dailyRun, "Expected to find daily schedule runRow")
	require.NotNil(t, hourlyRun, "Expected to find hourly schedule runRow")

	assert.Equal(t, run.RunStatusRunning, dailyRun.Status)
	assert.Equal(t, run.RunStatusRunning, hourlyRun.Status)

	t.Logf("Found daily runRow: %s (status: %s)", dailyRun.ScheduleID, dailyRun.Status)
	t.Logf("Found hourly runRow: %s (status: %s)", hourlyRun.ScheduleID, hourlyRun.Status)

	// Test 2: Verify Redis stream events exist (published by OutboxProcessor)
	t.Log("Verifying Redis stream events...")

	// Allow outbox processor a moment to publish any remaining entries
	time.Sleep(2 * time.Second)

	streamName := streams.TaskDispatchV1

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
	var events []dispatchEvent
	for _, stream := range messages {
		for _, msg := range stream.Messages {
			fields := make(map[string]string, len(msg.Values))
			for k, v := range msg.Values {
				fields[k], _ = v.(string)
			}
			_, dispatch, err := pkgevents.DecodeTaskDispatch(fields)
			require.NoError(t, err, "Redis message %v is not a task.dispatch:v1 entry", msg.Values)
			event := dispatchEvent{
				RunID:        dispatch.RunID,
				ScheduleName: dispatch.ScheduleName,
			}
			events = append(events, event)
			t.Logf("Read Redis event: run_id=%s, schedule_name=%s", event.RunID, event.ScheduleName)
		}
	}

	// Verify at least one event published
	require.NotEmpty(t, events, "No events found in Redis stream")
	assert.GreaterOrEqual(t, len(events), 2, "Expected at least 2 Redis events")

	// Verify runner_ids match the run ids
	dailyEvent := findEventByName(events, "daily")
	hourlyEvent := findEventByName(events, "hourly")

	require.NotNil(t, dailyEvent, "Expected to find daily event in Redis")
	require.NotNil(t, hourlyEvent, "Expected to find hourly event in Redis")

	assert.Equal(t, dailyRun.ScheduleID.String(), dailyEvent.RunID,
		"Daily runRow run_id should match the dispatch's run_id")
	assert.Equal(t, hourlyRun.ScheduleID.String(), hourlyEvent.RunID,
		"Hourly runRow run_id should match the dispatch's run_id")

	t.Log("E2E test passed: run records match Redis events")

	// Test 3: Verify duplicate run prevention
	t.Log("Testing duplicate run prevention...")

	// Wait for another 30 seconds - should NOT create new records because status is RUNNING
	time.Sleep(32 * time.Second)

	var runRowsAfter []postgres.RunRow
	err = db.Select(&runRowsAfter, `
		SELECT run_id, schedule_name, status, created_at
		FROM runs
		ORDER BY created_at DESC
	`)
	require.NoError(t, err, "Failed to query runs after second activation")

	// Should still have only 2 records (blocked by RUNNING status)
	assert.Equal(t, len(runRows), len(runRowsAfter),
		"Should not create new records when previous run is RUNNING")

	t.Log("Duplicate run prevention working correctly")
}

// findRunByName finds a scheduler runRow by schedule_name
func findRunByName(runRows []postgres.RunRow, name string) *postgres.RunRow {
	for _, runRow := range runRows {
		if runRow.ScheduleName == name {
			return &runRow
		}
	}
	return nil
}

// findEventByName finds a Redis event by schedule_name
func findEventByName(events []dispatchEvent, name string) *dispatchEvent {
	for _, event := range events {
		if event.ScheduleName == name {
			return &event
		}
	}
	return nil
}
