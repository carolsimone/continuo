package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	grpcinfra "github.com/carolsimone/continuo/orchestrator/adapters/grpc"
	httpinfra "github.com/carolsimone/continuo/orchestrator/adapters/http"
	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	"github.com/carolsimone/continuo/orchestrator/adapters/postgres"
	"github.com/carolsimone/continuo/orchestrator/adapters/redis"
	"github.com/carolsimone/continuo/orchestrator/config"
	domainEvent "github.com/carolsimone/continuo/orchestrator/domain/event"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/internal/lifecycle"
	"github.com/carolsimone/continuo/orchestrator/internal/sweeper"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	"github.com/carolsimone/continuo/orchestrator/service/queries"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	"github.com/carolsimone/continuo/orchestrator/service/watchdog"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Setup structured logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	v := &pkgconfig.Validator{}
	cfg := config.Load(v)
	if missing := v.Missing(); len(missing) > 0 {
		logger.Error("missing required env vars", "vars", strings.Join(missing, ", "))
		os.Exit(1)
	}

	logger.Info("Starting orchestrator service")

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize lifecycle manager
	lifecycleManager := lifecycle.NewApplicationLifecycle(logger)
	lifecycleManager.SetupSignalHandlers(ctx, cancel)

	// ========================================================================
	// INITIALIZE INFRASTRUCTURE
	// ========================================================================

	// 1. Neo4j client
	neo4jClient, err := neo4jinfra.NewNeo4jClient(
		cfg.Neo4j.URI,
		cfg.Neo4j.User,
		cfg.Neo4j.Password,
		logger,
	)
	if err != nil {
		logger.Error("Failed to create Neo4j client", "error", err)
		os.Exit(1)
	}
	logger.Info("Neo4j client initialized")

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Closing Neo4j connection")
		return neo4jClient.Close(ctx)
	})

	// 2. PostgreSQL client (for outbox / message processing)
	pgDB, err := postgres.NewPostgresClient(
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.DB,
		cfg.Postgres.User,
		cfg.Postgres.Password,
		logger,
	)
	if err != nil {
		logger.Error("Failed to create PostgreSQL client", "error", err)
		os.Exit(1)
	}
	logger.Info("PostgreSQL client initialized")

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Closing PostgreSQL connection")
		return pgDB.Close()
	})

	// 3. Redis client
	redisClient := goredis.NewClient(&goredis.Options{
		Addr:     cfg.Redis.Addr(),
		Password: cfg.Redis.Password,
		DB:       0,
	})

	if err := redisClient.Ping(ctx).Err(); err != nil {
		logger.Error("Failed to connect to Redis", "error", err)
		os.Exit(1)
	}
	logger.Info("Redis connection established")

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Closing Redis connection")
		return redisClient.Close()
	})

	// ========================================================================
	// INITIALIZE REPOSITORIES
	// ========================================================================

	topologyRepo := neo4jinfra.NewTopologyRepository(neo4jClient, logger)
	runRepoWrite := neo4jinfra.NewRunRepository(neo4jClient, logger)
	queryRepo := neo4jinfra.NewOrchestratorQueryRepository(neo4jClient, logger)
	runRepo := neo4jinfra.NewCompositeRunRepository(runRepoWrite, queryRepo)
	outboxRepo := postgres.NewOutboxRepository(pgDB, logger)
	publishedRepo := postgres.NewPublishedMessagesRepository(pgDB, logger)
	cancelledSchedulesRepo := postgres.NewCancelledSchedulesRepository(pgDB)

	// ========================================================================
	// INITIALIZE UNIT OF WORK & COMMAND HANDLERS
	// ========================================================================

	// Each handler gets its own UnitOfWork instance. UnitOfWork holds a stateful
	// Postgres transaction (inTx flag + *sqlx.Tx). Sharing one instance across
	// consumers would cause "transaction already in progress" errors when two
	// consumers process messages concurrently — the second Begin() sees inTx=true
	// and the message is never ACKed, getting stuck in the PEL forever.
	topologyStateRepo := postgres.NewTopologyStateRepository(pgDB)
	rejectedTopologyRepo := postgres.NewRejectedTopologyRepository(pgDB)
	ingestTopologyHandler := handlers.NewIngestTopologyHandler(uow.NewPostgresUnitOfWork(pgDB, logger), topologyRepo, topologyStateRepo, rejectedTopologyRepo, logger)
	initializeRunHandler := handlers.NewInitializeRunHandler(uow.NewPostgresUnitOfWork(pgDB, logger), runRepo, logger)
	handleNodeCompletedHandler := handlers.NewHandleNodeCompletedHandler(uow.NewPostgresUnitOfWork(pgDB, logger), runRepo, cancelledSchedulesRepo, logger)
	handleSchedulerStartedHandler := handlers.NewHandleSchedulerStartedHandler(uow.NewPostgresUnitOfWork(pgDB, logger), runRepo, logger)
	handleRerunHandler := handlers.NewHandleRerunHandler(uow.NewPostgresUnitOfWork(pgDB, logger), runRepo, logger)
	handleRebaseHandler := handlers.NewHandleRebaseHandler(uow.NewPostgresUnitOfWork(pgDB, logger), runRepo, logger)
	handleSingleNodeRunHandler := handlers.NewHandleSingleNodeRunHandler(uow.NewPostgresUnitOfWork(pgDB, logger), runRepo, logger)

	// ========================================================================
	// INITIALIZE OUTBOX PROCESSOR
	// ========================================================================

	outboxProcessor := handlers.NewOutboxProcessor(outboxRepo, publishedRepo, redisClient, logger)

	go func() {
		if err := outboxProcessor.Run(ctx); err != nil {
			logger.Error("Outbox processor error", "error", err)
		}
	}()

	// ========================================================================
	// START HTTP HEALTH CHECK SERVER
	// ========================================================================

	healthServer := httpinfra.NewHealthServer(cfg.HTTPPort, logger)

	go func() {
		if err := healthServer.Start(); err != nil {
			logger.Error("Health server error", "error", err)
		}
	}()

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		return healthServer.Shutdown(ctx)
	})

	// ========================================================================
	// START RUN SWEEPER
	// ========================================================================

	runSweeper := sweeper.New(runRepoWrite, cfg.RunHistoryRetentionDays, cfg.RunSweeperIntervalMinutes, logger)
	go runSweeper.Start(ctx)

	// ========================================================================
	// START DISPATCH WATCHDOG
	// ========================================================================

	stateConn, err := grpc.NewClient(cfg.StateGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("Failed to dial state gRPC", "endpoint", cfg.StateGRPCAddr, "error", err)
		os.Exit(1)
	}
	defer stateConn.Close()
	stateGRPCClient := statev1.NewStateServiceClient(stateConn)

	watchdogInstance := watchdog.NewWatchdog(
		watchdog.Config{
			Enabled:       cfg.WatchdogEnabled,
			Interval:      time.Duration(cfg.WatchdogIntervalSecs) * time.Second,
			NoProgressFor: time.Duration(cfg.WatchdogNoProgressMins) * time.Minute,
		},
		stateGRPCClient,
		logger,
	)
	go func() {
		if err := watchdogInstance.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("Watchdog Run exited with error", "error", err)
		}
	}()

	// ========================================================================
	// INITIALIZE CANCELLED SCHEDULES CONSUMER + SWEEPER
	// ========================================================================

	scheduleCancelledHandler := redis.NewScheduleCancelledHandler(cancelledSchedulesRepo, logger)
	scheduleCancelledConsumer := redis.NewStreamConsumer(
		redisClient,
		cfg.ScheduleCancelledStream,
		cfg.ScheduleCancelledGroup,
		scheduleCancelledHandler,
		logger,
	)
	go func() {
		if err := scheduleCancelledConsumer.Start(ctx); err != nil {
			logger.Error("Schedule cancelled consumer error", "error", err)
		}
	}()

	go func() {
		ticker := time.NewTicker(time.Duration(cfg.CancelledSchedulesSweepIntervalMin) * time.Minute)
		defer ticker.Stop()
		ttl := time.Duration(cfg.CancelledSchedulesTTLHours) * time.Hour
		for {
			select {
			case <-ticker.C:
				if n, err := cancelledSchedulesRepo.DeleteExpired(ctx, ttl); err != nil {
					logger.Error("cancelled_schedules sweep failed", "error", err)
				} else if n > 0 {
					logger.Info("Swept expired cancelled_schedules rows", "count", n)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// ========================================================================
	// INITIALIZE REDIS CONSUMERS
	// ========================================================================

	// Consumer 1: node.updated:v1 -> HandleNodeCompleted
	nodeUpdatedHandler := func(ctx context.Context, msg goredis.XMessage) error {
		cmd := domainModel.NodeCompletedInput{
			TaskID:       uuid.MustParse(msg.Values["task_id"].(string)),
			ScheduleID:   uuid.MustParse(msg.Values["schedule_id"].(string)),
			ScheduleName: msg.Values["schedule_name"].(string),
			ServiceName:  msg.Values["service_name"].(string),
			SchemaName:   msg.Values["schema_name"].(string),
			TableName:    msg.Values["table_name"].(string),
			Status:       msg.Values["status"].(string),
		}
		return handleNodeCompletedHandler.Handle(ctx, cmd, msg.ID)
	}
	nodeUpdatedConsumer := redis.NewStreamConsumer(
		redisClient,
		cfg.NodeUpdatedStream,
		cfg.NodeUpdatedGroup,
		nodeUpdatedHandler,
		logger,
	)

	// Consumer 2: manifest.loaded:v1 -> IngestTopology
	manifestLoadedHandler := func(ctx context.Context, msg goredis.XMessage) error {
		payloadStr, ok := msg.Values["payload"].(string)
		if !ok {
			return fmt.Errorf("missing or invalid payload in manifest.loaded message %s", msg.ID)
		}
		var nodes []domainEvent.ManifestLoadedNode
		if err := json.Unmarshal([]byte(payloadStr), &nodes); err != nil {
			return fmt.Errorf("failed to unmarshal manifest.loaded payload: %w", err)
		}
		cmd := domainModel.IngestTopologyInput{Nodes: nodes}
		return ingestTopologyHandler.Handle(ctx, cmd, msg.ID)
	}
	manifestLoadedConsumer := redis.NewStreamConsumer(
		redisClient,
		cfg.ManifestLoadedStream,
		cfg.ManifestLoadedGroup,
		manifestLoadedHandler,
		logger,
	)

	// Consumer 3: initialize.run:v1 -> InitializeRun
	initRunHandler := func(ctx context.Context, msg goredis.XMessage) error {
		scheduleName, _ := msg.Values["schedule_name"].(string)
		runID, _ := msg.Values["run_id"].(string)
		cmd := domainModel.InitializeRunInput{
			ScheduleName: scheduleName,
			RunID:        runID,
		}
		// Check for rerun target fields
		if svc, ok := msg.Values["rerun_service_name"].(string); ok && svc != "" {
			cmd.RerunTarget = &domainModel.RerunTarget{
				ServiceName: svc,
				SchemaName:  msg.Values["rerun_schema_name"].(string),
				TableName:   msg.Values["rerun_table_name"].(string),
			}
		}
		return initializeRunHandler.Handle(ctx, cmd, msg.ID)
	}
	initRunConsumer := redis.NewStreamConsumer(
		redisClient,
		cfg.InitializeRunStream,
		cfg.InitializeRunGroup,
		initRunHandler,
		logger,
	)

	// Consumer 4: scheduler.started:v1 -> HandleSchedulerStarted
	schedulerStartedHandler := func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := redis.ParseSchedulerStartedEvent(msg.Values)
		if err != nil {
			return fmt.Errorf("scheduler.started message %s: %w", msg.ID, err)
		}
		return handleSchedulerStartedHandler.Handle(ctx, evt, msg.ID)
	}
	schedulerStartedConsumer := redis.NewStreamConsumer(
		redisClient,
		cfg.SchedulerStartedStream,
		cfg.SchedulerStartedGroup,
		schedulerStartedHandler,
		logger,
	)

	// Consumer 5: trigger.rerun:v1 -> HandleRerun
	rerunHandler := func(ctx context.Context, msg goredis.XMessage) error {
		scheduleID, _ := msg.Values["schedule_id"].(string)
		scheduleName, _ := msg.Values["schedule_name"].(string)
		sourceRunID, _ := msg.Values["source_run_id"].(string)
		schemaName, _ := msg.Values["schema_name"].(string)
		tableName, _ := msg.Values["table_name"].(string)
		serviceName, _ := msg.Values["service_name"].(string)
		if scheduleID == "" || scheduleName == "" || sourceRunID == "" ||
			schemaName == "" || tableName == "" || serviceName == "" {
			return fmt.Errorf("missing required fields in rerun message %s", msg.ID)
		}
		cmd := domainModel.RerunInput{
			RunID:        scheduleID,
			ScheduleName: scheduleName,
			SourceRunID:  sourceRunID,
			ServiceName:  serviceName,
			SchemaName:   schemaName,
			TableName:    tableName,
		}
		return handleRerunHandler.Handle(ctx, cmd, msg.ID)
	}
	rerunConsumer := redis.NewStreamConsumer(
		redisClient,
		cfg.RerunStream,
		cfg.RerunGroup,
		rerunHandler,
		logger,
	)

	// Consumer 8: trigger.rebase:v1 -> HandleRebase
	rebaseHandler := func(ctx context.Context, msg goredis.XMessage) error {
		scheduleID, _ := msg.Values["schedule_id"].(string)
		scheduleName, _ := msg.Values["schedule_name"].(string)
		sourceRunID, _ := msg.Values["source_run_id"].(string)
		if scheduleID == "" || scheduleName == "" || sourceRunID == "" {
			return fmt.Errorf("missing required fields in rebase message %s", msg.ID)
		}
		cmd := domainModel.RebaseInput{
			RunID:        scheduleID,
			ScheduleName: scheduleName,
			SourceRunID:  sourceRunID,
		}
		return handleRebaseHandler.Handle(ctx, cmd, msg.ID)
	}
	rebaseConsumer := redis.NewStreamConsumer(
		redisClient,
		cfg.RebaseStream,
		cfg.RebaseGroup,
		rebaseHandler,
		logger,
	)

	// Consumer 7: trigger.single_node_run:v1 -> HandleSingleNodeRun
	singleNodeRunHandler := func(ctx context.Context, msg goredis.XMessage) error {
		scheduleID, _ := msg.Values["schedule_id"].(string)
		scheduleName, _ := msg.Values["schedule_name"].(string)
		schemaName, _ := msg.Values["schema_name"].(string)
		tableName, _ := msg.Values["table_name"].(string)
		serviceName, _ := msg.Values["service_name"].(string)
		metadataSource, _ := msg.Values["metadata_source"].(string)
		sourceRunID, _ := msg.Values["source_run_id"].(string)

		if scheduleID == "" || scheduleName == "" || schemaName == "" || tableName == "" || serviceName == "" {
			return fmt.Errorf("missing required fields in single_node_run message %s", msg.ID)
		}
		switch metadataSource {
		case "latest":
			if sourceRunID != "" {
				return fmt.Errorf("source_run_id must be empty when metadata_source=latest in message %s", msg.ID)
			}
		case "snapshot_of_run":
			if sourceRunID == "" {
				return fmt.Errorf("source_run_id required when metadata_source=snapshot_of_run in message %s", msg.ID)
			}
		default:
			return fmt.Errorf("invalid metadata_source %q in message %s", metadataSource, msg.ID)
		}

		req := domainModel.SingleNodeRunInput{
			RunID:          scheduleID,
			ScheduleName:   scheduleName,
			ServiceName:    serviceName,
			SchemaName:     schemaName,
			TableName:      tableName,
			MetadataSource: metadataSource,
			SourceRunID:    sourceRunID,
		}
		return handleSingleNodeRunHandler.Handle(ctx, req, msg.ID)
	}
	singleNodeRunConsumer := redis.NewStreamConsumer(
		redisClient,
		cfg.SingleNodeRunStream,
		cfg.SingleNodeRunGroup,
		singleNodeRunHandler,
		logger,
	)

	// Consumer 6: run.finalized:v1 -> FinalizeRun
	runFinalizedHandler := redis.NewRunFinalizedHandler(runRepoWrite, logger)
	runFinalizedConsumer := redis.NewStreamConsumer(
		redisClient,
		cfg.RunFinalizedStream,
		cfg.RunFinalizedGroup,
		runFinalizedHandler,
		logger,
	)

	// Start all consumers in goroutines
	go func() {
		if err := nodeUpdatedConsumer.Start(ctx); err != nil {
			logger.Error("Node updated consumer error", "error", err)
		}
	}()

	go func() {
		if err := manifestLoadedConsumer.Start(ctx); err != nil {
			logger.Error("Manifest loaded consumer error", "error", err)
		}
	}()

	go func() {
		if err := initRunConsumer.Start(ctx); err != nil {
			logger.Error("Initialize run consumer error", "error", err)
		}
	}()

	go func() {
		if err := schedulerStartedConsumer.Start(ctx); err != nil {
			logger.Error("Scheduler started consumer error", "error", err)
		}
	}()

	go func() {
		if err := rerunConsumer.Start(ctx); err != nil {
			logger.Error("Rerun consumer error", "error", err)
		}
	}()

	go func() {
		if err := rebaseConsumer.Start(ctx); err != nil {
			logger.Error("Rebase consumer error", "error", err)
		}
	}()

	go func() {
		if err := singleNodeRunConsumer.Start(ctx); err != nil {
			logger.Error("Single-node-run consumer error", "error", err)
		}
	}()

	go func() {
		if err := runFinalizedConsumer.Start(ctx); err != nil {
			logger.Error("Run finalized consumer error", "error", err)
		}
	}()

	// ========================================================================
	// START gRPC SERVER (BLOCKING)
	// ========================================================================

	runQueries := queries.NewRunQueryService(runRepo, topologyStateRepo, logger)
	queryHandler := grpcinfra.NewQueryHandler(queryRepo, runQueries, logger)

	grpcServer, err := grpcinfra.NewServer(cfg.GRPCPort, queryHandler, logger)
	if err != nil {
		logger.Error("Failed to create gRPC server", "error", err)
		os.Exit(1)
	}

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		return grpcServer.Shutdown(ctx)
	})

	logger.Info("Orchestrator service initialized, starting gRPC server",
		"grpc_port", cfg.GRPCPort,
		"http_port", cfg.HTTPPort,
	)

	if err := grpcServer.Start(); err != nil {
		logger.Error("gRPC server error", "error", err)
		os.Exit(1)
	}

	logger.Info("Orchestrator service stopped")
}
