package main

import (
	"context"
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
	orchpublisher "github.com/carolsimone/continuo/orchestrator/adapters/publisher"
	"github.com/carolsimone/continuo/orchestrator/adapters/redis"
	"github.com/carolsimone/continuo/orchestrator/config"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/internal/lifecycle"
	"github.com/carolsimone/continuo/orchestrator/internal/reconciler"
	"github.com/carolsimone/continuo/orchestrator/internal/sweeper"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	"github.com/carolsimone/continuo/orchestrator/service/queries"
	snapshotsvc "github.com/carolsimone/continuo/orchestrator/service/snapshotsvc"
	"github.com/carolsimone/continuo/orchestrator/service/watchdog"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
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
	queryRepo := neo4jinfra.NewOrchestratorQueryRepository(neo4jClient, logger)
	runAggRepo := neo4jinfra.NewRunAggregateRepository(neo4jClient, logger)
	snapshotTxRunner := neo4jinfra.NewSnapshotTxRunner(neo4jClient)
	cancelledSchedulesRepo := postgres.NewCancelledSchedulesRepository(pgDB)
	snapshotService := snapshotsvc.NewService(snapshotTxRunner, cancelledSchedulesRepo, logger)

	// ========================================================================
	// INITIALIZE UNIT OF WORK & COMMAND HANDLERS
	// ========================================================================

	// Each handler gets its own UnitOfWork instance. UnitOfWork holds a stateful
	// Postgres transaction (inTx flag + *sqlx.Tx). Sharing one instance across
	// consumers would cause "transaction already in progress" errors when two
	// consumers process messages concurrently — the second Begin() sees inTx=true
	// and the message is never ACKed, getting stuck in the PEL forever.
	topologyStateRepo := postgres.NewTopologyStateRepository(pgDB)
	initializeRunHandler := handlers.NewInitializeRunHandler(postgres.NewPostgresUnitOfWork(pgDB, logger), snapshotService, logger)
	handleNodeCompletedHandler := handlers.NewHandleNodeCompletedHandler(postgres.NewPostgresUnitOfWork(pgDB, logger), runAggRepo, cancelledSchedulesRepo, logger)
	handleSchedulerStartedHandler := handlers.NewHandleSchedulerStartedHandler(postgres.NewPostgresUnitOfWork(pgDB, logger), queryRepo, snapshotService, logger)
	handleRerunHandler := handlers.NewHandleRerunHandler(postgres.NewPostgresUnitOfWork(pgDB, logger), snapshotService, logger)
	handleRebaseHandler := handlers.NewHandleRebaseHandler(postgres.NewPostgresUnitOfWork(pgDB, logger), snapshotService, logger)
	handleSingleNodeRunHandler := handlers.NewHandleSingleNodeRunHandler(postgres.NewPostgresUnitOfWork(pgDB, logger), snapshotService, logger)

	// ========================================================================
	// INITIALIZE OUTBOX PROCESSOR
	// ========================================================================

	outboxPub := orchpublisher.NewOutboxPublisher(redisClient, logger)
	outboxProc := pkgoutbox.NewProcessor(
		pgDB,
		"orchestrator_outbox",
		outboxPub,
		nil, // no terminal-failure hook for orchestrator
		logger,
		pkgoutbox.ProcessorConfig{Tick: time.Second, BatchSize: 100},
	)
	go func() {
		if err := outboxProc.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("Outbox processor exited", "error", err)
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

	runSweeper := sweeper.New(runAggRepo, cfg.RunHistoryRetentionDays, cfg.RunSweeperIntervalMinutes, logger)
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
	// START RECONCILER — converge active :Run projections to state's status
	// ========================================================================

	runStatusReader := grpcinfra.NewRunStatusReader(stateGRPCClient)
	reconcilerInstance := reconciler.New(queryRepo, runStatusReader, runAggRepo, cfg.ReconcilerIntervalSecs, logger)
	go reconcilerInstance.Start(ctx)

	// ========================================================================
	// INITIALIZE CANCELLED SCHEDULES CONSUMER + SWEEPER
	// ========================================================================

	scheduleCancelledHandler := handlers.NewScheduleCancelledHandler(cancelledSchedulesRepo, logger)
	scheduleCancelledBinding := redis.NewScheduleCancelledBinding(scheduleCancelledHandler, logger)
	scheduleCancelledConsumer := pkgredis.NewStreamConsumer(
		redisClient,
		streams.ScheduleCancelledV1,
		streams.OrchestratorScheduleCancelled,
		scheduleCancelledBinding,
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
		return handleNodeCompletedHandler.Handle(ctx, cmd, msg.ID, messageprocessing.ExtractOutboxEntryID(msg.Values))
	}
	nodeUpdatedConsumer := pkgredis.NewStreamConsumer(
		redisClient,
		streams.NodeUpdatedV1,
		streams.OrchestratorNodeUpdated,
		nodeUpdatedHandler,
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
		return initializeRunHandler.Handle(ctx, cmd, msg.ID, messageprocessing.ExtractOutboxEntryID(msg.Values))
	}
	initRunConsumer := pkgredis.NewStreamConsumer(
		redisClient,
		streams.InitializeRunV1,
		streams.OrchestratorInitializeRun,
		initRunHandler,
		logger,
	)

	// Consumer 4: scheduler.started:v1 -> HandleSchedulerStarted
	schedulerStartedHandler := func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := redis.ParseSchedulerStartedEvent(msg.Values)
		if err != nil {
			return fmt.Errorf("scheduler.started message %s: %w", msg.ID, err)
		}
		return handleSchedulerStartedHandler.Handle(ctx, evt, msg.ID, messageprocessing.ExtractOutboxEntryID(msg.Values))
	}
	schedulerStartedConsumer := pkgredis.NewStreamConsumer(
		redisClient,
		streams.SchedulerStartedV1,
		streams.OrchestratorSchedulerStarted,
		schedulerStartedHandler,
		logger,
	)

	// Consumer 5: trigger.rerun:v1 -> HandleRerun
	rerunHandler := func(ctx context.Context, msg goredis.XMessage) error {
		scheduleID, _ := msg.Values["schedule_id"].(string)
		scheduleName, _ := msg.Values["schedule_name"].(string)
		sourceRunID, _ := msg.Values["source_run_id"].(string)
		if scheduleID == "" || scheduleName == "" || sourceRunID == "" {
			return fmt.Errorf("missing required fields in rerun message %s", msg.ID)
		}
		cmd := domainModel.RerunInput{
			RunID:        scheduleID,
			ScheduleName: scheduleName,
			SourceRunID:  sourceRunID,
		}
		return handleRerunHandler.Handle(ctx, cmd, msg.ID, messageprocessing.ExtractOutboxEntryID(msg.Values))
	}
	rerunConsumer := pkgredis.NewStreamConsumer(
		redisClient,
		streams.TriggerRerunV1,
		streams.OrchestratorRerun,
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
		return handleRebaseHandler.Handle(ctx, cmd, msg.ID, messageprocessing.ExtractOutboxEntryID(msg.Values))
	}
	rebaseConsumer := pkgredis.NewStreamConsumer(
		redisClient,
		streams.TriggerRebaseV1,
		streams.OrchestratorRebase,
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
		return handleSingleNodeRunHandler.Handle(ctx, req, msg.ID, messageprocessing.ExtractOutboxEntryID(msg.Values))
	}
	singleNodeRunConsumer := pkgredis.NewStreamConsumer(
		redisClient,
		streams.TriggerSingleNodeRunV1,
		streams.OrchestratorSingleNodeRun,
		singleNodeRunHandler,
		logger,
	)

	// Consumer: run.finalized:v1 — projects state's terminal scheduler outcome
	// onto Neo4j :Run.completed_at / terminal_status. Covers edge cases where
	// the aggregate's internal finalization path is not exercised (e.g.
	// full-inherited rebases that produce no node.updated:v1 traffic).
	runFinalizedHandler := handlers.NewRunFinalizedHandler(runAggRepo, logger)
	runFinalizedBinding := redis.NewRunFinalizedBinding(runFinalizedHandler, logger)
	runFinalizedConsumer := pkgredis.NewStreamConsumer(
		redisClient,
		streams.RunFinalizedV1,
		streams.OrchestratorRunFinalized,
		runFinalizedBinding,
		logger,
	)

	// Consumer: release.promoted:v1 — atomically replaces the Neo4j topology
	// when release-controller promotes a candidate release to production, then
	// emits schedules.loaded:v1 so state can refresh its schedule projections.
	// The consumer is dormant until release-controller emits its first
	// release.promoted:v1 event in production.
	releasePromotionRepo := neo4jinfra.NewReleasePromotionRepository(neo4jClient, logger)
	releasePromotedHandler := handlers.NewReleasePromotedHandler(postgres.NewPostgresUnitOfWork(pgDB, logger), releasePromotionRepo, topologyRepo, topologyStateRepo, logger)
	releasePromotedBinding := redis.NewReleasePromotedBinding(releasePromotedHandler, logger)
	releasePromotedConsumer := pkgredis.NewStreamConsumer(
		redisClient,
		streams.ReleasePromotedV1,
		streams.OrchestratorReleasePromoted,
		releasePromotedBinding,
		logger,
	)

	// Start all consumers in goroutines
	go func() {
		if err := nodeUpdatedConsumer.Start(ctx); err != nil {
			logger.Error("Node updated consumer error", "error", err)
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

	go func() {
		if err := releasePromotedConsumer.Start(ctx); err != nil {
			logger.Error("Release promoted consumer error", "error", err)
		}
	}()

	// ========================================================================
	// START gRPC SERVER (BLOCKING)
	// ========================================================================

	runQueries := queries.NewRunQueryService(queryRepo, topologyStateRepo, logger)
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
