package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/carolsimone/continuo/orchestrator/application/command"
	"github.com/carolsimone/continuo/orchestrator/config"
	grpcinfra "github.com/carolsimone/continuo/orchestrator/infrastructure/grpc"
	httpinfra "github.com/carolsimone/continuo/orchestrator/infrastructure/http"
	neo4jinfra "github.com/carolsimone/continuo/orchestrator/infrastructure/neo4j"
	"github.com/carolsimone/continuo/orchestrator/infrastructure/postgres"
	"github.com/carolsimone/continuo/orchestrator/infrastructure/redis"
	"github.com/carolsimone/continuo/orchestrator/internal/lifecycle"
	"github.com/carolsimone/continuo/orchestrator/internal/sweeper"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
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

	// 4. State gRPC client
	stateClient, err := grpcinfra.NewStateClient(cfg.StateGRPCAddr, logger)
	if err != nil {
		logger.Error("Failed to create state service client", "error", err)
		os.Exit(1)
	}
	logger.Info("State service gRPC client initialized")

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Closing state service gRPC client")
		return stateClient.Close()
	})

	// ========================================================================
	// INITIALIZE REPOSITORIES
	// ========================================================================

	topologyRepo := neo4jinfra.NewTopologyRepository(neo4jClient, logger)
	runRepoWrite := neo4jinfra.NewRunRepository(neo4jClient, logger)
	queryRepo := neo4jinfra.NewQueryRepository(neo4jClient, logger)
	runRepo := neo4jinfra.NewCompositeRunRepository(runRepoWrite, queryRepo)
	outboxRepo := postgres.NewOutboxRepository(pgDB, logger)
	publishedRepo := postgres.NewPublishedMessagesRepository(pgDB, logger)

	// ========================================================================
	// INITIALIZE UNIT OF WORK & COMMAND HANDLERS
	// ========================================================================

	unitOfWork := uow.NewPostgresUnitOfWork(pgDB, logger)

	ingestTopologyHandler := command.NewIngestTopologyHandler(unitOfWork, topologyRepo, logger)
	initializeRunHandler := command.NewInitializeRunHandler(unitOfWork, runRepo, logger)
	handleNodeCompletedHandler := command.NewHandleNodeCompletedHandler(unitOfWork, runRepo, stateClient, logger)

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
	// INITIALIZE REDIS CONSUMERS (3 streams)
	// ========================================================================

	// Consumer 1: node.updated:v1 -> HandleNodeCompleted
	nodeUpdatedHandler := func(ctx context.Context, msg goredis.XMessage) error {
		cmd := command.HandleNodeCompletedCmd{
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
		var nodes []command.TopologyNodePayload
		if err := json.Unmarshal([]byte(payloadStr), &nodes); err != nil {
			return fmt.Errorf("failed to unmarshal manifest.loaded payload: %w", err)
		}
		cmd := command.IngestTopologyCmd{Nodes: nodes}
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
		cmd := command.InitializeRunCmd{
			ScheduleName: scheduleName,
			RunID:        runID,
		}
		// Check for rerun target fields
		if svc, ok := msg.Values["rerun_service_name"].(string); ok && svc != "" {
			cmd.RerunTarget = &command.RerunTarget{
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

	// ========================================================================
	// START gRPC SERVER (BLOCKING)
	// ========================================================================

	queryHandler := grpcinfra.NewQueryHandler(queryRepo, logger)

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
