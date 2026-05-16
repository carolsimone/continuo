package main

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/carolsimone/continuo/state/adapters/http"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/adapters/redis"
	"github.com/carolsimone/continuo/state/config"
	"github.com/carolsimone/continuo/state/database"
	grpcserver "github.com/carolsimone/continuo/state/internal/grpc"
	"github.com/carolsimone/continuo/state/internal/grpc/handlers"
	"github.com/carolsimone/continuo/state/internal/lifecycle"
	"github.com/carolsimone/continuo/state/internal/scheduler"
	"github.com/carolsimone/continuo/state/ports"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
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

	logger.Info("Starting state service")

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize lifecycle manager
	lifecycleManager := lifecycle.NewApplicationLifecycle(logger)
	lifecycleManager.SetupSignalHandlers(ctx, cancel)

	// Initialize PostgreSQL connection
	db, err := database.NewConnection(cfg.Postgres)
	if err != nil {
		logger.Error("Failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	logger.Info("PostgreSQL connection established")

	// Register database cleanup
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Closing database connection")
		return db.Close()
	})

	// Initialize repositories
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	catalogRepo := postgres.NewScheduleCatalogRepository(db, logger)
	logger.Info("Schedule catalog repository initialized")
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)
	taskExecutionRepo := postgres.NewTaskExecutionRepository(db, logger)
	outboxRepo := postgres.NewOutboxRepository(db, logger)

	// Initialize Redis client
	redisClient := goredis.NewClient(&goredis.Options{
		Addr:     cfg.Redis.Addr(),
		Password: cfg.Redis.Password,
	})

	// Test Redis connection
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err := redisClient.Ping(pingCtx).Err(); err != nil {
		logger.Error("Failed to connect to Redis", "error", err)
		os.Exit(1)
	}
	logger.Info("Redis connection established")

	// Register Redis client cleanup
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Closing Redis client")
		return redisClient.Close()
	})

	// Start outbox processor
	outboxProcessor := svchandlers.NewOutboxProcessor(outboxRepo, redisClient, logger)
	go func() {
		if err := outboxProcessor.Run(ctx); err != nil {
			logger.Error("Outbox processor error", "error", err)
		}
	}()

	// Aggregate-level port adapters wired into the UoW so handlers that
	// operate on domain aggregates (Run, ScheduleCatalog) have typed access.
	runRepoPort := postgres.NewRunRepository(db, schedulerRepo, taskRepo, outboxRepo, logger)
	catalogRepoPort := postgres.NewCatalogRepositoryAdapter(db, catalogRepo, logger)
	outboxPub := postgres.NewOutboxPublisher(outboxRepo)
	clk := ports.SystemClock{}

	// UoW factory shared by every stream binding below. Each invocation
	// returns a fresh PostgresUnitOfWork over the same repos and *sqlx.DB
	// so concurrent message handlers do not share transaction state.
	uowFactory := func() uow.UnitOfWork {
		return uow.NewPostgresUnitOfWork(db, schedulerRepo, taskRepo, taskExecutionRepo, catalogRepo, outboxRepo, runRepoPort, catalogRepoPort, outboxPub, clk, logger)
	}

	// Schedule catalog consumer (consumes schedules.loaded:v1). The
	// StreamConsumer drives the parser+dedup+UoW binding declared in the
	// redis adapter. Lifecycle is tied to ctx — the lifecycle manager
	// cancels ctx on shutdown, which exits Start cleanly.
	catalogHandler := svchandlers.NewScheduleCatalogHandler(logger)
	catalogBinding := redis.NewScheduleCatalogBinding(uowFactory, catalogHandler, logger)
	catalogConsumer := pkgredis.NewStreamConsumer(
		redisClient,
		streams.SchedulesLoadedV1,
		streams.StateScheduleCatalog,
		catalogBinding,
		logger,
	)
	logger.Info("Schedule catalog consumer initialized")
	go func() {
		if err := catalogConsumer.Start(ctx); err != nil {
			logger.Error("Schedule catalog consumer error", "error", err)
		}
	}()

	// run.entries.dispatched:v1 consumer. The StreamConsumer drives the
	// parser+dedup+UoW binding declared in the redis adapter. Lifecycle is
	// tied to ctx — the lifecycle manager cancels ctx on shutdown, which
	// exits Start cleanly.
	runEntriesDispatchedHandler := svchandlers.NewRunEntriesDispatchedHandler(logger)
	runEntriesDispatchedBinding := redis.NewRunEntriesDispatchedBinding(uowFactory, runEntriesDispatchedHandler, logger)
	runEntriesDispatchedConsumer := pkgredis.NewStreamConsumer(
		redisClient,
		streams.RunEntriesDispatchedV1,
		streams.StateRunEntriesDispatched,
		runEntriesDispatchedBinding,
		logger,
	)
	logger.Info("Run entries dispatched consumer initialized")
	go func() {
		if err := runEntriesDispatchedConsumer.Start(ctx); err != nil {
			logger.Error("Run entries dispatched consumer error", "error", err)
		}
	}()

	// run.entries.dispatch_failed:v1 consumer. The StreamConsumer drives the
	// parser+dedup+UoW binding declared in the redis adapter. Lifecycle is
	// tied to ctx — the lifecycle manager cancels ctx on shutdown, which
	// exits Start cleanly.
	runEntriesDispatchFailedHandler := svchandlers.NewRunEntriesDispatchFailedHandler(logger)
	runEntriesDispatchFailedBinding := redis.NewRunEntriesDispatchFailedBinding(uowFactory, runEntriesDispatchFailedHandler, logger)
	runEntriesDispatchFailedConsumer := pkgredis.NewStreamConsumer(
		redisClient,
		streams.RunEntriesDispatchFailedV1,
		streams.StateRunEntriesDispatchFailed,
		runEntriesDispatchFailedBinding,
		logger,
	)
	logger.Info("Run entries dispatch failed consumer initialized")
	go func() {
		if err := runEntriesDispatchFailedConsumer.Start(ctx); err != nil {
			logger.Error("Run entries dispatch failed consumer error", "error", err)
		}
	}()

	// task.status.updated:v1 consumer. The StreamConsumer drives the
	// parser+dedup+UoW binding declared in the redis adapter. Lifecycle is
	// tied to ctx — the lifecycle manager cancels ctx on shutdown, which
	// exits Start cleanly.
	taskStatusUpdatedHandler := svchandlers.NewTaskStatusUpdatedHandler(logger)
	taskStatusUpdatedBinding := redis.NewTaskStatusUpdatedBinding(uowFactory, taskStatusUpdatedHandler, logger)
	taskStatusUpdatedConsumer := pkgredis.NewStreamConsumer(
		redisClient,
		streams.TaskStatusUpdatedV1,
		streams.StateTaskStatusUpdated,
		taskStatusUpdatedBinding,
		logger,
	)
	logger.Info("Task status updated consumer initialized")
	go func() {
		if err := taskStatusUpdatedConsumer.Start(ctx); err != nil {
			logger.Error("Task status updated consumer error", "error", err)
		}
	}()

	// task.execution.recorded:v1 consumer. The StreamConsumer drives the
	// parser+dedup+UoW binding declared in the redis adapter. Lifecycle is
	// tied to ctx — the lifecycle manager cancels ctx on shutdown, which
	// exits Start cleanly.
	taskExecutionRecordedHandler := svchandlers.NewTaskExecutionRecordedHandler(logger)
	taskExecutionRecordedBinding := redis.NewTaskExecutionRecordedBinding(uowFactory, taskExecutionRecordedHandler, logger)
	taskExecutionRecordedConsumer := pkgredis.NewStreamConsumer(
		redisClient,
		streams.TaskExecutionRecordedV1,
		streams.StateTaskExecutionRecorded,
		taskExecutionRecordedBinding,
		logger,
	)
	logger.Info("Task execution recorded consumer initialized")
	go func() {
		if err := taskExecutionRecordedConsumer.Start(ctx); err != nil {
			logger.Error("Task execution recorded consumer error", "error", err)
		}
	}()

	// Initialize activation handler shared by the cron loop and gRPC methods.
	activateHandler := svchandlers.NewActivateScheduleHandler(logger)
	logger.Info("Activation handler initialized")

	// Load schedules config — fail fast if missing or malformed
	schedulesConfig, err := scheduler.LoadSchedulesConfig(cfg.SchedulesConfigPath)
	if err != nil {
		logger.Error("Failed to load schedules config", "error", err)
		os.Exit(1)
	}
	logger.Info("Schedules config loaded", "schedules", len(schedulesConfig.Schedules))

	// Initialize cron scheduler
	cronScheduler, err := scheduler.NewCronSchedulerWithConfig(activateHandler, uowFactory, logger, schedulesConfig)
	if err != nil {
		logger.Error("Failed to create cron scheduler", "error", err)
		os.Exit(1)
	}

	// Register cron scheduler cleanup
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Stopping cron scheduler")
		return cronScheduler.Stop(ctx)
	})

	// Start cron scheduler
	if err := cronScheduler.Start(); err != nil {
		logger.Error("Failed to start cron scheduler", "error", err)
		os.Exit(1)
	}
	logger.Info("Cron scheduler started")

	// Initialize gRPC handlers
	schedulerHandler := handlers.NewSchedulerHandler(schedulerRepo, activateHandler, catalogRepo, schedulesConfig, uowFactory, logger)
	taskHandler := handlers.NewTaskHandler(taskRepo, logger)
	taskExecutionHandler := handlers.NewTaskExecutionHandler(taskExecutionRepo, logger)
	rerunHandler := handlers.NewRerunHandler(db, schedulerRepo, taskRepo, outboxRepo, logger)
	singleNodeRunHandler := handlers.NewSingleNodeRunHandler(db, schedulerRepo, taskRepo, outboxRepo, logger)
	rebaseHandler := handlers.NewRebaseHandler(db, schedulerRepo, taskRepo, outboxRepo, logger)
	nodeRunRepo := postgres.NewNodeRunRepository(db, logger)
	nodeRunHandler := handlers.NewNodeRunHandler(nodeRunRepo, logger)

	// Create gRPC server
	grpcServer, err := grpcserver.NewServer(cfg.GRPCPort, schedulerHandler, taskHandler, taskExecutionHandler, rerunHandler, singleNodeRunHandler, rebaseHandler, nodeRunHandler, logger)
	if err != nil {
		logger.Error("Failed to create gRPC server", "error", err)
		os.Exit(1)
	}

	// Register gRPC server cleanup
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		return grpcServer.Shutdown(ctx)
	})

	// Start gRPC server in background
	go func() {
		if err := grpcServer.Start(); err != nil {
			logger.Error("gRPC server error", "error", err)
		}
	}()

	// Start HTTP health server (health-only; rerun moved to gRPC)
	healthServer := http.NewServer(cfg.HealthPort, logger)

	// Register health server cleanup
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		return healthServer.Shutdown(ctx)
	})

	go func() {
		if err := healthServer.Start(); err != nil {
			logger.Error("Health server error", "error", err)
		}
	}()

	logger.Info("State service started successfully",
		"grpc_port", cfg.GRPCPort,
		"health_port", cfg.HealthPort,
	)

	// Wait for shutdown signal
	<-ctx.Done()
	logger.Info("Shutting down...")
	time.Sleep(2 * time.Second)
	logger.Info("Service stopped")
}
