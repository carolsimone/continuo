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
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
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

	// Initialize schedule catalog consumer (consumes schedules.loaded:v1)
	catalogConsumer, err := redis.NewScheduleCatalogConsumer(
		redisClient,
		cfg.RedisStreamSchedulesLoaded,
		catalogRepo,
		db,
		logger,
	)
	if err != nil {
		logger.Error("Failed to create schedule catalog consumer", "error", err)
		os.Exit(1)
	}
	logger.Info("Schedule catalog consumer initialized")

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Stopping schedule catalog consumer")
		catalogConsumer.Stop()
		return nil
	})

	// Start catalog consumer in background
	go func() {
		if err := catalogConsumer.Start(ctx); err != nil {
			logger.Error("Schedule catalog consumer error", "error", err)
		}
	}()

	// Initialize run.entries.dispatched:v1 consumer
	runEntriesConsumer, err := redis.NewRunEntriesDispatchedConsumer(
		redisClient,
		cfg.RedisStreamRunEntriesDispatched,
		db,
		schedulerRepo,
		taskRepo,
		logger,
	)
	if err != nil {
		logger.Error("Failed to create run entries dispatched consumer", "error", err)
		os.Exit(1)
	}
	logger.Info("Run entries dispatched consumer initialized")

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Stopping run entries dispatched consumer")
		runEntriesConsumer.Stop()
		return nil
	})

	// Start run entries dispatched consumer in background
	go func() {
		if err := runEntriesConsumer.Start(ctx); err != nil {
			logger.Error("Run entries dispatched consumer error", "error", err)
		}
	}()

	// Initialize run.entries.dispatch_failed:v1 consumer
	runEntriesDispatchFailedConsumer, err := redis.NewRunEntriesDispatchFailedConsumer(
		redisClient,
		cfg.RedisStreamRunEntriesDispatchFailed,
		db,
		schedulerRepo,
		outboxRepo,
		logger,
	)
	if err != nil {
		logger.Error("Failed to create run entries dispatch failed consumer", "error", err)
		os.Exit(1)
	}
	logger.Info("Run entries dispatch failed consumer initialized")

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Stopping run entries dispatch failed consumer")
		runEntriesDispatchFailedConsumer.Stop()
		return nil
	})

	go func() {
		if err := runEntriesDispatchFailedConsumer.Start(ctx); err != nil {
			logger.Error("Run entries dispatch failed consumer error", "error", err)
		}
	}()

	// Initialize run.rerun.dispatched:v1 consumer
	runRerunConsumer, err := redis.NewRunRerunDispatchedConsumer(
		redisClient,
		cfg.RedisStreamRunRerunDispatched,
		db,
		schedulerRepo,
		taskRepo,
		logger,
	)
	if err != nil {
		logger.Error("Failed to create run rerun dispatched consumer", "error", err)
		os.Exit(1)
	}
	logger.Info("Run rerun dispatched consumer initialized")

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Stopping run rerun dispatched consumer")
		runRerunConsumer.Stop()
		return nil
	})

	// Start run rerun dispatched consumer in background
	go func() {
		if err := runRerunConsumer.Start(ctx); err != nil {
			logger.Error("Run rerun dispatched consumer error", "error", err)
		}
	}()

	// Initialize task.status.updated:v1 consumer
	taskStatusConsumer, err := redis.NewTaskStatusUpdatedConsumer(
		redisClient,
		cfg.RedisStreamTaskStatusUpdated,
		db,
		schedulerRepo,
		taskRepo,
		outboxRepo,
		logger,
	)
	if err != nil {
		logger.Error("Failed to create task status updated consumer", "error", err)
		os.Exit(1)
	}
	logger.Info("Task status updated consumer initialized")

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Stopping task status updated consumer")
		taskStatusConsumer.Stop()
		return nil
	})

	// Start task status updated consumer in background
	go func() {
		if err := taskStatusConsumer.Start(ctx); err != nil {
			logger.Error("Task status updated consumer error", "error", err)
		}
	}()

	// Initialize task.execution.recorded:v1 consumer
	taskExecutionRecordedConsumer, err := redis.NewTaskExecutionRecordedConsumer(
		redisClient,
		cfg.RedisStreamTaskExecutionRecorded,
		db,
		taskExecutionRepo,
		logger,
	)
	if err != nil {
		logger.Error("Failed to create task execution recorded consumer", "error", err)
		os.Exit(1)
	}
	logger.Info("Task execution recorded consumer initialized")

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Stopping task execution recorded consumer")
		taskExecutionRecordedConsumer.Stop()
		return nil
	})

	// Start task execution recorded consumer in background
	go func() {
		if err := taskExecutionRecordedConsumer.Start(ctx); err != nil {
			logger.Error("Task execution recorded consumer error", "error", err)
		}
	}()

	// Initialize schedule activator and activation service
	activator := scheduler.NewScheduleActivator(schedulerRepo, logger)
	activationService := scheduler.NewScheduleActivationService(
		db,
		activator,
		catalogRepo,
		schedulerRepo,
		outboxRepo,
		cfg.RedisStreamSchedulerStarted,
		logger,
	)
	logger.Info("Schedule activator and activation service initialized")

	// Load schedules config — fail fast if missing or malformed
	schedulesConfig, err := scheduler.LoadSchedulesConfig(cfg.SchedulesConfigPath)
	if err != nil {
		logger.Error("Failed to load schedules config", "error", err)
		os.Exit(1)
	}
	logger.Info("Schedules config loaded", "schedules", len(schedulesConfig.Schedules))

	// Initialize cron scheduler
	cronScheduler, err := scheduler.NewCronSchedulerWithConfig(activationService, logger, schedulesConfig)
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
	schedulerHandler := handlers.NewSchedulerHandler(schedulerRepo, activationService, catalogRepo, schedulesConfig, logger)
	schedulerHandler.WithCancelDeps(db, taskRepo, outboxRepo)
	taskHandler := handlers.NewTaskHandler(taskRepo, logger)
	taskExecutionHandler := handlers.NewTaskExecutionHandler(taskExecutionRepo, logger)
	rerunHandler := handlers.NewRerunHandler(db, schedulerRepo, taskRepo, outboxRepo, logger)
	singleNodeRunHandler := handlers.NewSingleNodeRunHandler(db, schedulerRepo, taskRepo, outboxRepo, logger)

	// Create gRPC server
	grpcServer, err := grpcserver.NewServer(cfg.GRPCPort, schedulerHandler, taskHandler, taskExecutionHandler, rerunHandler, singleNodeRunHandler, logger)
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
