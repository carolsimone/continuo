package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/carolsimone/continuo/k8s-controller/adapters/http"
	"github.com/carolsimone/continuo/k8s-controller/adapters/k8s"
	"github.com/carolsimone/continuo/k8s-controller/adapters/postgres"
	"github.com/carolsimone/continuo/k8s-controller/adapters/redis"
	s3adapter "github.com/carolsimone/continuo/k8s-controller/adapters/s3"
	"github.com/carolsimone/continuo/k8s-controller/config"
	"github.com/carolsimone/continuo/k8s-controller/domain/command"
	"github.com/carolsimone/continuo/k8s-controller/internal/lifecycle"
	"github.com/carolsimone/continuo/k8s-controller/service/handlers"
	"github.com/carolsimone/continuo/k8s-controller/service/messagebus"
	"github.com/carolsimone/continuo/k8s-controller/service/uow"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

func main() {
	// Step 1: Setup structured JSON logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	v := &pkgconfig.Validator{}
	cfg := config.Load(v)
	if missing := v.Missing(); len(missing) > 0 {
		logger.Error("missing required env vars", "vars", strings.Join(missing, ", "))
		os.Exit(1)
	}

	logger.Info("Starting k8s-controller service")

	// Step 2: Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Step 3: Initialize lifecycle manager for graceful shutdown
	lifecycleManager := lifecycle.NewApplicationLifecycle(logger)
	lifecycleManager.SetupSignalHandlers(ctx, cancel)

	// Step 4: Initialize Redis client
	redisClient := goredis.NewClient(&goredis.Options{
		Addr:     cfg.Redis.Addr(),
		Password: cfg.Redis.Password,
	})
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Closing Redis connection")
		return redisClient.Close()
	})

	logger.Info("Connected to Redis", "addr", cfg.Redis.Addr())

	// Step 5: Initialize PostgreSQL client
	pgDB, err := postgres.NewPostgresClient(
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.DB,
		cfg.Postgres.User,
		cfg.Postgres.Password,
		logger,
	)
	if err != nil {
		logger.Error("Failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Closing PostgreSQL connection")
		return pgDB.Close()
	})

	// Step 6: Initialize repositories and transaction runner
	outboxRepo := postgres.NewOutboxRepository(pgDB, logger)
	txRunner := uow.NewPostgresTransactionRunner(pgDB, logger)

	logger.Info("PostgreSQL repositories initialized")

	// Step 8: Initialize K8s client
	k8sClient, err := k8s.NewK8sClient(logger)
	if err != nil {
		logger.Error("Failed to initialize K8s client", "error", err)
		os.Exit(1)
	}

	// Step 9: Initialize Redis producer
	producer := redis.NewMultiProducer(
		redisClient,
		streams.CheckK8sV1,
		streams.RetryTaskV1,
		streams.TaskFailedV1,
		logger,
	)

	logger.Info("Redis producer initialized",
		"check_stream", streams.CheckK8sV1,
		"retry_stream", streams.RetryTaskV1,
		"failed_stream", streams.TaskFailedV1,
	)

	// Step 10: Initialize S3 log uploader
	s3Client := s3adapter.NewS3Client(
		cfg.S3.EndpointURL,
		cfg.S3.Bucket,
		cfg.S3.Region,
		cfg.S3.AccessKeyID,
		cfg.S3.SecretAccessKey,
	)

	// Initialize cancelled schedules repository (needed by CheckStatusHandler guard)
	cancelledSchedulesRepo := postgres.NewCancelledSchedulesRepository(pgDB)

	// Step 10b: Initialize check status handler with transaction runner
	handlerConfig := &handlers.HandlerConfig{
		K8sNamespace:          cfg.K8sNamespace,
		CheckDelaySeconds:     cfg.K8sCheckDelaySeconds,
		ErrorMessageMaxLen:    cfg.ErrorMessageMaxLength,
		LogTailLines:          int64(cfg.LogTailLines),
		DefaultTaskMaxRetries: cfg.DefaultTaskMaxRetries,
	}
	checkStatusHandler := handlers.NewCheckStatusHandler(k8sClient, txRunner, s3Client, handlerConfig, cancelledSchedulesRepo, logger)

	// Step 11: Create command handlers map (CQRS pattern)
	commandHandlers := map[string]messagebus.CommandHandler{
		"command.CheckJobStatus": func(ctx context.Context, cmd command.Command) error {
			return checkStatusHandler.Handle(ctx, cmd.(command.CheckJobStatus))
		},
	}

	// Step 12: Create MessageBus
	messageBus := messagebus.NewMessageBus(commandHandlers, logger)

	logger.Info("Message bus initialized")

	// Step 13: Initialize per-stream Redis consumers.
	// Each consumer runs in its own goroutine against a dedicated consumer group.
	consumerName := fmt.Sprintf("consumer-%s", uuid.New().String()[:8])

	deployedConsumer, err := redis.NewConsumer(
		redisClient,
		consumerName,
		streams.K8sDeployed,
		streams.NodeDeployedV1,
		messageBus,
		cfg.DefaultTaskMaxRetries,
		false, // node.deployed:v1 messages are processed immediately
		logger,
	)
	if err != nil {
		logger.Error("Failed to initialize deployed consumer", "error", err)
		os.Exit(1)
	}

	checkConsumer, err := redis.NewConsumer(
		redisClient,
		consumerName,
		streams.K8sCheckStatus,
		streams.CheckK8sV1,
		messageBus,
		cfg.DefaultTaskMaxRetries,
		true, // check.k8s:v1 messages are re-circulated until check_after elapses
		logger,
	)
	if err != nil {
		logger.Error("Failed to initialize check consumer", "error", err)
		os.Exit(1)
	}

	logger.Info("Redis consumers initialized",
		"deployed_stream", streams.NodeDeployedV1,
		"deployed_group", streams.K8sDeployed,
		"check_stream", streams.CheckK8sV1,
		"check_group", streams.K8sCheckStatus,
	)

	// Step 14: Initialize and start OutboxProcessor (background)
	outboxProcessor := handlers.NewOutboxProcessor(
		outboxRepo,
		producer,
		logger,
	)

	go func() {
		logger.Info("Starting outbox processor")
		if err := outboxProcessor.Run(ctx); err != nil && err != context.Canceled {
			logger.Error("Outbox processor stopped with error", "error", err)
		}
	}()

	logger.Info("Outbox processor started")

	// Step 15: Initialize and start StuckEntryResolver (background)
	resolverConfig := &handlers.ResolverConfig{
		CheckIntervalSeconds:  cfg.Resolver.CheckIntervalSeconds,
		StuckThresholdSeconds: cfg.Resolver.StuckThresholdSeconds,
		BatchSize:             cfg.Resolver.BatchSize,
		MaxResolveAttempts:    cfg.Resolver.MaxAttempts,
	}

	stuckEntryResolver := handlers.NewStuckEntryResolver(
		outboxRepo,
		resolverConfig,
		logger,
	)

	go func() {
		logger.Info("Starting stuck entry resolver")
		if err := stuckEntryResolver.Run(ctx); err != nil && err != context.Canceled {
			logger.Error("Stuck entry resolver stopped with error", "error", err)
		}
	}()

	logger.Info("Stuck entry resolver started",
		"check_interval", resolverConfig.CheckIntervalSeconds,
		"stuck_threshold", resolverConfig.StuckThresholdSeconds,
	)

	// Step 16: Start HTTP Health Server (background)
	healthServer := http.NewHealthServer(cfg.HTTPPort, logger)
	go func() {
		if err := healthServer.Start(); err != nil {
			logger.Error("Health server stopped", "error", err)
		}
	}()
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Shutting down health server")
		return healthServer.Shutdown(ctx)
	})

	logger.Info("HTTP health server started", "port", cfg.HTTPPort)

	// ========================================================================
	// INITIALIZE CANCELLED SCHEDULES CONSUMER + SWEEPER
	// ========================================================================

	scheduleCancelledConsumer, err := redis.NewScheduleCancelledConsumer(
		redisClient,
		streams.ScheduleCancelledV1,
		streams.K8sScheduleCancelled,
		cancelledSchedulesRepo,
		logger,
	)
	if err != nil {
		logger.Error("Failed to create schedule cancelled consumer", "error", err)
		os.Exit(1)
	}
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

	// Step 17: Start Redis consumers.
	// The deployed consumer runs in the background; the check consumer runs as the main blocking loop.
	go func() {
		logger.Info("Starting deployed consumer")
		if err := deployedConsumer.Start(ctx); err != nil && err != context.Canceled {
			logger.Error("Deployed consumer stopped with error", "error", err)
		}
	}()

	logger.Info("Starting check consumer (main loop)")
	if err := checkConsumer.Start(ctx); err != nil && err != context.Canceled {
		logger.Error("Check consumer stopped with error", "error", err)
		os.Exit(1)
	}

	logger.Info("k8s-controller service stopped")
}
