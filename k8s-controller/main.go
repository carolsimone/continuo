package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/carolsimone/continuo/k8s-controller/adapters/grpc"
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

	goredis "github.com/redis/go-redis/v9"
)

func main() {
	// Step 1: Setup structured JSON logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	cfg := config.Load()

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

	// Step 6: Initialize repositories and Unit of Work
	outboxRepo := postgres.NewOutboxRepository(pgDB, logger)
	unitOfWork := uow.NewPostgresUnitOfWork(pgDB, logger)

	logger.Info("PostgreSQL repositories initialized")

	// Step 7: Initialize gRPC state client
	stateClient, err := grpc.NewStateClient(cfg.StateGRPCAddr, logger)
	if err != nil {
		logger.Error("Failed to initialize state client", "error", err)
		os.Exit(1)
	}
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Closing state gRPC connection")
		return stateClient.Close()
	})

	// Step 8: Initialize K8s client
	k8sClient, err := k8s.NewK8sClient(logger)
	if err != nil {
		logger.Error("Failed to initialize K8s client", "error", err)
		os.Exit(1)
	}

	// Step 9: Initialize Redis producer
	producer := redis.NewMultiProducer(
		redisClient,
		cfg.RedisProducerCheckStream,
		cfg.RedisProducerRetryStream,
		cfg.RedisProducerFailedStream,
		logger,
	)

	logger.Info("Redis producer initialized",
		"check_stream", cfg.RedisProducerCheckStream,
		"retry_stream", cfg.RedisProducerRetryStream,
		"failed_stream", cfg.RedisProducerFailedStream,
	)

	// Step 10: Initialize S3 log uploader
	s3Client := s3adapter.NewS3Client(
		cfg.S3.EndpointURL,
		cfg.S3.Bucket,
		cfg.S3.Region,
		cfg.S3.AccessKeyID,
		cfg.S3.SecretAccessKey,
	)

	// Step 10b: Initialize check status handler with UoW
	handlerConfig := &handlers.HandlerConfig{
		K8sNamespace:       cfg.K8sNamespace,
		CheckDelaySeconds:  cfg.K8sCheckDelaySeconds,
		ErrorMessageMaxLen: cfg.ErrorMessageMaxLength,
		LogTailLines:       int64(cfg.LogTailLines),
	}
	checkStatusHandler := handlers.NewCheckStatusHandler(k8sClient, stateClient, unitOfWork, s3Client, handlerConfig, logger)

	// Step 11: Create command handlers map (CQRS pattern)
	commandHandlers := map[string]messagebus.CommandHandler{
		"command.CheckJobStatus": func(ctx context.Context, cmd command.Command) error {
			return checkStatusHandler.Handle(ctx, cmd.(command.CheckJobStatus))
		},
	}

	// Step 12: Create MessageBus
	messageBus := messagebus.NewMessageBus(commandHandlers, logger)

	logger.Info("Message bus initialized")

	// Step 13: Initialize Redis dual-stream consumer
	consumer, err := redis.NewDualStreamConsumer(
		redisClient,
		cfg.RedisConsumerDeployedStream,
		cfg.RedisConsumerCheckStream,
		cfg.RedisConsumerGroup,
		messageBus,
		logger,
	)
	if err != nil {
		logger.Error("Failed to initialize consumer", "error", err)
		os.Exit(1)
	}

	logger.Info("Redis consumer initialized",
		"deployed_stream", cfg.RedisConsumerDeployedStream,
		"check_stream", cfg.RedisConsumerCheckStream,
		"consumer_group", cfg.RedisConsumerGroup,
	)

	// Step 14: Initialize and start OutboxProcessor (background)
	outboxProcessor := handlers.NewOutboxProcessor(
		outboxRepo,
		stateClient,
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

	// Step 17: Start Redis Consumer (BLOCKING - main loop)
	logger.Info("Starting Redis consumer (main loop)")
	if err := consumer.Start(ctx); err != nil && err != context.Canceled {
		logger.Error("Consumer stopped with error", "error", err)
		os.Exit(1)
	}

	logger.Info("k8s-controller service stopped")
}
