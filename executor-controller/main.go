package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/carolsimone/continuo/executor-controller/adapters/grpc"
	"github.com/carolsimone/continuo/executor-controller/adapters/http"
	"github.com/carolsimone/continuo/executor-controller/adapters/k8s"
	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/executor-controller/adapters/redis"
	"github.com/carolsimone/continuo/executor-controller/config"
	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/internal/lifecycle"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/messagebus"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	goredis "github.com/redis/go-redis/v9"
)

func main() {
	// Setup structured logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	logger.Info("Starting executor-controller service")

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize lifecycle manager
	lifecycleManager := lifecycle.NewApplicationLifecycle(logger)
	lifecycleManager.SetupSignalHandlers(ctx, cancel)

	// ========================================================================
	// INITIALIZE DEPENDENCIES
	// ========================================================================

	// 1. PostgreSQL (for deployment_outbox table)
	pgDB, err := postgres.NewPostgresClient(
		config.GetPostgresHost(),
		config.GetPostgresPort(),
		config.GetPostgresDB(),
		config.GetPostgresUser(),
		config.GetPostgresPassword(),
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

	// 2. Redis client
	redisClient := goredis.NewClient(&goredis.Options{
		Addr: fmt.Sprintf("%s:%d", config.GetRedisHost(), config.GetRedisPort()),
		DB:   0,
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

	// 3. K8s client (in-cluster config)
	k8sClient, err := k8s.NewK8sClient(logger)
	if err != nil {
		logger.Error("Failed to create K8s client", "error", err)
		os.Exit(1)
	}
	logger.Info("K8s client initialized")

	// 4. gRPC client to state service
	stateClient, err := grpc.NewStateClient(config.GetStateServiceGRPCAddr(), logger)
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

	outboxRepo := postgres.NewOutboxRepository(pgDB, logger)

	// ========================================================================
	// INITIALIZE UNIT OF WORK & HANDLERS
	// ========================================================================

	unitOfWork := uow.NewPostgresUnitOfWork(pgDB, logger)

	// Create handler
	deployHandler := handlers.NewDeployHandler(unitOfWork, logger)

	// Create command handlers map
	commandHandlers := map[string]messagebus.CommandHandler{
		"command.DeployJob": func(ctx context.Context, cmd command.Command) error {
			return deployHandler.Handle(ctx, cmd.(command.DeployJob))
		},
	}

	// Create MessageBus
	messageBus := messagebus.NewMessageBus(
		unitOfWork,
		commandHandlers,
		map[string][]messagebus.EventHandler{}, // No event handlers yet
		logger,
	)

	// ========================================================================
	// INITIALIZE REDIS CONSUMER & PRODUCER
	// ========================================================================

	// Create dual-stream consumer for query.model:v1 and task.retry:v1 streams
	consumer, err := redis.NewConsumer(
		redisClient,
		config.GetRedisConsumerStream(),      // query.model:v1
		config.GetRedisConsumerRetryStream(), // task.retry:v1
		config.GetRedisConsumerGroup(),
		messageBus,
		pgDB,
		logger,
	)
	if err != nil {
		logger.Error("Failed to create Redis consumer", "error", err)
		os.Exit(1)
	}

	logger.Info("Redis consumer initialized",
		"streams", []string{config.GetRedisConsumerStream(), config.GetRedisConsumerRetryStream()},
		"consumer_group", config.GetRedisConsumerGroup(),
	)

	// Create producer for executor.deployed:v1 stream
	producer := redis.NewProducer(
		redisClient,
		config.GetRedisProducerStream(),
		logger,
	)

	// ========================================================================
	// INITIALIZE OUTBOX PROCESSOR
	// ========================================================================

	outboxProcessor := handlers.NewOutboxProcessor(
		outboxRepo,
		k8sClient,
		stateClient,
		producer,
		logger,
	)

	// Start outbox processor in background goroutine
	go func() {
		if err := outboxProcessor.Run(ctx); err != nil {
			logger.Error("Outbox processor error", "error", err)
		}
	}()

	// ========================================================================
	// START HTTP HEALTH CHECK SERVER
	// ========================================================================

	healthServer := http.NewHealthServer(config.GetHTTPPort(), logger)

	go func() {
		if err := healthServer.Start(); err != nil {
			logger.Error("Health server error", "error", err)
		}
	}()

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		return healthServer.Shutdown(ctx)
	})

	// ========================================================================
	// START REDIS CONSUMER (BLOCKING)
	// ========================================================================

	logger.Info("Service initialization complete, starting consumer")

	if err := consumer.Start(ctx); err != nil {
		logger.Error("Consumer error", "error", err)
		os.Exit(1)
	}

	logger.Info("Executor-controller service stopped")
}
