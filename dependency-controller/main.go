package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	grpcclient "github.com/carolsimone/continuo/dependency-controller/adapters/grpc"
	"github.com/carolsimone/continuo/dependency-controller/adapters/http"
	"github.com/carolsimone/continuo/dependency-controller/adapters/postgres"
	"github.com/carolsimone/continuo/dependency-controller/adapters/redis"
	"github.com/carolsimone/continuo/dependency-controller/config"
	"github.com/carolsimone/continuo/dependency-controller/domain/command"
	"github.com/carolsimone/continuo/dependency-controller/internal/lifecycle"
	"github.com/carolsimone/continuo/dependency-controller/service/handlers"
	"github.com/carolsimone/continuo/dependency-controller/service/messagebus"
	"github.com/carolsimone/continuo/dependency-controller/service/uow"
	goredis "github.com/redis/go-redis/v9"
)

func main() {
	// Setup structured logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	logger.Info("Starting dependency-controller service")

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize lifecycle manager
	lifecycleManager := lifecycle.NewApplicationLifecycle(logger)
	lifecycleManager.SetupSignalHandlers(ctx, cancel)

	// ========================================================================
	// INITIALIZE DEPENDENCIES
	// ========================================================================

	// 1. PostgreSQL (for outbox table)
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
		Addr:     fmt.Sprintf("%s:%d", config.GetRedisHost(), config.GetRedisPort()),
		Password: config.GetRedisPassword(),
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

	// 3. gRPC client to state service
	stateClient, err := grpcclient.NewStateClient(config.GetStateServiceGRPCAddr(), logger)
	if err != nil {
		logger.Error("Failed to create state service client", "error", err)
		os.Exit(1)
	}
	logger.Info("State service gRPC client initialized")

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Closing state service gRPC client")
		return stateClient.Close()
	})

	// 4. gRPC client to graph service
	graphClient, err := grpcclient.NewGraphClient(config.GetGraphServiceGRPCAddr(), logger)
	if err != nil {
		logger.Error("Failed to create graph service client", "error", err)
		os.Exit(1)
	}
	logger.Info("Graph service gRPC client initialized")

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Closing graph service gRPC client")
		return graphClient.Close()
	})

	// ========================================================================
	// INITIALIZE REPOSITORIES
	// ========================================================================

	outboxRepo := postgres.NewOutboxRepository(pgDB, logger)
	publishedMsgRepo := postgres.NewPublishedMessagesRepository(pgDB, logger)

	// ========================================================================
	// INITIALIZE UNIT OF WORK & HANDLERS
	// ========================================================================

	unitOfWork := uow.NewPostgresUnitOfWork(pgDB, logger)

	// Create handler
	processHandler := handlers.NewProcessStatusHandler(
		unitOfWork,
		graphClient,
		stateClient,
		logger,
	)

	// Create command handlers map
	commandHandlers := map[string]messagebus.CommandHandler{
		"command.ProcessNodeStatus": func(ctx context.Context, cmd command.Command, messageID string) error {
			return processHandler.Handle(ctx, cmd.(command.ProcessNodeStatus), messageID)
		},
	}

	// Create MessageBus
	messageBus := messagebus.NewMessageBus(
		unitOfWork,
		commandHandlers,
		map[string][]messagebus.EventHandler{}, // No event handlers
		logger,
	)

	// ========================================================================
	// INITIALIZE REDIS CONSUMER & PRODUCER
	// ========================================================================

	// Create consumer for update.table:v1 stream
	consumer, err := redis.NewConsumer(
		redisClient,
		config.GetRedisConsumerStream(),
		config.GetRedisConsumerGroup(),
		messageBus,
		pgDB,
		logger,
	)
	if err != nil {
		logger.Error("Failed to create Redis consumer", "error", err)
		os.Exit(1)
	}

	// Create producer for query.model:v1 stream
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
		publishedMsgRepo,
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

	logger.Info("Dependency-controller service stopped")
}
