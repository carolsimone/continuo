package main

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"github.com/carolsimone/continuo/startup-controller/adapters/grpc"
	"github.com/carolsimone/continuo/startup-controller/adapters/http"
	"github.com/carolsimone/continuo/startup-controller/adapters/postgres"
	"github.com/carolsimone/continuo/startup-controller/adapters/redis"
	"github.com/carolsimone/continuo/startup-controller/config"
	"github.com/carolsimone/continuo/startup-controller/domain/command"
	"github.com/carolsimone/continuo/startup-controller/internal/lifecycle"
	"github.com/carolsimone/continuo/startup-controller/service/handlers"
	"github.com/carolsimone/continuo/startup-controller/service/messagebus"
	"github.com/carolsimone/continuo/startup-controller/service/uow"
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

	logger.Info("Starting startup-controller service")

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

	// 2. Redis client
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

	// 3. gRPC client to state service
	stateClient, err := grpc.NewStateClient(cfg.StateGRPCAddr, logger)
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

	// Create handlers
	initHandler := handlers.NewInitializeSchedulerHandler(
		unitOfWork,
		stateClient,
		cfg.InitializeRunProducerStream,
		logger,
	)

	rerunHandler := handlers.NewRerunHandler(
		unitOfWork,
		stateClient,
		cfg.InitializeRunProducerStream,
		logger,
	)

	runInitializedHandler := handlers.NewHandleRunInitializedHandler(
		unitOfWork,
		stateClient,
		logger,
	)

	rerunReadyHandler := handlers.NewHandleRerunReadyHandler(
		unitOfWork,
		stateClient,
		logger,
	)

	// Create command handlers map
	commandHandlers := map[string]messagebus.CommandHandler{
		"command.InitializeScheduler": func(ctx context.Context, cmd command.Command) error {
			return initHandler.Handle(ctx, cmd.(command.InitializeScheduler))
		},
		"command.RerunNode": func(ctx context.Context, cmd command.Command) error {
			return rerunHandler.Handle(ctx, cmd.(command.RerunNode))
		},
		"command.RunInitialized": func(ctx context.Context, cmd command.Command) error {
			return runInitializedHandler.Handle(ctx, cmd.(command.RunInitialized))
		},
		"command.RerunReady": func(ctx context.Context, cmd command.Command) error {
			return rerunReadyHandler.Handle(ctx, cmd.(command.RerunReady))
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
	// INITIALIZE REDIS CONSUMERS & PRODUCERS
	// ========================================================================

	// Create consumer for scheduler.started:v1 stream
	consumer, err := redis.NewConsumer(
		redisClient,
		cfg.RedisConsumerStream,
		cfg.RedisConsumerGroup,
		messageBus,
		logger,
	)
	if err != nil {
		logger.Error("Failed to create Redis consumer", "error", err)
		os.Exit(1)
	}

	// Create consumer for rerun:v1 stream
	rerunConsumer, err := redis.NewRerunConsumer(
		redisClient,
		"rerun:v1",
		"startup-controller-rerun",
		messageBus,
		logger,
	)
	if err != nil {
		logger.Error("Failed to create rerun consumer", "error", err)
		os.Exit(1)
	}

	// Create consumer for run.initialized:v1 stream
	runInitializedConsumer, err := redis.NewRunInitializedConsumer(
		redisClient,
		cfg.RunInitializedConsumerStream,
		cfg.RunInitializedConsumerGroup,
		messageBus,
		logger,
	)
	if err != nil {
		logger.Error("Failed to create run.initialized consumer", "error", err)
		os.Exit(1)
	}

	// Create consumer for rerun.ready:v1 stream
	rerunReadyConsumer, err := redis.NewRerunReadyConsumer(
		redisClient,
		cfg.RerunReadyConsumerStream,
		cfg.RerunReadyConsumerGroup,
		messageBus,
		logger,
	)
	if err != nil {
		logger.Error("Failed to create rerun.ready consumer", "error", err)
		os.Exit(1)
	}

	// Create producers keyed by stream name
	queryModelProducer := redis.NewProducer(
		redisClient,
		cfg.RedisProducerStream,
		logger,
	)

	initializeRunProducer := redis.NewProducer(
		redisClient,
		cfg.InitializeRunProducerStream,
		logger,
	)

	producers := map[string]*redis.Producer{
		cfg.RedisProducerStream:         queryModelProducer,
		cfg.InitializeRunProducerStream: initializeRunProducer,
	}

	// ========================================================================
	// INITIALIZE OUTBOX PROCESSOR
	// ========================================================================

	outboxProcessor := handlers.NewOutboxProcessor(
		outboxRepo,
		producers,
		logger,
	)

	// Start outbox processor in background goroutine
	go func() {
		if err := outboxProcessor.Run(ctx); err != nil {
			logger.Error("Outbox processor error", "error", err)
		}
	}()

	// ========================================================================
	// CRASH RECOVERY
	// ========================================================================

	// Reset any 'in_progress' initializations to 'pending'
	count, err := stateClient.ResetInProgressInitializations(ctx)
	if err != nil {
		logger.Error("Failed to reset in_progress initializations", "error", err)
		// Don't exit - this is not critical
	}
	if count > 0 {
		logger.Warn("Reset in_progress initializations on startup",
			"count", count,
			"reason", "crash recovery",
		)
	}

	// ========================================================================
	// START HTTP HEALTH CHECK SERVER
	// ========================================================================

	healthServer := http.NewHealthServer(cfg.HTTPPort, logger)

	go func() {
		if err := healthServer.Start(); err != nil {
			logger.Error("Health server error", "error", err)
		}
	}()

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		return healthServer.Shutdown(ctx)
	})

	// ========================================================================
	// START REDIS CONSUMERS (BLOCKING)
	// ========================================================================

	logger.Info("Service initialization complete, starting consumers")

	// Start additional consumers in background
	go func() {
		if err := rerunConsumer.Start(ctx); err != nil {
			logger.Error("Rerun consumer error", "error", err)
		}
	}()

	go func() {
		if err := runInitializedConsumer.Start(ctx); err != nil {
			logger.Error("run.initialized consumer error", "error", err)
		}
	}()

	go func() {
		if err := rerunReadyConsumer.Start(ctx); err != nil {
			logger.Error("rerun.ready consumer error", "error", err)
		}
	}()

	if err := consumer.Start(ctx); err != nil {
		logger.Error("Consumer error", "error", err)
		os.Exit(1)
	}

	logger.Info("Startup-controller service stopped")
}
