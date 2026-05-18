package main

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/carolsimone/continuo/executor-controller/adapters/http"
	"github.com/carolsimone/continuo/executor-controller/adapters/k8s"
	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/executor-controller/adapters/redis"
	"github.com/carolsimone/continuo/executor-controller/config"
	"github.com/carolsimone/continuo/executor-controller/internal/lifecycle"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
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

	// 3. K8s client (in-cluster config)
	k8sClient, err := k8s.NewK8sClient(logger)
	if err != nil {
		logger.Error("Failed to create K8s client", "error", err)
		os.Exit(1)
	}
	logger.Info("K8s client initialized")

	// ========================================================================
	// INITIALIZE REPOSITORIES
	// ========================================================================

	outboxRepo := postgres.NewOutboxRepository(pgDB, logger)
	cancelledSchedulesRepo := postgres.NewCancelledSchedulesRepository(pgDB)

	// ========================================================================
	// INITIALIZE UOW FACTORY + HANDLERS + BINDINGS
	// ========================================================================

	// uowFactory returns a fresh PostgresUnitOfWork per inbound message so
	// concurrent handlers never share transaction state.
	uowFactory := func() uow.UnitOfWork {
		return uow.NewPostgresUnitOfWork(pgDB, logger)
	}

	queryHandler := handlers.NewQueryModelHandler(logger)
	retryHandler := handlers.NewRetryTaskHandler(logger)
	scheduleCancelledHandler := handlers.NewScheduleCancelledHandler(logger)

	queryBinding := redis.NewQueryModelBinding(uowFactory, queryHandler, logger)
	retryBinding := redis.NewRetryTaskBinding(uowFactory, retryHandler, logger)
	scheduleCancelledBinding := redis.NewScheduleCancelledBinding(
		uowFactory, scheduleCancelledHandler, logger)

	// ========================================================================
	// INITIALIZE REDIS PRODUCERS + CONSUMERS
	// ========================================================================

	publisher := redis.NewProducer(redisClient, logger)

	queryConsumer := pkgredis.NewStreamConsumer(
		redisClient, streams.QueryModelV1, streams.ExecutorQueryModel,
		queryBinding, logger)
	logger.Info("query.model consumer initialized",
		"stream", streams.QueryModelV1, "group", streams.ExecutorQueryModel)

	retryConsumer := pkgredis.NewStreamConsumer(
		redisClient, streams.RetryTaskV1, streams.ExecutorRetry,
		retryBinding, logger)
	logger.Info("retry.task consumer initialized",
		"stream", streams.RetryTaskV1, "group", streams.ExecutorRetry)

	scheduleCancelledConsumer := pkgredis.NewStreamConsumer(
		redisClient, streams.ScheduleCancelledV1, streams.ExecutorScheduleCancelled,
		scheduleCancelledBinding, logger)
	logger.Info("schedule.cancelled consumer initialized",
		"stream", streams.ScheduleCancelledV1, "group", streams.ExecutorScheduleCancelled)

	// ========================================================================
	// INITIALIZE OUTBOX PROCESSOR
	// ========================================================================

	outboxProcessor := handlers.NewOutboxProcessor(
		outboxRepo,
		k8sClient,
		publisher,
		streams.NodeDeployedV1,
		streams.TaskStatusUpdatedV1,
		streams.NodeUpdatedV1,
		cfg.K8sNamespace,
		logger,
	)

	go func() {
		if err := outboxProcessor.Run(ctx); err != nil {
			logger.Error("Outbox processor error", "error", err)
		}
	}()

	// ========================================================================
	// CANCELLED SCHEDULES TTL SWEEPER
	// ========================================================================

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
	// START REDIS CONSUMERS
	// ========================================================================

	logger.Info("Service initialization complete, starting consumers")

	go func() {
		if err := retryConsumer.Start(ctx); err != nil {
			logger.Error("retry.task consumer error", "error", err)
		}
	}()
	go func() {
		if err := scheduleCancelledConsumer.Start(ctx); err != nil {
			logger.Error("schedule.cancelled consumer error", "error", err)
		}
	}()

	// Block on the query.model consumer; all consumers stop when ctx is cancelled.
	if err := queryConsumer.Start(ctx); err != nil {
		logger.Error("query.model consumer error", "error", err)
		os.Exit(1)
	}

	logger.Info("Executor-controller service stopped")
}
