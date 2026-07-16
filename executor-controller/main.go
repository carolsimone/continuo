package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/carolsimone/continuo/executor-controller/adapters/commandcfg"
	"github.com/carolsimone/continuo/executor-controller/adapters/http"
	"github.com/carolsimone/continuo/executor-controller/adapters/k8s"
	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/executor-controller/adapters/publisher"
	"github.com/carolsimone/continuo/executor-controller/adapters/redis"
	"github.com/carolsimone/continuo/executor-controller/config"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	"github.com/carolsimone/continuo/executor-controller/internal/lifecycle"
	"github.com/carolsimone/continuo/executor-controller/service/deployer"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/routing"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
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

	// 1. PostgreSQL (for executor_outbox table)
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

	// 2. dbt-warehouse PostgreSQL client (used to drop candidate schemas after
	// validation completes; same host/port/credentials as the main PG client but
	// targets the dbt materialization database).
	dbtDB, err := postgres.NewPostgresClient(
		cfg.DBTWarehouse.Host,
		cfg.DBTWarehouse.Port,
		cfg.DBTWarehouse.DB,
		cfg.DBTWarehouse.User,
		cfg.DBTWarehouse.Password,
		logger,
	)
	if err != nil {
		logger.Error("Failed to connect to dbt warehouse", "error", err)
		os.Exit(1)
	}
	logger.Info("dbt-warehouse client initialized")

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Closing dbt-warehouse connection")
		return dbtDB.Close()
	})

	candidateSchemaCleaner := postgres.NewCandidateSchemaCleaner(dbtDB, logger)
	candidateSchemaCreator := postgres.NewCandidateSchemaCreator(dbtDB, logger)

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

	// 3. K8s client (in-cluster config)
	// dbt command dialect: resolved per service at Job-build time. A missing
	// file means built-in plain-dbt commands; an invalid file is fatal so a
	// config typo surfaces at boot, never mid-release.
	cmdResolver, err := commandcfg.Load(os.Getenv("DBT_COMMANDS_CONFIG_PATH"), logger)
	if err != nil {
		logger.Error("Invalid dbt commands config", "error", err)
		os.Exit(1)
	}
	k8sClient, err := k8s.NewK8sClient(logger, cmdResolver)
	if err != nil {
		logger.Error("Failed to create K8s client", "error", err)
		os.Exit(1)
	}
	logger.Info("K8s client initialized")

	// ========================================================================
	// INITIALIZE REPOSITORIES
	// ========================================================================

	cancelledSchedulesRepo := postgres.NewCancelledSchedulesRepository(pgDB)

	// ========================================================================
	// INITIALIZE UOW FACTORY + HANDLERS + BINDINGS
	// ========================================================================

	// uowFactory returns a fresh UnitOfWork per inbound message so
	// concurrent handlers never share transaction state.
	uowFactory := func() uow.UnitOfWork {
		return postgres.NewUnitOfWork(pgDB, logger)
	}

	// executionRouting decides, per production record, whether the record gets
	// its own Kubernetes Job or waits to be claimed by a worker pool.
	executionRouting := routing.NewPolicy(cfg.ExecutionMode, cfg.ExecutionModeOverrides)

	queryHandler := handlers.NewQueryModelHandler(executionRouting, logger)
	retryHandler := handlers.NewRetryTaskHandler(executionRouting, logger)
	scheduleCancelledHandler := handlers.NewScheduleCancelledHandler(logger)
	validationReqHandler := handlers.NewValidationRequestedHandler(logger)
	validationNodeHandler := handlers.NewValidationNodeCompletedHandler(logger)
	seedBuildReqHandler := handlers.NewSeedBuildRequestedHandler(logger)
	seedBuildNodeHandler := handlers.NewSeedBuildNodeCompletedHandler(logger)
	compileReqHandler := handlers.NewCompileRequestedHandler(logger)
	compileNodeHandler := handlers.NewCompileNodeCompletedHandler(logger)
	jobTerminalHandler := handlers.NewJobTerminalHandler(logger)

	queryBinding := redis.NewQueryModelBinding(uowFactory, queryHandler, logger)
	retryBinding := redis.NewRetryTaskBinding(uowFactory, retryHandler, logger)
	scheduleCancelledBinding := redis.NewScheduleCancelledBinding(
		uowFactory, scheduleCancelledHandler, logger)
	validationReqBinding := redis.NewValidationRequestedBinding(
		uowFactory, validationReqHandler, candidateSchemaCreator, logger)
	validationNodeBinding := redis.NewValidationNodeCompletedBinding(
		uowFactory, validationNodeHandler, logger)
	seedBuildReqBinding := redis.NewSeedBuildRequestedBinding(
		uowFactory, seedBuildReqHandler, candidateSchemaCreator, logger)
	seedBuildNodeBinding := redis.NewSeedBuildNodeCompletedBinding(
		uowFactory, seedBuildNodeHandler, logger)
	compileReqBinding := redis.NewCompileRequestedBinding(
		uowFactory, compileReqHandler, logger)
	compileNodeBinding := redis.NewCompileNodeCompletedBinding(
		uowFactory, compileNodeHandler, logger)
	jobTerminalBinding := redis.NewJobTerminalBinding(
		uowFactory, jobTerminalHandler, logger)
	validationCompletedTeardownBinding := redis.NewValidationCompletedTeardownBinding(
		candidateSchemaCleaner, logger)
	releaseRejectedTeardownBinding := redis.NewReleaseRejectedTeardownBinding(
		candidateSchemaCleaner, logger)
	releasePromotedTeardownBinding := redis.NewReleasePromotedTeardownBinding(
		candidateSchemaCleaner, logger)

	// ========================================================================
	// INITIALIZE REDIS PRODUCERS + CONSUMERS
	// ========================================================================

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

	validationReqConsumer := pkgredis.NewStreamConsumer(
		redisClient, streams.ValidationRequestedV1, streams.ExecutorValidationRequested,
		validationReqBinding, logger)
	logger.Info("validation.requested consumer initialized",
		"stream", streams.ValidationRequestedV1, "group", streams.ExecutorValidationRequested)

	validationNodeConsumer := pkgredis.NewStreamConsumer(
		redisClient, streams.ValidationNodeCompletedV1, streams.ExecutorValidationNodeCompleted,
		validationNodeBinding, logger)
	logger.Info("validation.node.completed consumer initialized",
		"stream", streams.ValidationNodeCompletedV1, "group", streams.ExecutorValidationNodeCompleted)

	seedBuildReqConsumer := pkgredis.NewStreamConsumer(
		redisClient, streams.SeedBuildRequestedV1, streams.ExecutorSeedBuildRequested,
		seedBuildReqBinding, logger)
	logger.Info("seed.build.requested consumer initialized",
		"stream", streams.SeedBuildRequestedV1, "group", streams.ExecutorSeedBuildRequested)

	seedBuildNodeConsumer := pkgredis.NewStreamConsumer(
		redisClient, streams.SeedBuildNodeCompletedV1, streams.ExecutorSeedBuildNodeCompleted,
		seedBuildNodeBinding, logger)
	logger.Info("seed.build.node.completed consumer initialized",
		"stream", streams.SeedBuildNodeCompletedV1, "group", streams.ExecutorSeedBuildNodeCompleted)

	compileReqConsumer := pkgredis.NewStreamConsumer(
		redisClient, streams.CompileRequestedV1, streams.ExecutorCompileRequested,
		compileReqBinding, logger)
	logger.Info("compile.requested consumer initialized",
		"stream", streams.CompileRequestedV1, "group", streams.ExecutorCompileRequested)

	compileNodeConsumer := pkgredis.NewStreamConsumer(
		redisClient, streams.CompileNodeCompletedV1, streams.ExecutorCompileNodeCompleted,
		compileNodeBinding, logger)
	logger.Info("compile.node.completed consumer initialized",
		"stream", streams.CompileNodeCompletedV1, "group", streams.ExecutorCompileNodeCompleted)

	jobTerminalConsumer := pkgredis.NewStreamConsumer(
		redisClient, streams.ExecutorJobTerminalV1, streams.ExecutorJobTerminal,
		jobTerminalBinding, logger)
	logger.Info("executor.job.terminal consumer initialized",
		"stream", streams.ExecutorJobTerminalV1, "group", streams.ExecutorJobTerminal)

	validationCompletedTeardownConsumer := pkgredis.NewStreamConsumer(
		redisClient, streams.ValidationCompletedV1, streams.ExecutorValidationCompleted,
		validationCompletedTeardownBinding, logger)
	logger.Info("validation.completed teardown consumer initialized",
		"stream", streams.ValidationCompletedV1, "group", streams.ExecutorValidationCompleted)

	releaseRejectedTeardownConsumer := pkgredis.NewStreamConsumer(
		redisClient, streams.ReleaseRejectedV1, streams.ExecutorReleaseRejected,
		releaseRejectedTeardownBinding, logger)
	logger.Info("release.rejected teardown consumer initialized",
		"stream", streams.ReleaseRejectedV1, "group", streams.ExecutorReleaseRejected)

	releasePromotedTeardownConsumer := pkgredis.NewStreamConsumer(
		redisClient, streams.ReleasePromotedV1, streams.ExecutorReleasePromoted,
		releasePromotedTeardownBinding, logger)
	logger.Info("release.promoted teardown consumer initialized",
		"stream", streams.ReleasePromotedV1, "group", streams.ExecutorReleasePromoted)

	// ========================================================================
	// INITIALIZE OUTBOX PROCESSOR
	// ========================================================================

	outboxPub := publisher.NewOutboxPublisher(redisClient, logger)
	outboxProcessor := pkgoutbox.NewProcessor(
		pgDB,
		"executor_outbox",
		outboxPub,
		nil, // terminal failures are ordinary outbox rows written by the dispatcher
		logger,
		// PerAggregateFIFO so a task's RUNNING announcement publishes before its
		// node_deployed row, since they share the task aggregate_id.
		pkgoutbox.ProcessorConfig{Tick: 5 * time.Second, BatchSize: 100, PerAggregateFIFO: true},
	)

	go func() {
		if err := outboxProcessor.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("Outbox processor exited", "error", err)
		}
	}()

	k8sDeployer := k8s.NewDeployer(k8sClient, cfg.K8sNamespace)
	deployDispatcher := deployer.NewDispatcher(
		pgDB, k8sDeployer,
		func(exec pkgoutbox.Executor) repository.DeploymentRepository {
			return postgres.NewDeploymentsRepository(exec, logger)
		},
		func(exec pkgoutbox.Executor) repository.ValidationAggregateRepository {
			return postgres.NewValidationAggregateRepository(exec)
		},
		cfg.MaxConcurrentExecutions, logger,
		// BatchSize is left unset so it resolves to the configured execution
		// cap: a batch can never start more work than the cap allows.
		deployer.DispatcherConfig{Tick: 5 * time.Second},
	)

	go func() {
		if err := deployDispatcher.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("Deploy dispatcher exited", "error", err)
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

	// All consumers run as goroutines so no single stream blocks the main
	// goroutine. Lifecycle is tied to ctx; the lifecycle manager cancels ctx
	// on shutdown, which exits each Start cleanly.
	go func() {
		if err := queryConsumer.Start(ctx); err != nil {
			logger.Error("query.model consumer error", "error", err)
		}
	}()
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
	go func() {
		if err := validationReqConsumer.Start(ctx); err != nil {
			logger.Error("validation.requested consumer error", "error", err)
		}
	}()
	go func() {
		if err := validationNodeConsumer.Start(ctx); err != nil {
			logger.Error("validation.node.completed consumer error", "error", err)
		}
	}()
	go func() {
		if err := seedBuildReqConsumer.Start(ctx); err != nil {
			logger.Error("seed.build.requested consumer error", "error", err)
		}
	}()
	go func() {
		if err := seedBuildNodeConsumer.Start(ctx); err != nil {
			logger.Error("seed.build.node.completed consumer error", "error", err)
		}
	}()
	go func() {
		if err := compileReqConsumer.Start(ctx); err != nil {
			logger.Error("compile.requested consumer error", "error", err)
		}
	}()
	go func() {
		if err := compileNodeConsumer.Start(ctx); err != nil {
			logger.Error("compile.node.completed consumer error", "error", err)
		}
	}()
	go func() {
		if err := jobTerminalConsumer.Start(ctx); err != nil {
			logger.Error("executor.job.terminal consumer error", "error", err)
		}
	}()
	go func() {
		if err := validationCompletedTeardownConsumer.Start(ctx); err != nil {
			logger.Error("validation.completed teardown consumer error", "error", err)
		}
	}()
	go func() {
		if err := releaseRejectedTeardownConsumer.Start(ctx); err != nil {
			logger.Error("release.rejected teardown consumer error", "error", err)
		}
	}()
	go func() {
		if err := releasePromotedTeardownConsumer.Start(ctx); err != nil {
			logger.Error("release.promoted teardown consumer error", "error", err)
		}
	}()

	// Block until shutdown is requested. Each consumer's Start returns
	// when ctx is cancelled.
	<-ctx.Done()

	logger.Info("Executor-controller service stopped")
}
