package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/liveness"
	pkgmessageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/adapters/http"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	statepublisher "github.com/carolsimone/continuo/state/adapters/publisher"
	"github.com/carolsimone/continuo/state/adapters/redis"
	"github.com/carolsimone/continuo/state/config"
	"github.com/carolsimone/continuo/state/database"
	grpcserver "github.com/carolsimone/continuo/state/internal/grpc"
	"github.com/carolsimone/continuo/state/internal/grpc/handlers"
	"github.com/carolsimone/continuo/state/internal/lifecycle"
	"github.com/carolsimone/continuo/state/internal/scheduler"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	ports "github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/uow"
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

	// Initialize lifecycle manager and liveness registry. The lifecycle manager
	// drives the ordered graceful shutdown (stop intake → drain → close infra);
	// the registry tracks background workers and feeds the /ready probe.
	lifecycleManager := lifecycle.NewApplicationLifecycle(logger)
	lifecycleManager.SetupSignalHandlers(cancel, cfg.ShutdownGrace)

	liveReg := liveness.NewRegistry()

	// runConsumer starts a tracked stream consumer. Its goroutine is tracked by
	// the lifecycle WaitGroup so shutdown drains in-flight handler invocations
	// before infra is closed. A non-nil return (a genuine exit rather than a
	// clean ctx-cancel stop) flips readiness so the unhealthy pod is restarted.
	runConsumer := func(name string, consumer *pkgredis.StreamConsumer) {
		liveReg.RegisterWorker(name)
		lifecycleManager.Go(func() {
			err := consumer.Start(ctx)
			liveReg.WorkerExited(name, err)
			if err != nil {
				logger.Error("Consumer exited", "consumer", name, "error", err)
			}
		})
	}

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

	// Cached dependency probes feed /ready. They run at most once per TTL so the
	// readiness endpoint stays cheap under Kubernetes probe traffic; a failed
	// ping flips readiness until the dependency recovers.
	liveReg.AddProbe("redis", 5*time.Second, func(ctx context.Context) error {
		return redisClient.Ping(ctx).Err()
	})
	liveReg.AddProbe("postgres", 5*time.Second, func(ctx context.Context) error {
		return db.PingContext(ctx)
	})

	// Start outbox processor backed by pkg/outbox. The publisher XADDs each
	// entry's JSONB payload to its stream. Nested non-scalar fields are
	// re-encoded to JSON strings so Redis receives only plain scalars.
	outboxPub := statepublisher.NewOutboxPublisher(redisClient, logger)
	outboxProc := pkgoutbox.NewProcessor(
		db,
		"state_outbox",
		outboxPub,
		nil, // no terminal-failure hook for state
		logger,
		pkgoutbox.ProcessorConfig{Tick: 500 * time.Millisecond, BatchSize: 100},
	)
	liveReg.RegisterWorker("outbox_processor")
	lifecycleManager.Go(func() {
		err := outboxProc.Run(ctx)
		if errors.Is(err, context.Canceled) {
			err = nil // clean stop on shutdown
		}
		liveReg.WorkerExited("outbox_processor", err)
		if err != nil {
			logger.Error("Outbox processor exited", "error", err)
		}
	})

	// Retention sweeper — keeps the two unbounded-growth tables in check:
	// processed state_outbox rows and terminal message_processing dedup rows
	// (the latter retains a full payload per consumed message). Both are pruned
	// past the retention window on the same timer using DB-clock cutoffs.
	mpPruner := pkgmessageprocessing.NewPruner(db, logger)
	retentionSweeper := pkgoutbox.NewRetentionSweeper(
		[]pkgoutbox.RetentionTarget{
			pkgoutbox.OutboxRetentionTarget(db, "state_outbox", logger),
			{
				Name:  "message_processing",
				Prune: mpPruner.DeleteTerminalOlderThan,
			},
		},
		pkgoutbox.RetentionConfig{
			Retention: time.Duration(cfg.RetentionDays) * 24 * time.Hour,
			Interval:  time.Duration(cfg.RetentionSweepIntervalMin) * time.Minute,
		},
		logger,
	)
	go retentionSweeper.Run(ctx)

	clk := ports.SystemClock{}

	// UoW factory shared by every stream binding below. Each invocation
	// returns a fresh PostgresUnitOfWork over the same low-level repos and
	// *sqlx.DB so concurrent message handlers do not share transaction state.
	// The UoW builds tx-bound aggregate adapters (Run, Catalog, Outbox,
	// TaskExecutions) per accessor call.
	uowFactory := func() uow.UnitOfWork {
		return postgres.NewPostgresUnitOfWork(db, schedulerRepo, taskRepo, taskExecutionRepo, catalogRepo, clk, logger)
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
	runConsumer("schedule_catalog", catalogConsumer)

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
	runConsumer("run_entries_dispatched", runEntriesDispatchedConsumer)

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
	runConsumer("run_entries_dispatch_failed", runEntriesDispatchFailedConsumer)

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
	runConsumer("task_status_updated", taskStatusUpdatedConsumer)

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
	runConsumer("task_execution_recorded", taskExecutionRecordedConsumer)

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
	rerunUC := svchandlers.NewTriggerRerunHandler(logger)
	rerunHandler := handlers.NewRerunHandler(rerunUC, uowFactory, logger)
	rebaseUC := svchandlers.NewTriggerRebaseHandler(logger)
	rebaseHandler := handlers.NewRebaseHandler(rebaseUC, uowFactory, logger)
	singleNodeRunUC := svchandlers.NewTriggerSingleNodeRunHandler(logger)
	singleNodeRunHandler := handlers.NewSingleNodeRunHandler(singleNodeRunUC, uowFactory, logger)
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

	// Start HTTP health server. /health is a liveness probe; /ready is backed by
	// the liveness registry so traffic stops when a consumer exits or a backing
	// store is unreachable.
	healthServer := http.NewServer(cfg.HealthPort, liveReg, logger)

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

	// Block until the graceful-shutdown sequence has fully completed. The signal
	// handler cancels ctx (stops intake), drains in-flight goroutines, then runs
	// the infra-close handlers; Done() closes only after that sequence finishes,
	// so there is no fixed sleep racing the shutdown.
	<-lifecycleManager.Done()
	logger.Info("Service stopped")
}
