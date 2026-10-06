package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/lifecycle"
	"github.com/carolsimone/continuo/pkg/liveness"
	pkgmessageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/adapters/http"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	statepublisher "github.com/carolsimone/continuo/state/adapters/publisher"
	"github.com/carolsimone/continuo/state/adapters/redis"
	"github.com/carolsimone/continuo/state/config"
	grpcserver "github.com/carolsimone/continuo/state/internal/grpc"
	"github.com/carolsimone/continuo/state/internal/grpc/handlers"
	"github.com/carolsimone/continuo/state/internal/scheduler"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	ports "github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/uow"
	goredis "github.com/redis/go-redis/v9"
)

// outboxHeartbeatStale is the liveness budget for the outbox processor's Run
// loop. An idle loop turns at least once per pkgoutbox.FallbackTick (5s), so 60s
// is comfortably above it: a wedged (not exited) processor trips within a
// minute, while an idle-but-live one never does.
const outboxHeartbeatStale = 60 * time.Second

// dbPool bounds the Postgres pool when DB_MAX_OPEN_CONNS / DB_MAX_IDLE_CONNS
// are unset: two connections per stream consumer (a handler's transaction and
// a read outside it), plus the outbox relay, background loops and request
// handlers.
var dbPool = pkgconfig.PoolConfig{MaxOpenConns: 20, MaxIdleConns: 10}

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
	if len(cfg.IgnoredPoolKeys) > 0 {
		logger.Warn("Ignoring Postgres pool settings state does not read; set DB_MAX_OPEN_CONNS and DB_MAX_IDLE_CONNS instead",
			"ignored", strings.Join(cfg.IgnoredPoolKeys, ", "))
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize lifecycle manager and liveness registry. The lifecycle manager
	// drives the ordered graceful shutdown (stop intake → drain → close infra);
	// the registry tracks background workers and feeds the /ready probe.
	lifecycleManager := lifecycle.NewApplicationLifecycle(logger)
	lifecycleManager.SetupSignalHandlers(cancel, cfg.ShutdownGrace)

	liveReg := liveness.NewRegistry()
	metricsReg := pkgmetrics.New(config.ServiceName)

	// runConsumer starts a tracked stream consumer. Its goroutine is tracked by
	// the lifecycle WaitGroup so shutdown drains in-flight handler invocations
	// before infra is closed. A non-nil return (a genuine exit rather than a
	// clean ctx-cancel stop) flips both readiness and liveness so the unhealthy
	// pod is restarted; a worker heartbeat probe also catches a consumer whose
	// read loop has gone wedged without exiting.
	runConsumer := func(name string, consumer *pkgredis.StreamConsumer) {
		consumer.SetService(config.ServiceName)
		consumer.SetObserver(metricsReg.Consumers())
		liveReg.RegisterWorker(name)
		liveReg.AddWorkerProbe(name+"_heartbeat", 10*time.Second, func(context.Context) error {
			return consumer.Healthy(consumer.HeartbeatBudget())
		})
		lifecycleManager.Go(func() {
			err := consumer.Start(ctx)
			liveReg.WorkerExited(name, err)
			if err != nil {
				logger.Error("Consumer exited", "consumer", name, "error", err)
			}
		})
	}

	// Start HTTP health server before any dependency is reachable. /health is a
	// plain process-up probe; /ready and /livez are both backed by the liveness
	// registry but answer different questions — /ready also fails on a
	// dependency outage (stops traffic), /livez fails only on a dead or wedged
	// consumer (restarts the pod). Serving /livez from the start keeps the
	// liveness probe answering while the boot waits for Redis; the startup gate
	// holds /ready false until every consumer and server below is running.
	startup := liveReg.AddStartupGate()
	healthServer := http.NewServer(cfg.HealthPort, liveReg, logger)
	go func() {
		if err := healthServer.Start(); err != nil {
			logger.Error("Health server error", "error", err)
		}
	}()

	// The Prometheus listener comes up with the health server so a dependency
	// outage stays observable. Serve runs in a plain goroutine; the lifecycle
	// manager stops it at shutdown.
	metricsServer, err := pkgmetrics.Listen(cfg.MetricsPort, metricsReg, logger)
	if err != nil {
		logger.Error("Failed to bind the metrics port", "port", cfg.MetricsPort, "error", err)
		os.Exit(1)
	}
	go func() {
		if err := metricsServer.Serve(); err != nil {
			logger.Error("Metrics server error", "error", err)
		}
	}()
	lifecycleManager.RegisterShutdownHandler(metricsServer.Shutdown)

	// Initialize PostgreSQL connection
	db, err := pkgdb.Open(ctx, cfg.Postgres, dbPool)
	if err != nil {
		logger.Error("Failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	logger.Info("PostgreSQL connection established")
	metricsReg.WatchDB(db.DB, cfg.Postgres.DB)

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

	// Redis is a separate workload that may still be starting (or its Service
	// not yet resolvable) when this process boots, so wait for it with backoff
	// instead of exiting on the first refused dial.
	err = pkgredis.WaitForRedis(ctx, redisClient, pkgredis.DefaultStartupPolicy(), logger)
	if errors.Is(err, context.Canceled) {
		logger.Info("Shutdown requested while waiting for Redis")
		os.Exit(0)
	}
	if err != nil {
		logger.Error("Failed to connect to Redis", "addr", cfg.Redis.Addr(), "error", err)
		os.Exit(1)
	}
	logger.Info("Redis connection established")
	metricsReg.WatchRedis(redisClient)

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
	// re-encoded to JSON strings so Redis receives only plain scalars. The
	// relay wakes on the notification a committed insert sends.
	outboxWaker, err := pkgoutbox.NewPostgresWaker(ctx, cfg.Postgres.DSN(), postgres.OutboxTable, logger)
	if errors.Is(err, context.Canceled) {
		logger.Info("Shutdown requested while waiting for the outbox listener")
		os.Exit(0)
	}
	if err != nil {
		logger.Error("Failed to listen for outbox notifications", "error", err)
		os.Exit(1)
	}
	// Closing the listener can wait on a connection attempt in progress, so
	// the handler stops waiting at the shutdown deadline.
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error { return outboxWaker.CloseContext(ctx) })
	outboxPub := statepublisher.NewOutboxPublisher(redisClient, logger)
	outboxProc := pkgoutbox.NewProcessor(
		db,
		postgres.OutboxTable,
		outboxPub,
		nil, // no terminal-failure hook for state
		logger,
		pkgoutbox.ProcessorConfig{Tick: pkgoutbox.FallbackTick, BatchSize: 100, Waker: outboxWaker, Observer: metricsReg.Outbox()},
	)
	metricsReg.WatchOutbox(outboxProc)
	liveReg.RegisterWorker("outbox_processor")
	liveReg.AddWorkerProbe("outbox_processor_heartbeat", 10*time.Second, func(context.Context) error {
		return outboxProc.Healthy(outboxHeartbeatStale)
	})
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
	// processed state_outbox rows and message_processing dedup rows (the latter
	// retains a full payload per consumed message). Processed outbox rows are
	// kept RETENTION_DAYS; dedup rows, whatever their state, are kept at least
	// 30 days (the replay horizon), longer when RETENTION_DAYS is longer. Both
	// are pruned on the same timer using DB-clock cutoffs.
	mpPruner := pkgmessageprocessing.NewPruner(db, postgres.OutboxTable, logger)
	retentionSweeper := pkgoutbox.NewRetentionSweeper(
		[]pkgoutbox.RetentionTarget{
			pkgoutbox.OutboxRetentionTarget(db, postgres.OutboxTable, logger),
			{
				Name:         "message_processing",
				Prune:        mpPruner.DeleteOlderThan,
				MinRetention: model.ReplayHorizon,
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

	// Every state consumer is the same shape: a domain handler wrapped by a
	// redis binding (parser + dedup + UoW transaction), driven by a
	// StreamConsumer on its (stream, group). They are declared in one table and
	// started uniformly via runConsumer below; consumer lifecycle is tied to
	// ctx — the lifecycle manager cancels ctx on shutdown, which exits each
	// Start cleanly. Stream/group names always come from pkg/streams constants.
	consumers := []struct {
		name    string
		stream  string
		group   string
		binding pkgredis.MessageHandler
	}{
		{
			name:    "schedule_catalog",
			stream:  streams.SchedulesLoadedV1,
			group:   streams.StateScheduleCatalog,
			binding: redis.NewScheduleCatalogBinding(uowFactory, svchandlers.NewScheduleCatalogHandler(logger), logger),
		},
		{
			name:    "run_entries_dispatched",
			stream:  streams.RunEntriesDispatchedV1,
			group:   streams.StateRunEntriesDispatched,
			binding: redis.NewRunEntriesDispatchedBinding(uowFactory, svchandlers.NewRunEntriesDispatchedHandler(logger), logger),
		},
		{
			name:    "run_entries_dispatch_failed",
			stream:  streams.RunEntriesDispatchFailedV1,
			group:   streams.StateRunEntriesDispatchFailed,
			binding: redis.NewRunEntriesDispatchFailedBinding(uowFactory, svchandlers.NewRunEntriesDispatchFailedHandler(logger), logger),
		},
		{
			name:    "task_status_updated",
			stream:  streams.TaskStatusUpdatedV1,
			group:   streams.StateTaskStatusUpdated,
			binding: redis.NewTaskStatusUpdatedBinding(uowFactory, svchandlers.NewTaskStatusUpdatedHandler(logger), logger),
		},
		{
			name:    "task_execution_recorded",
			stream:  streams.TaskExecutionRecordedV1,
			group:   streams.StateTaskExecutionRecorded,
			binding: redis.NewTaskExecutionRecordedBinding(uowFactory, svchandlers.NewTaskExecutionRecordedHandler(logger), logger),
		},
		{
			name:    "release_seeds_pending",
			stream:  streams.ReleaseSeedsPendingV1,
			group:   streams.StateReleaseSeedsPending,
			binding: redis.NewReleaseSeedsPendingBinding(uowFactory, svchandlers.NewPromotedSeedsHandler(logger), logger),
		},
	}
	for _, c := range consumers {
		runConsumer(c.name, pkgredis.NewStreamConsumer(redisClient, c.stream, c.group, c.binding, logger))
		logger.Info("Stream consumer initialized", "consumer", c.name)
	}

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

	// Register health server cleanup
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		return healthServer.Shutdown(ctx)
	})

	// Everything is initialised and serving: readiness may now turn true.
	startup.Done()

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
