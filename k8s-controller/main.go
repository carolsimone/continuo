package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/carolsimone/continuo/k8s-controller/adapters/http"
	"github.com/carolsimone/continuo/k8s-controller/adapters/k8s"
	"github.com/carolsimone/continuo/k8s-controller/adapters/postgres"
	k8spub "github.com/carolsimone/continuo/k8s-controller/adapters/publisher"
	"github.com/carolsimone/continuo/k8s-controller/adapters/redis"
	s3adapter "github.com/carolsimone/continuo/k8s-controller/adapters/s3"
	"github.com/carolsimone/continuo/k8s-controller/config"
	"github.com/carolsimone/continuo/k8s-controller/internal/lifecycle"
	"github.com/carolsimone/continuo/k8s-controller/service/handlers"
	"github.com/carolsimone/continuo/k8s-controller/service/uow"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/liveness"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
)

// K8sStatusChecker is a consumer-defined port in service/handlers, so the
// adapter cannot carry the implements-assertion without an adapter→application
// import. Assert it here at the composition root, where both packages are
// already wired, to get an explicit compile-time check.
var _ handlers.K8sStatusChecker = (*k8s.K8sClient)(nil)

// consumerHeartbeatStale is how long a stream consumer's read loop may go
// without an iteration before the readiness probe considers it stalled — not
// "erroring while it retries" (that path advances the heartbeat every
// iteration; see pkg/redis.StreamConsumer.Healthy) but genuinely wedged:
// blocked in a call that never returns, or a goroutine that exited some way
// other than Start's normal ctx.Done() path. Set well above the loop's normal
// cadence (an iteration happens at least every ~1-4s even mid-outage) so a
// slow-but-alive loop is never flagged.
const consumerHeartbeatStale = 30 * time.Second

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

	// Liveness registry feeding /ready (deploy config points BOTH the
	// readiness AND liveness Kubernetes probes at it — see
	// deploy/continuo/values.yaml). Tracks background workers (consumers,
	// outbox processor, stuck-entry resolver) plus cached dependency probes,
	// so a wedged or exited consumer restarts the pod instead of leaving it
	// at 1/1 Running with a dead background loop nothing else can see.
	liveReg := liveness.NewRegistry()

	// runConsumer starts a tracked stream consumer: RegisterWorker before
	// launch so a missing worker is observable from the first probe,
	// WorkerExited when Start returns (a non-nil error is a genuine unhandled
	// exit — Start's own retry loop already absorbs transient Redis errors,
	// so this only fires on the very first ensureConsumerGroup startup call
	// or when ctx.Done() has already been closed), and a heartbeat probe so a
	// wedged-but-not-exited loop is caught too.
	runConsumer := func(name string, consumer *pkgredis.StreamConsumer) {
		liveReg.RegisterWorker(name)
		liveReg.AddProbe(name+"_heartbeat", 10*time.Second, func(context.Context) error {
			return consumer.Healthy(consumerHeartbeatStale)
		})
	}

	// Step 4: Initialize Redis client
	redisClient := goredis.NewClient(&goredis.Options{
		Addr:     cfg.Redis.Addr(),
		Password: cfg.Redis.Password,
	})
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Closing Redis connection")
		return redisClient.Close()
	})
	liveReg.AddProbe("redis", 5*time.Second, func(ctx context.Context) error {
		return redisClient.Ping(ctx).Err()
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
	liveReg.AddProbe("postgres", 5*time.Second, func(ctx context.Context) error {
		return pgDB.PingContext(ctx)
	})

	logger.Info("PostgreSQL repositories initialized")

	// Step 8: Initialize K8s client
	k8sClient, err := k8s.NewK8sClient(logger)
	if err != nil {
		logger.Error("Failed to initialize K8s client", "error", err)
		os.Exit(1)
	}

	logger.Info("K8s client initialized")

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

	// Step 10b: Initialize check status handler.
	handlerConfig := &handlers.HandlerConfig{
		K8sNamespace:          cfg.K8sNamespace,
		CheckDelaySeconds:     cfg.K8sCheckDelaySeconds,
		ErrorMessageMaxLen:    cfg.ErrorMessageMaxLength,
		LogTailLines:          int64(cfg.LogTailLines),
		DefaultTaskMaxRetries: cfg.DefaultTaskMaxRetries,
	}

	// UnitOfWork factory — a fresh transaction-scoped UoW per message. Creating
	// it per message keeps concurrent handler invocations isolated.
	uowFactory := func() uow.UnitOfWork {
		return postgres.NewPostgresUnitOfWork(pgDB, logger)
	}

	checkStatusHandler := handlers.NewCheckStatusHandler(k8sClient, s3Client, handlerConfig, cancelledSchedulesRepo, logger)

	deployedConsumer := pkgredis.NewStreamConsumer(
		redisClient,
		streams.NodeDeployedV1,
		streams.K8sDeployed,
		redis.NewNodeDeployedBinding(uowFactory, checkStatusHandler, logger),
		logger,
	)
	checkConsumer := pkgredis.NewStreamConsumer(
		redisClient,
		streams.CheckK8sV1,
		streams.K8sCheckStatus,
		redis.NewCheckK8sBinding(redisClient, uowFactory, checkStatusHandler, logger),
		logger,
	)

	logger.Info("Redis consumers initialized",
		"deployed_stream", streams.NodeDeployedV1,
		"deployed_group", streams.K8sDeployed,
		"check_stream", streams.CheckK8sV1,
		"check_group", streams.K8sCheckStatus,
	)

	// Step 14: Initialize and start the canonical outbox processor (background).
	// The OutboxPublisher routes each k8s_outbox row to its typed Redis stream.
	outboxPub := k8spub.NewOutboxPublisher(redisClient, logger)
	outboxProc := pkgoutbox.NewProcessor(
		pgDB,
		"k8s_outbox",
		outboxPub,
		nil, // no terminal-failure hook for k8s
		logger,
		pkgoutbox.ProcessorConfig{Tick: time.Second, BatchSize: 100},
	)

	liveReg.RegisterWorker("outbox_processor")
	go func() {
		logger.Info("Starting outbox processor")
		err := outboxProc.Run(ctx)
		if errors.Is(err, context.Canceled) {
			err = nil // clean stop on shutdown
		}
		liveReg.WorkerExited("outbox_processor", err)
		if err != nil {
			logger.Error("Outbox processor stopped with error", "error", err)
		}
	}()

	logger.Info("Outbox processor started")

	// Step 15: Initialize and start StuckEntryResolver (background).
	// Uses a dedicated repository against k8s_outbox for stuck-entry remediation.
	stuckRepo := postgres.NewK8sOutboxStuckRepository(pgDB, logger)
	resolverConfig := &handlers.ResolverConfig{
		CheckIntervalSeconds:  cfg.Resolver.CheckIntervalSeconds,
		StuckThresholdSeconds: cfg.Resolver.StuckThresholdSeconds,
		BatchSize:             cfg.Resolver.BatchSize,
		MaxResolveAttempts:    cfg.Resolver.MaxAttempts,
	}

	stuckEntryResolver := handlers.NewStuckEntryResolver(
		stuckRepo,
		resolverConfig,
		logger,
	)

	liveReg.RegisterWorker("stuck_entry_resolver")
	go func() {
		logger.Info("Starting stuck entry resolver")
		err := stuckEntryResolver.Run(ctx)
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		liveReg.WorkerExited("stuck_entry_resolver", err)
		if err != nil {
			logger.Error("Stuck entry resolver stopped with error", "error", err)
		}
	}()

	logger.Info("Stuck entry resolver started",
		"check_interval", resolverConfig.CheckIntervalSeconds,
		"stuck_threshold", resolverConfig.StuckThresholdSeconds,
	)

	// Step 16: Start HTTP Health Server (background)
	healthServer := http.NewHealthServer(cfg.HTTPPort, liveReg, logger)
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

	scheduleCancelledConsumer := pkgredis.NewStreamConsumer(
		redisClient,
		streams.ScheduleCancelledV1,
		streams.K8sScheduleCancelled,
		redis.NewScheduleCancelledBinding(cancelledSchedulesRepo, logger),
		logger,
	)
	runConsumer("schedule_cancelled", scheduleCancelledConsumer)
	go func() {
		err := scheduleCancelledConsumer.Start(ctx)
		liveReg.WorkerExited("schedule_cancelled", err)
		if err != nil {
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
	runConsumer("node_deployed", deployedConsumer)
	go func() {
		logger.Info("Starting deployed consumer")
		err := deployedConsumer.Start(ctx)
		liveReg.WorkerExited("node_deployed", err)
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("Deployed consumer stopped with error", "error", err)
		}
	}()

	runConsumer("check_k8s", checkConsumer)
	logger.Info("Starting check consumer (main loop)")
	err = checkConsumer.Start(ctx)
	liveReg.WorkerExited("check_k8s", err)
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("Check consumer stopped with error", "error", err)
		os.Exit(1)
	}

	logger.Info("k8s-controller service stopped")
}
