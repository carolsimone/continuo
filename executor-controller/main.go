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
	"github.com/carolsimone/continuo/executor-controller/service/deployer"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/lifecycle"
	"github.com/carolsimone/continuo/pkg/liveness"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
)

// consumerHandlerTimeout bounds each message handler invocation with a context
// deadline, so a genuinely-hung handler eventually returns control to the read
// loop. Most handlers do short DB writes and K8s job dispatch (completion
// arrives as a separate event), so 60s far exceeds any legitimate invocation
// while still bounding a wedge. The candidate-schema handlers are the
// exception — see schemaOpHandlerTimeout.
const consumerHandlerTimeout = 60 * time.Second

// schemaOpHandlerTimeout is the handler budget for consumers that block on a
// candidate-schema engine-image Job (validation/seed-build ensure, teardown
// drop): the k8s adapter waits up to k8s.SchemaOpJobTimeout for the Job to
// terminate, so the message deadline must sit above that wait — under it, a
// cold image pull or slow DDL would cancel the handler and churn retries while
// the Job keeps running.
const schemaOpHandlerTimeout = k8s.SchemaOpJobTimeout + time.Minute

// schemaOpHeartbeatStale mirrors consumerHeartbeatStale for the schema-op
// consumers: it MUST exceed schemaOpHandlerTimeout plus a margin so a handler
// legitimately waiting on a Job never trips liveness.
const schemaOpHeartbeatStale = schemaOpHandlerTimeout + 2*time.Minute

// consumerHeartbeatStale is the liveness heartbeat budget: how long a consumer's
// read loop may make no progress before the liveness probe restarts the pod. It
// MUST exceed consumerHandlerTimeout plus a margin so a legitimately in-flight
// handler never trips liveness (the heartbeat advances per handler attempt; see
// pkg/redis.StreamConsumer.safeInvoke), while a true wedge — a handler that
// ignores its deadline, or a goroutine that stopped iterating — still trips
// within budget.
const consumerHeartbeatStale = 3 * time.Minute

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
	lifecycleManager.SetupSignalHandlers(cancel, cfg.ShutdownGrace)

	// Health registry feeding /ready (readiness) and /livez (liveness) — deploy
	// config points the two Kubernetes probes at those DIFFERENT paths (see
	// deploy/continuo/values.yaml). Worker registrations and consumer heartbeats
	// feed both checks (a dead/wedged consumer restarts the pod); dependency
	// probes (Redis/Postgres/dbt-warehouse) feed readiness ONLY (a backing-store
	// outage stops traffic but must not restart a pod whose consumers are already
	// retrying).
	liveReg := liveness.NewRegistry()

	// runConsumer starts a tracked stream consumer: a bounded handler deadline
	// so a hung handler eventually returns; RegisterWorker before launch so a
	// missing worker is observable; WorkerExited when Start returns (which now
	// happens on a permanent bootstrap error too, not only clean shutdown); and
	// a worker heartbeat probe so a wedged-but-not-exited loop is caught.
	startConsumer := func(name string, consumer *pkgredis.StreamConsumer, handlerTimeout, heartbeatStale time.Duration) {
		consumer.SetHandlerTimeout(handlerTimeout)
		liveReg.RegisterWorker(name)
		liveReg.AddWorkerProbe(name+"_heartbeat", 10*time.Second, func(context.Context) error {
			return consumer.Healthy(heartbeatStale)
		})
		lifecycleManager.Go(func() {
			err := consumer.Start(ctx)
			liveReg.WorkerExited(name, err)
			if err != nil {
				logger.Error("Consumer exited", "consumer", name, "error", err)
			}
		})
	}
	runConsumer := func(name string, consumer *pkgredis.StreamConsumer) {
		startConsumer(name, consumer, consumerHandlerTimeout, consumerHeartbeatStale)
	}
	// runSchemaOpConsumer starts a consumer whose handler blocks on a
	// candidate-schema engine-image Job, with the larger budget that wait needs.
	runSchemaOpConsumer := func(name string, consumer *pkgredis.StreamConsumer) {
		startConsumer(name, consumer, schemaOpHandlerTimeout, schemaOpHeartbeatStale)
	}

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
	liveReg.AddProbe("postgres", 5*time.Second, func(ctx context.Context) error {
		return pgDB.PingContext(ctx)
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
	liveReg.AddProbe("redis", 5*time.Second, func(ctx context.Context) error {
		return redisClient.Ping(ctx).Err()
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

	// Candidate-schema lifecycle: the executor never connects to the warehouse. It
	// schedules a one-shot engine-image Job (harness ensure_schema/drop_schema op) and
	// blocks on the result, so the engine adapter baked into VALIDATION_IMAGE owns the
	// DDL dialect and the warehouse credentials stay on the Job, not the control plane.
	candidateSchemaCreator := k8s.NewCandidateSchemaCreator(k8sClient, cfg.K8sNamespace, logger)
	candidateSchemaCleaner := k8s.NewCandidateSchemaCleaner(k8sClient, cfg.K8sNamespace, logger)

	// ========================================================================
	// INITIALIZE REPOSITORIES
	// ========================================================================

	cancelledSchedulesRepo := postgres.NewCancelledSchedulesRepository(pgDB)

	// ========================================================================
	// INITIALIZE UOW FACTORY + HANDLERS + BINDINGS
	// ========================================================================

	// uowFactory returns a fresh PostgresUnitOfWork per inbound message so
	// concurrent handlers never share transaction state.
	uowFactory := func() uow.UnitOfWork {
		return postgres.NewPostgresUnitOfWork(pgDB, logger)
	}

	queryHandler := handlers.NewQueryModelHandler(logger)
	retryHandler := handlers.NewRetryTaskHandler(logger)
	scheduleCancelledHandler := handlers.NewScheduleCancelledHandler(logger)
	validationReqHandler := handlers.NewValidationRequestedHandler(logger)
	validationNodeHandler := handlers.NewValidationNodeCompletedHandler(logger)
	seedBuildReqHandler := handlers.NewSeedBuildRequestedHandler(logger)
	seedBuildNodeHandler := handlers.NewSeedBuildNodeCompletedHandler(logger)
	compileReqHandler := handlers.NewCompileRequestedHandler(logger)
	compileNodeHandler := handlers.NewCompileNodeCompletedHandler(logger)

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
	validationResultTeardownBinding := redis.NewValidationResultTeardownBinding(
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

	// schemaOpReclaim keeps the PEL sweep from stealing a message whose handler is
	// legitimately blocked on a schema-op Job: with the default 30s MinIdle a peer
	// replica would re-claim (and double-drive) the message mid-wait, so entries on
	// these streams become claimable only after the full schema-op budget has passed.
	schemaOpReclaim := pkgredis.WithReclaimMinIdle(schemaOpHandlerTimeout + time.Minute)

	validationReqConsumer := pkgredis.NewStreamConsumer(
		redisClient, streams.ValidationRequestedV1, streams.ExecutorValidationRequested,
		validationReqBinding, logger, schemaOpReclaim)
	logger.Info("validation.requested consumer initialized",
		"stream", streams.ValidationRequestedV1, "group", streams.ExecutorValidationRequested)

	validationNodeConsumer := pkgredis.NewStreamConsumer(
		redisClient, streams.ValidationNodeCompletedV1, streams.ExecutorValidationNodeCompleted,
		validationNodeBinding, logger)
	logger.Info("validation.node.completed consumer initialized",
		"stream", streams.ValidationNodeCompletedV1, "group", streams.ExecutorValidationNodeCompleted)

	seedBuildReqConsumer := pkgredis.NewStreamConsumer(
		redisClient, streams.SeedBuildRequestedV1, streams.ExecutorSeedBuildRequested,
		seedBuildReqBinding, logger, schemaOpReclaim)
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

	validationResultTeardownConsumer := pkgredis.NewStreamConsumer(
		redisClient, streams.ValidationResultV1, streams.ExecutorValidationResultTeardown,
		validationResultTeardownBinding, logger, schemaOpReclaim)
	logger.Info("validation.result teardown consumer initialized",
		"stream", streams.ValidationResultV1, "group", streams.ExecutorValidationResultTeardown)

	releaseRejectedTeardownConsumer := pkgredis.NewStreamConsumer(
		redisClient, streams.ReleaseRejectedV1, streams.ExecutorReleaseRejected,
		releaseRejectedTeardownBinding, logger, schemaOpReclaim)
	logger.Info("release.rejected teardown consumer initialized",
		"stream", streams.ReleaseRejectedV1, "group", streams.ExecutorReleaseRejected)

	releasePromotedTeardownConsumer := pkgredis.NewStreamConsumer(
		redisClient, streams.ReleasePromotedV1, streams.ExecutorReleasePromoted,
		releasePromotedTeardownBinding, logger, schemaOpReclaim)
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

	liveReg.RegisterWorker("outbox_processor")
	lifecycleManager.Go(func() {
		err := outboxProcessor.Run(ctx)
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		liveReg.WorkerExited("outbox_processor", err)
		if err != nil {
			logger.Error("Outbox processor exited", "error", err)
		}
	})

	k8sDeployer := k8s.NewDeployer(k8sClient, cfg.K8sNamespace)
	deployDispatcher := deployer.NewDispatcher(
		pgDB, k8sDeployer,
		func(exec pkgoutbox.Executor) repository.DeploymentRepository {
			return postgres.NewDeploymentsRepository(exec, logger)
		},
		func(exec pkgoutbox.Executor) repository.ValidationAggregateRepository {
			return postgres.NewValidationAggregateRepository(exec)
		},
		cfg.MaxConcurrentJobs, logger,
		deployer.DispatcherConfig{Tick: 5 * time.Second, BatchSize: 50},
	)

	liveReg.RegisterWorker("deploy_dispatcher")
	lifecycleManager.Go(func() {
		err := deployDispatcher.Run(ctx)
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		liveReg.WorkerExited("deploy_dispatcher", err)
		if err != nil {
			logger.Error("Deploy dispatcher exited", "error", err)
		}
	})

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

	healthServer := http.NewHealthServer(cfg.HTTPPort, liveReg, logger)

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
	// on shutdown, which exits each Start cleanly. runConsumer additionally
	// registers each one with the liveness registry backing /ready.
	runConsumer("query_model", queryConsumer)
	runConsumer("retry_task", retryConsumer)
	runConsumer("schedule_cancelled", scheduleCancelledConsumer)
	runSchemaOpConsumer("validation_requested", validationReqConsumer)
	runConsumer("validation_node_completed", validationNodeConsumer)
	runSchemaOpConsumer("seed_build_requested", seedBuildReqConsumer)
	runConsumer("seed_build_node_completed", seedBuildNodeConsumer)
	runConsumer("compile_requested", compileReqConsumer)
	runConsumer("compile_node_completed", compileNodeConsumer)
	runSchemaOpConsumer("validation_result_teardown", validationResultTeardownConsumer)
	runSchemaOpConsumer("release_rejected_teardown", releaseRejectedTeardownConsumer)
	runSchemaOpConsumer("release_promoted_teardown", releasePromotedTeardownConsumer)

	// Block until the full shutdown sequence has completed: intake stopped,
	// tracked goroutines drained, and infra-close handlers run.
	<-lifecycleManager.Done()

	logger.Info("Executor-controller service stopped")
}
