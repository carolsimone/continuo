package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"

	grpcinfra "github.com/carolsimone/continuo/orchestrator/adapters/grpc"
	httpinfra "github.com/carolsimone/continuo/orchestrator/adapters/http"
	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	"github.com/carolsimone/continuo/orchestrator/adapters/postgres"
	orchpublisher "github.com/carolsimone/continuo/orchestrator/adapters/publisher"
	"github.com/carolsimone/continuo/orchestrator/adapters/redis"
	s3infra "github.com/carolsimone/continuo/orchestrator/adapters/s3"
	"github.com/carolsimone/continuo/orchestrator/config"
	"github.com/carolsimone/continuo/orchestrator/internal/reconciler"
	"github.com/carolsimone/continuo/orchestrator/internal/sweeper"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	"github.com/carolsimone/continuo/orchestrator/service/queries"
	snapshotsvc "github.com/carolsimone/continuo/orchestrator/service/snapshotsvc"
	"github.com/carolsimone/continuo/orchestrator/service/watchdog"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/lifecycle"
	"github.com/carolsimone/continuo/pkg/liveness"
	pkgmessageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// consumerHeartbeatStale is the liveness heartbeat budget: how long a
// consumer's read loop may go without completing an iteration before /livez
// reports it wedged. These consumers set no handler timeout, so nothing
// enforces this budget against the slowest handler — it is an empirical
// margin, not an enforced relationship, and must stay comfortably above the
// slowest handler invocation observed in practice.
const consumerHeartbeatStale = 3 * time.Minute

// outboxHeartbeatStale is the liveness budget for the outbox processor's Run
// loop. The poll tick is 1s, so 60s is comfortably above it: a wedged (not
// exited) processor trips within a minute, while an idle-but-live one never
// does.
const outboxHeartbeatStale = 60 * time.Second

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

	logger.Info("Starting orchestrator service")

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
	// clean ctx-cancel stop) flips both readiness and liveness so the unhealthy
	// pod is restarted; a worker heartbeat probe also catches a consumer whose
	// read loop has gone wedged without exiting.
	runConsumer := func(name string, consumer *pkgredis.StreamConsumer) {
		liveReg.RegisterWorker(name)
		liveReg.AddWorkerProbe(name+"_heartbeat", 10*time.Second, func(context.Context) error {
			return consumer.Healthy(consumerHeartbeatStale)
		})
		lifecycleManager.Go(func() {
			err := consumer.Start(ctx)
			liveReg.WorkerExited(name, err)
			if err != nil {
				logger.Error("Consumer exited", "consumer", name, "error", err)
			}
		})
	}

	// ========================================================================
	// INITIALIZE INFRASTRUCTURE
	// ========================================================================

	// 1. Neo4j client
	neo4jClient, err := neo4jinfra.NewNeo4jClient(
		cfg.Neo4j.URI,
		cfg.Neo4j.User,
		cfg.Neo4j.Password,
		logger,
	)
	if err != nil {
		logger.Error("Failed to create Neo4j client", "error", err)
		os.Exit(1)
	}
	logger.Info("Neo4j client initialized")

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Closing Neo4j connection")
		return neo4jClient.Close(ctx)
	})

	// Apply Neo4j constraints and indexes before any consumer or gRPC server
	// starts, so the first message never races a full label scan. Idempotent
	// (IF NOT EXISTS) and fatal on failure — serving traffic against an
	// unindexed/unconstrained graph is not acceptable.
	if err := neo4jinfra.InitSchema(ctx, neo4jClient, logger); err != nil {
		logger.Error("Failed to initialize Neo4j schema", "error", err)
		os.Exit(1)
	}
	logger.Info("Neo4j schema initialized")

	// 2. PostgreSQL client (for outbox / message processing)
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

	// Cached dependency probes feed /ready. They run at most once per TTL so the
	// readiness endpoint stays cheap under Kubernetes probe traffic; a failed
	// ping flips readiness until the dependency recovers.
	liveReg.AddProbe("redis", 5*time.Second, func(ctx context.Context) error {
		return redisClient.Ping(ctx).Err()
	})
	liveReg.AddProbe("postgres", 5*time.Second, func(ctx context.Context) error {
		return pgDB.PingContext(ctx)
	})

	// ========================================================================
	// INITIALIZE REPOSITORIES
	// ========================================================================

	topologyRepo := neo4jinfra.NewTopologyRepository(neo4jClient, logger)
	queryRepo := neo4jinfra.NewOrchestratorQueryRepository(neo4jClient, logger)
	runAggRepo := neo4jinfra.NewRunAggregateRepository(neo4jClient, logger)
	snapshotTxRunner := neo4jinfra.NewSnapshotTxRunner(neo4jClient)
	cancelledSchedulesRepo := postgres.NewCancelledSchedulesRepository(pgDB)
	snapshotService := snapshotsvc.NewService(snapshotTxRunner, cancelledSchedulesRepo, logger)

	// ========================================================================
	// INITIALIZE UNIT OF WORK & COMMAND HANDLERS
	// ========================================================================

	// Each handler gets its own UnitOfWork instance. UnitOfWork holds a stateful
	// Postgres transaction (inTx flag + *sqlx.Tx). Sharing one instance across
	// consumers would cause "transaction already in progress" errors when two
	// consumers process messages concurrently — the second Begin() sees inTx=true
	// and the message is never ACKed, getting stuck in the PEL forever.
	topologyStateRepo := postgres.NewTopologyStateRepository(pgDB)
	handleNodeCompletedHandler := handlers.NewHandleNodeCompletedHandler(postgres.NewPostgresUnitOfWork(pgDB, logger), runAggRepo, cancelledSchedulesRepo, logger)
	handleSchedulerStartedHandler := handlers.NewHandleSchedulerStartedHandler(postgres.NewPostgresUnitOfWork(pgDB, logger), snapshotService, logger)
	handleRerunHandler := handlers.NewHandleRerunHandler(postgres.NewPostgresUnitOfWork(pgDB, logger), snapshotService, logger)
	handleRebaseHandler := handlers.NewHandleRebaseHandler(postgres.NewPostgresUnitOfWork(pgDB, logger), snapshotService, logger)
	handleSingleNodeRunHandler := handlers.NewHandleSingleNodeRunHandler(postgres.NewPostgresUnitOfWork(pgDB, logger), snapshotService, logger)

	// ========================================================================
	// INITIALIZE OUTBOX PROCESSOR
	// ========================================================================

	outboxPub := orchpublisher.NewOutboxPublisher(redisClient, logger)
	outboxProc := pkgoutbox.NewProcessor(
		pgDB,
		"orchestrator_outbox",
		outboxPub,
		nil, // no terminal-failure hook for orchestrator
		logger,
		pkgoutbox.ProcessorConfig{Tick: time.Second, BatchSize: 100},
	)
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

	// ========================================================================
	// START RETENTION SWEEPER
	// ========================================================================
	//
	// Keeps the two unbounded-growth tables in check: processed
	// orchestrator_outbox rows and terminal message_processing dedup rows (the
	// latter retains a full payload per consumed message). Both are pruned past
	// the retention window on the same timer using DB-clock cutoffs.

	mpPruner := pkgmessageprocessing.NewPruner(pgDB, "orchestrator_outbox", logger)
	retentionSweeper := pkgoutbox.NewRetentionSweeper(
		[]pkgoutbox.RetentionTarget{
			pkgoutbox.OutboxRetentionTarget(pgDB, "orchestrator_outbox", logger),
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

	// ========================================================================
	// START HTTP HEALTH CHECK SERVER
	// ========================================================================

	healthServer := httpinfra.NewHealthServer(cfg.HTTPPort, liveReg, logger)

	go func() {
		if err := healthServer.Start(); err != nil {
			logger.Error("Health server error", "error", err)
		}
	}()

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		return healthServer.Shutdown(ctx)
	})

	// ========================================================================
	// START RUN SWEEPER
	// ========================================================================

	runSweeper := sweeper.New(runAggRepo, cfg.RunHistoryRetentionDays, cfg.RunSweeperIntervalMinutes, logger)
	go runSweeper.Start(ctx)

	// ========================================================================
	// START DISPATCH WATCHDOG
	// ========================================================================

	stateConn, err := grpc.NewClient(cfg.StateGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("Failed to dial state gRPC", "endpoint", cfg.StateGRPCAddr, "error", err)
		os.Exit(1)
	}
	defer func() { _ = stateConn.Close() }()
	stateGRPCClient := statev1.NewStateServiceClient(stateConn)

	stuckScheduleAdapter := grpcinfra.NewStuckScheduleAdapter(stateGRPCClient)
	watchdogInstance := watchdog.NewWatchdog(
		watchdog.Config{
			Enabled:       cfg.WatchdogEnabled,
			Interval:      time.Duration(cfg.WatchdogIntervalSecs) * time.Second,
			NoProgressFor: time.Duration(cfg.WatchdogNoProgressMins) * time.Minute,
		},
		stuckScheduleAdapter,
		stuckScheduleAdapter,
		logger,
	)
	go func() {
		if err := watchdogInstance.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("Watchdog Run exited with error", "error", err)
		}
	}()

	// ========================================================================
	// START RECONCILER — converge active :Run projections to state's status
	// ========================================================================

	runStatusReader := grpcinfra.NewRunStatusReader(stateGRPCClient)
	reconcilerInstance := reconciler.New(queryRepo, runStatusReader, runAggRepo, cfg.ReconcilerIntervalSecs, logger)
	go reconcilerInstance.Start(ctx)

	// ========================================================================
	// INITIALIZE CANCELLED SCHEDULES CONSUMER + SWEEPER
	// ========================================================================

	scheduleCancelledHandler := handlers.NewScheduleCancelledHandler(cancelledSchedulesRepo, logger)
	scheduleCancelledBinding := redis.NewScheduleCancelledBinding(scheduleCancelledHandler, logger)
	scheduleCancelledConsumer := pkgredis.NewStreamConsumer(
		redisClient,
		streams.ScheduleCancelledV1,
		streams.OrchestratorScheduleCancelled,
		scheduleCancelledBinding,
		logger,
	)
	runConsumer("schedule_cancelled", scheduleCancelledConsumer)

	lifecycleManager.Go(func() {
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
	})

	// ========================================================================
	// INITIALIZE REDIS CONSUMERS
	// ========================================================================

	// run.finalized:v1 projects state's terminal scheduler outcome onto Neo4j
	// :Run.completed_at / terminal_status. Covers edge cases where the
	// aggregate's internal finalization path is not exercised (e.g.
	// full-inherited rebases that produce no node.updated:v1 traffic).
	runFinalizedHandler := handlers.NewRunFinalizedHandler(runAggRepo, logger)

	// release.promoted:v1 atomically replaces the Neo4j topology when
	// release-controller promotes a candidate release to production, then emits
	// schedules.loaded:v1 so state can refresh its schedule projections. The
	// consumer is dormant until release-controller emits its first
	// release.promoted:v1 event in production.
	releasePromotionRepo := neo4jinfra.NewReleasePromotionRepository(neo4jClient, logger)
	releasePromotedHandler := handlers.NewReleasePromotedHandler(postgres.NewPostgresUnitOfWork(pgDB, logger), releasePromotionRepo, topologyRepo, topologyStateRepo, logger)

	// trigger.promoted_seeds:v1 — projects the run state created for a promoted
	// release onto its changed seeds, so building them into the prod schema runs
	// through the same lifecycle as any other run.
	handlePromotedSeedsHandler := handlers.NewHandlePromotedSeedsRunHandler(postgres.NewPostgresUnitOfWork(pgDB, logger), snapshotService, logger)

	// release.promoted:v1 (versions) — third independent consumer group. It reads
	// the release's code-bundle document from object storage and records the
	// :NodeVersion / :CodeUnitVersion history behind the topology. Isolated from
	// the swap so promotion never waits on object storage, and free to retry
	// until the swap it trails has landed.
	codeBundleReader := s3infra.NewCodeBundleReader(
		cfg.S3.EndpointURL, cfg.S3.Bucket, cfg.S3.Region,
		cfg.S3.AccessKeyID, cfg.S3.SecretAccessKey,
	)
	codeVersionRepo := neo4jinfra.NewCodeVersionRepository(neo4jClient, logger)
	releasePromotedVersionsHandler := handlers.NewReleasePromotedVersionsHandler(
		postgres.NewPostgresUnitOfWork(pgDB, logger), codeBundleReader, codeVersionRepo, logger)

	// remediation.requested:v1 (rejections) + remediation.pr_opened:v1
	// (proposals) — the failure-precedent case base. The rejections handler
	// reuses the versions path's bundle reader to fetch the failing code; the
	// proposals handler needs no bundle. Orchestrator remains the sole Neo4j
	// writer.
	caseBaseRepo := neo4jinfra.NewCaseBaseRepository(neo4jClient, logger)
	rejectionsHandler := handlers.NewRemediationRequestedRejectionsHandler(
		postgres.NewPostgresUnitOfWork(pgDB, logger), codeBundleReader, caseBaseRepo, logger)
	proposalsHandler := handlers.NewPrOpenedProposalsHandler(
		postgres.NewPostgresUnitOfWork(pgDB, logger), caseBaseRepo, logger)

	// Every orchestrator consumer is the same shape: a domain handler wrapped
	// by a redis binding, driven by a StreamConsumer on its (stream, group).
	// They are declared in one table and started uniformly via runConsumer;
	// stream/group names always come from pkg/streams constants.
	consumers := []struct {
		name    string
		stream  string
		group   string
		binding pkgredis.MessageHandler
	}{
		{"node_updated", streams.NodeUpdatedV1, streams.OrchestratorNodeUpdated, redis.NewNodeCompletedBinding(handleNodeCompletedHandler, logger)},
		{"scheduler_started", streams.SchedulerStartedV1, streams.OrchestratorSchedulerStarted, redis.NewSchedulerStartedBinding(handleSchedulerStartedHandler, logger)},
		{"rerun", streams.TriggerRerunV1, streams.OrchestratorRerun, redis.NewRerunBinding(handleRerunHandler, logger)},
		{"rebase", streams.TriggerRebaseV1, streams.OrchestratorRebase, redis.NewRebaseBinding(handleRebaseHandler, logger)},
		{"single_node_run", streams.TriggerSingleNodeRunV1, streams.OrchestratorSingleNodeRun, redis.NewSingleNodeRunBinding(handleSingleNodeRunHandler, logger)},
		{"run_finalized", streams.RunFinalizedV1, streams.OrchestratorRunFinalized, redis.NewRunFinalizedBinding(runFinalizedHandler, logger)},
		{"release_promoted", streams.ReleasePromotedV1, streams.OrchestratorReleasePromoted, redis.NewReleasePromotedBinding(releasePromotedHandler, logger)},
		{"release_promoted_versions", streams.ReleasePromotedV1, streams.OrchestratorReleasePromotedVersions, redis.NewReleasePromotedVersionsBinding(releasePromotedVersionsHandler, logger)},
		{"promoted_seeds", streams.TriggerPromotedSeedsV1, streams.OrchestratorPromotedSeeds, redis.NewPromotedSeedsBinding(handlePromotedSeedsHandler, logger)},
		{"remediation_requested_rejections", streams.RemediationRequestedV1, streams.OrchestratorRemediationRequestedRejections, redis.NewRemediationRequestedBinding(rejectionsHandler, logger)},
		{"remediation_pr_opened_proposals", streams.RemediationPrOpenedV1, streams.OrchestratorRemediationPrOpenedProposals, redis.NewPrOpenedBinding(proposalsHandler, logger)},
	}
	for _, c := range consumers {
		runConsumer(c.name, pkgredis.NewStreamConsumer(redisClient, c.stream, c.group, c.binding, logger))
	}

	// ========================================================================
	// START gRPC SERVER
	// ========================================================================

	runQueries := queries.NewRunQueryService(queryRepo, topologyStateRepo, logger)
	// Topology shapes are immutable per topology_generation, so wrap the schedule
	// reader in an LRU cache keyed by (schedule_name, generation). Run graphs
	// carry live status overlays and stay uncached (served by queryRepo).
	scheduleGraphReader := neo4jinfra.NewCachingScheduleGraphReader(queryRepo, topologyStateRepo, logger)
	codeVersionQueryRepo := neo4jinfra.NewCodeVersionQueryRepository(neo4jClient, logger)
	codeVersionQueries := queries.NewCodeVersionQueryService(codeVersionQueryRepo)
	precedentQueryRepo := neo4jinfra.NewPrecedentQueryRepository(neo4jClient, logger)
	precedentQueries := queries.NewPrecedentQueryService(precedentQueryRepo)
	queryHandler := grpcinfra.NewQueryHandler(scheduleGraphReader, runQueries, codeVersionQueries, precedentQueries, logger)

	grpcServer, err := grpcinfra.NewServer(cfg.GRPCPort, queryHandler, logger)
	if err != nil {
		logger.Error("Failed to create gRPC server", "error", err)
		os.Exit(1)
	}

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		return grpcServer.Shutdown(ctx)
	})

	logger.Info("Orchestrator service initialized, starting gRPC server",
		"grpc_port", cfg.GRPCPort,
		"http_port", cfg.HTTPPort,
	)

	go func() {
		if err := grpcServer.Start(); err != nil {
			logger.Error("gRPC server error", "error", err)
		}
	}()

	// Block until the graceful-shutdown sequence has fully completed: stop
	// intake, drain in-flight goroutines, then close infra. No fixed sleep.
	<-lifecycleManager.Done()
	logger.Info("Orchestrator service stopped")
}
