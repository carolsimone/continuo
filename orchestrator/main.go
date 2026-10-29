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
	"github.com/carolsimone/continuo/orchestrator/adapters/redis"
	s3infra "github.com/carolsimone/continuo/orchestrator/adapters/s3"
	"github.com/carolsimone/continuo/orchestrator/config"
	"github.com/carolsimone/continuo/orchestrator/internal/sweeper"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	"github.com/carolsimone/continuo/orchestrator/service/queries"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/lifecycle"
	"github.com/carolsimone/continuo/pkg/liveness"
	pkgmessageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/carolsimone/continuo/pkg/objectstore"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
)

// topologyHandlerTimeout bounds the consumers whose one message covers a whole
// DAG or release, which they write to Neo4j in one handler.
const topologyHandlerTimeout = 5 * time.Minute

// dbPool bounds the Postgres pool when DB_MAX_OPEN_CONNS / DB_MAX_IDLE_CONNS
// are unset: two connections per stream consumer (a handler's transaction and
// a read outside it), plus background loops and request handlers.
var dbPool = pkgconfig.PoolConfig{MaxOpenConns: 35, MaxIdleConns: 5}

func main() {
	// The logger writes at the level LOG_LEVEL names. An unknown level is
	// recorded on v, and the missing-configuration check below stops the
	// service once this logger can report it.
	v := &pkgconfig.Validator{}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: pkgconfig.LoadLogLevel(v)}))

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

	// ========================================================================
	// INITIALIZE INFRASTRUCTURE
	// ========================================================================

	// The health server comes up before any dependency is reachable so the
	// liveness probe keeps answering while Neo4j or Redis is still starting;
	// otherwise a wait longer than the probe's failure window would restart the
	// pod anyway.
	// Readiness stays false until startup completes (see the startup gate).
	startup := liveReg.AddStartupGate()
	healthServer := httpinfra.NewHealthServer(cfg.HTTPPort, liveReg, logger)
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

	// 1. Neo4j client. Neo4j is a separate workload that may still be starting
	// (or its Service not yet resolvable) when this pod boots, so wait for it
	// with backoff instead of exiting and relying on a pod restart.
	neo4jClient, err := neo4jinfra.NewNeo4jClientWithRetry(
		ctx,
		cfg.Neo4j.URI,
		cfg.Neo4j.User,
		cfg.Neo4j.Password,
		logger,
		neo4jinfra.DefaultStartupPolicy(),
	)
	if errors.Is(err, context.Canceled) {
		// A shutdown signal arrived while waiting for Neo4j; nothing was started.
		logger.Info("Shutdown requested while waiting for Neo4j")
		os.Exit(0)
	}
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

	// 2. PostgreSQL client (for message processing)
	pgDB, err := pkgdb.Open(ctx, cfg.Postgres, dbPool)
	if err != nil {
		logger.Error("Failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	logger.Info("PostgreSQL client initialized")
	metricsReg.WatchDB(pgDB.DB, cfg.Postgres.DB)

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

	// Redis is a separate workload too: wait for it with backoff rather than
	// exiting on the first refused dial.
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

	queryRepo := neo4jinfra.NewOrchestratorQueryRepository(neo4jClient, logger)
	runHistoryRepo := neo4jinfra.NewRunHistoryRepository(neo4jClient, logger)

	// ========================================================================
	// START RETENTION SWEEPER
	// ========================================================================
	//
	// Keeps the message_processing dedup table, which retains a full payload per
	// consumed message, from growing without bound. Dedup rows, whatever their
	// state, are kept at least 30 days (the replay horizon), longer when
	// RETENTION_DAYS is longer. The sweep uses DB-clock cutoffs.

	mpPruner := pkgmessageprocessing.NewPrunerWithoutOutbox(pgDB, logger)
	retentionSweeper := pkgoutbox.NewRetentionSweeper(
		[]pkgoutbox.RetentionTarget{
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

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		return healthServer.Shutdown(ctx)
	})

	// ========================================================================
	// START RUN SWEEPER
	// ========================================================================

	runSweeper := sweeper.New(runHistoryRepo, cfg.RunHistoryRetentionDays, cfg.RunSweeperIntervalMinutes, logger)
	go runSweeper.Start(ctx)

	// ========================================================================
	// INITIALIZE REDIS CONSUMERS
	// ========================================================================

	// Each handler gets its own UnitOfWork instance. UnitOfWork holds a stateful
	// Postgres transaction (inTx flag + *sqlx.Tx). Sharing one instance across
	// consumers would cause "transaction already in progress" errors when two
	// consumers process messages concurrently — the second Begin() sees inTx=true
	// and the message is never ACKed, getting stuck in the PEL forever.
	//
	// One object-storage client, passed to each reader as its
	// objectstore.Reader role: the topology artifact reader of both promotion
	// groups, and the code-bundle reader of the versions and rejections
	// groups.
	objectStore, err := objectstore.New(ctx, objectstore.ConfigFromS3(cfg.S3))
	if err != nil {
		logger.Error("Failed to build the object-storage client", "error", err)
		os.Exit(1)
	}

	// release.promoted:v2 swaps the live Neo4j topology when release-controller
	// promotes a release, reading the topology from the release's artifact in
	// object storage. The swap decides by promotion seq, so a late or redriven
	// older promotion never reverts the live topology.
	topologyArtifactReader := s3infra.NewTopologyArtifactReader(objectStore)
	releasePromotionRepo := neo4jinfra.NewReleasePromotionRepository(neo4jClient, logger)
	releasePromotedHandler := handlers.NewReleasePromotedHandler(postgres.NewPostgresUnitOfWork(pgDB, logger), topologyArtifactReader, releasePromotionRepo, logger)

	// release.promoted:v2 (versions) — an independent consumer group. It reads
	// the release's code-bundle document and topology artifact from object
	// storage and records the :NodeVersion / :CodeUnitVersion history behind the
	// topology. Isolated from the swap so promotion never waits on the bundle,
	// and free to retry until the swap it trails has landed.
	codeBundleReader := s3infra.NewCodeBundleReader(objectStore)
	codeVersionRepo := neo4jinfra.NewCodeVersionRepository(neo4jClient, logger)
	releasePromotedVersionsHandler := handlers.NewReleasePromotedVersionsHandler(
		postgres.NewPostgresUnitOfWork(pgDB, logger), codeBundleReader, topologyArtifactReader, codeVersionRepo, logger)

	// remediation.requested:v2 (rejections) + remediation.pr_opened:v1
	// (proposals) + remediation.pr_closed:v1 (provenance) — the
	// failure-precedent case base. The rejections handler reuses the versions
	// path's bundle reader to fetch the failing code; the proposals and
	// provenance handlers need no bundle. The provenance handler draws the
	// RESOLVED_BY/EDITED case-base edges on a merged PR and stamps every PR's
	// terminal state. Orchestrator remains the sole Neo4j writer.
	caseBaseRepo := neo4jinfra.NewCaseBaseRepository(neo4jClient, logger)
	rejectionsHandler := handlers.NewRemediationRequestedRejectionsHandler(
		postgres.NewPostgresUnitOfWork(pgDB, logger), codeBundleReader, caseBaseRepo, logger)
	proposalsHandler := handlers.NewPrOpenedProposalsHandler(
		postgres.NewPostgresUnitOfWork(pgDB, logger), caseBaseRepo, logger)
	provenanceHandler := handlers.NewPrClosedProvenanceHandler(
		postgres.NewPostgresUnitOfWork(pgDB, logger), caseBaseRepo, logger)

	// Every orchestrator consumer is the same shape: a domain handler wrapped
	// by a redis binding, driven by a StreamConsumer on its (stream, group).
	// They are declared in one table and started uniformly via runConsumer;
	// stream/group names always come from pkg/streams constants. timeout is the
	// handler deadline; 0 keeps pkgredis.DefaultHandlerTimeout.
	consumers := []struct {
		name    string
		stream  string
		group   string
		binding pkgredis.MessageHandler
		timeout time.Duration
	}{
		{"release_promoted", streams.ReleasePromotedV2, streams.OrchestratorReleasePromoted, redis.NewReleasePromotedBinding(releasePromotedHandler, logger), topologyHandlerTimeout},
		{"release_promoted_versions", streams.ReleasePromotedV2, streams.OrchestratorReleasePromotedVersions, redis.NewReleasePromotedVersionsBinding(releasePromotedVersionsHandler, logger), topologyHandlerTimeout},
		{"remediation_requested_rejections", streams.RemediationRequestedV2, streams.OrchestratorRemediationRequestedRejections, redis.NewRemediationRequestedBinding(rejectionsHandler, logger), 0},
		{"remediation_pr_opened_proposals", streams.RemediationPrOpenedV1, streams.OrchestratorRemediationPrOpenedProposals, redis.NewPrOpenedBinding(proposalsHandler, logger), 0},
		{"remediation_pr_closed_provenance", streams.RemediationPrClosedV1, streams.OrchestratorRemediationPrClosedProvenance, redis.NewPrClosedBinding(provenanceHandler, logger), 0},
	}
	for _, c := range consumers {
		runConsumer(c.name, pkgredis.NewStreamConsumer(redisClient, c.stream, c.group, c.binding, logger,
			pkgredis.WithHandlerTimeout(c.timeout),
			pkgredis.WithInfraClassifier(neo4jinfra.IsUnavailable)))
	}

	// ========================================================================
	// START gRPC SERVER
	// ========================================================================

	codeVersionQueryRepo := neo4jinfra.NewCodeVersionQueryRepository(neo4jClient, logger)
	codeVersionQueries := queries.NewCodeVersionQueryService(codeVersionQueryRepo)
	precedentQueryRepo := neo4jinfra.NewPrecedentQueryRepository(neo4jClient, logger)
	precedentQueries := queries.NewPrecedentQueryService(precedentQueryRepo)
	queryHandler := grpcinfra.NewQueryHandler(queryRepo, codeVersionQueries, precedentQueries, logger)

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

	// Everything is initialised and serving: readiness (held false by the
	// startup gate since the health server came up) may now turn true.
	startup.Done()

	// Block until the graceful-shutdown sequence has fully completed: stop
	// intake, drain in-flight goroutines, then close infra. No fixed sleep.
	<-lifecycleManager.Done()
	logger.Info("Orchestrator service stopped")
}
