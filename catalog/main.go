package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"

	grpcinfra "github.com/carolsimone/continuo/catalog/adapters/grpc"
	httpinfra "github.com/carolsimone/continuo/catalog/adapters/http"
	metricsinfra "github.com/carolsimone/continuo/catalog/adapters/metrics"
	neo4jinfra "github.com/carolsimone/continuo/catalog/adapters/neo4j"
	"github.com/carolsimone/continuo/catalog/adapters/redis"
	s3infra "github.com/carolsimone/continuo/catalog/adapters/s3"
	"github.com/carolsimone/continuo/catalog/config"
	"github.com/carolsimone/continuo/catalog/service/backup"
	"github.com/carolsimone/continuo/catalog/service/handlers"
	"github.com/carolsimone/continuo/catalog/service/queries"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/lifecycle"
	"github.com/carolsimone/continuo/pkg/liveness"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/carolsimone/continuo/pkg/objectstore"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	pkgretention "github.com/carolsimone/continuo/pkg/retention"
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
)

// topologyHandlerTimeout bounds the consumers whose one message covers a whole
// DAG or release, which they write to Neo4j in one handler.
const topologyHandlerTimeout = 5 * time.Minute

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

	logger.Info("Starting catalog service")

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

	// Migrate the graph to the catalog's shape and apply its constraints and
	// indexes before any consumer or gRPC server starts, while readiness is
	// still false, so the first message never meets an unmigrated graph or
	// races a full label scan. Idempotent, resumable after a crash, and fatal
	// on failure.
	if err := neo4jinfra.InitSchema(ctx, neo4jClient, logger); err != nil {
		logger.Error("Failed to initialize Neo4j schema", "error", err)
		os.Exit(1)
	}
	logger.Info("Neo4j schema initialized")

	// 2. Redis client
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
	liveReg.AddProbe("neo4j", 5*time.Second, neo4jClient.VerifyConnectivity)

	// ========================================================================
	// INITIALIZE REPOSITORIES
	// ========================================================================

	queryRepo := neo4jinfra.NewNodeQueryRepository(neo4jClient, logger)

	// Marker sweeper — keeps the processed-message markers, one node per
	// consumed message, from growing without bound. A marker is kept at least
	// 30 days (the replay horizon), longer when RETENTION_DAYS is longer, so a
	// message the streams can still redeliver always finds its marker. The
	// sweep uses database-clock cutoffs, deletes in batches, and skips a tenant
	// whose export lease is held. It runs every RETENTION_SWEEP_INTERVAL_MINUTES
	// as a tracked goroutine, so shutdown waits for a sweep in flight.
	markerSweeper := neo4jinfra.NewMarkerSweeper(neo4jClient, logger)
	retentionSweeper := pkgretention.NewSweeper(
		[]pkgretention.Target{
			{
				Name:         "processed_messages",
				Prune:        markerSweeper.Prune,
				AfterSweep:   markerSweeper.AfterSweep,
				MinRetention: model.ReplayHorizon,
			},
		},
		pkgretention.Config{
			Retention: time.Duration(cfg.RetentionDays) * 24 * time.Hour,
			Interval:  time.Duration(cfg.RetentionSweepIntervalMin) * time.Minute,
		},
		logger,
	)
	lifecycleManager.Go(func() { retentionSweeper.Run(ctx) })

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		return healthServer.Shutdown(ctx)
	})

	// ========================================================================
	// INITIALIZE REDIS CONSUMERS
	// ========================================================================

	// One object-storage client, passed to each adapter as the roles it uses:
	// the topology artifact reader of both promotion groups and the
	// code-bundle reader of the versions and rejections groups read objects,
	// and the backup store streams, lists and deletes each tenant's exports.
	objectStore, err := objectstore.New(ctx, objectstore.ConfigFromS3(cfg.S3))
	if err != nil {
		logger.Error("Failed to build the object-storage client", "error", err)
		os.Exit(1)
	}

	// The Neo4j unit of work writes a message's data and its processed-message
	// marker in one transaction. It holds no per-message state, so one
	// instance serves every consumer.
	graphUnitOfWork := neo4jinfra.NewUnitOfWork(neo4jClient, logger)

	// release.promoted:v2 swaps the tenant's live Neo4j topology when
	// release-controller promotes a release, reading the topology from the
	// release's artifact in object storage. The swap decides by promotion seq,
	// so a late or redriven older promotion never reverts the live topology.
	topologyArtifactReader := s3infra.NewTopologyArtifactReader(objectStore)
	releasePromotedHandler := handlers.NewReleasePromotedHandler(graphUnitOfWork, topologyArtifactReader, logger)

	// release.promoted:v2 (versions) — an independent consumer group. It reads
	// the release's code-bundle document and topology artifact from object
	// storage and records the :NodeVersion / :CodeUnitVersion history behind the
	// topology, in batches, through the Neo4j unit of work. Isolated from the
	// swap so promotion never waits on the bundle; it does not wait for the
	// swap either, creating inactive a node the swap has not written yet.
	codeBundleReader := s3infra.NewCodeBundleReader(objectStore)
	releasePromotedVersionsHandler := handlers.NewReleasePromotedVersionsHandler(
		graphUnitOfWork, codeBundleReader, topologyArtifactReader, logger)

	// remediation.requested:v2 (rejections) + remediation.pr_opened:v1
	// (proposals) + remediation.pr_closed:v1 (provenance) — the
	// failure-precedent case base, written under the tenant each message names
	// through the Neo4j unit of work. The rejections handler reuses the
	// versions path's bundle reader to fetch the failing code; the proposals
	// and provenance handlers need no bundle. The provenance handler draws the
	// RESOLVED_BY/EDITED case-base edges on a merged PR and stamps every PR's
	// terminal state. The catalog service remains the sole Neo4j writer.
	rejectionsHandler := handlers.NewRemediationRequestedRejectionsHandler(graphUnitOfWork, codeBundleReader, logger)
	proposalsHandler := handlers.NewPrOpenedProposalsHandler(graphUnitOfWork, logger)
	provenanceHandler := handlers.NewPrClosedProvenanceHandler(graphUnitOfWork, logger)

	// Every catalog consumer is the same shape: a domain handler wrapped
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
		{"release_promoted", streams.ReleasePromotedV2, streams.CatalogReleaseTopology, redis.NewReleasePromotedBinding(releasePromotedHandler, logger), topologyHandlerTimeout},
		{"release_promoted_versions", streams.ReleasePromotedV2, streams.CatalogReleaseVersions, redis.NewReleasePromotedVersionsBinding(releasePromotedVersionsHandler, logger), topologyHandlerTimeout},
		{"remediation_requested_rejections", streams.RemediationRequestedV2, streams.CatalogRemediationRequestedRejections, redis.NewRemediationRequestedBinding(rejectionsHandler, logger), 0},
		{"remediation_pr_opened_proposals", streams.RemediationPrOpenedV1, streams.CatalogRemediationPrOpenedProposals, redis.NewPrOpenedBinding(proposalsHandler, logger), 0},
		{"remediation_pr_closed_provenance", streams.RemediationPrClosedV1, streams.CatalogRemediationPrClosedProvenance, redis.NewPrClosedBinding(provenanceHandler, logger), 0},
	}
	for _, c := range consumers {
		runConsumer(c.name, pkgredis.NewStreamConsumer(redisClient, c.stream, c.group, c.binding, logger,
			pkgredis.WithHandlerTimeout(c.timeout),
			pkgredis.WithInfraClassifier(neo4jinfra.IsUnavailable)))
	}

	// ========================================================================
	// START THE BACKUP
	// ========================================================================

	// With CATALOG_BACKUP_ENABLED, every tenant's graph is exported at each
	// slot of CATALOG_BACKUP_SCHEDULE (UTC) to
	// tenants/<tenant>/backups/catalog/<UTC time>.jsonl.gz, streamed so memory
	// does not grow with the graph, and the newest CATALOG_BACKUP_KEEP exports
	// of each tenant are kept. An export holds the tenant's export lease, so
	// the tenant's writes back off on every pod while it runs. The scheduler
	// is a tracked goroutine: shutdown cancels an export in flight, which
	// aborts its upload and releases its lease. The export series are
	// registered only here.
	if cfg.Backup.Enabled {
		backupStore := s3infra.NewBackupStore(objectStore)
		graphTenants := neo4jinfra.NewGraphTenants(neo4jClient)
		exporter := backup.NewExporter(
			neo4jinfra.NewExportLease(neo4jClient, logger),
			neo4jinfra.NewGraphExporter(neo4jClient, config.Version(), logger),
			backupStore,
			metricsinfra.NewBackupMetrics(metricsReg, backupStore, graphTenants, time.Now, logger),
			cfg.Backup.Keep,
			time.Now,
			logger,
		)
		backupScheduler, err := backup.NewScheduler(cfg.Backup.Schedule, graphTenants, exporter, time.Now, logger)
		if err != nil {
			logger.Error("Failed to build the catalog backup scheduler", "error", err)
			os.Exit(1)
		}
		lifecycleManager.Go(func() { backupScheduler.Run(ctx) })
		logger.Info("Catalog backup scheduled", "schedule", cfg.Backup.Schedule, "keep", cfg.Backup.Keep)
	} else {
		logger.Info("Catalog backup disabled (CATALOG_BACKUP_ENABLED=false)")
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

	logger.Info("Catalog service initialized, starting gRPC server",
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
	logger.Info("Catalog service stopped")
}
