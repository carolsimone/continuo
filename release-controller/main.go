package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	remediationv1 "github.com/carolsimone/continuo/agent-remediation/api/remediation/v1"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/pkg/liveness"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	rcgrpc "github.com/carolsimone/continuo/release-controller/adapters/grpc"
	httpinfra "github.com/carolsimone/continuo/release-controller/adapters/http"
	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
	redisadapter "github.com/carolsimone/continuo/release-controller/adapters/redis"
	s3adapter "github.com/carolsimone/continuo/release-controller/adapters/s3"
	"github.com/carolsimone/continuo/release-controller/adapters/serialization"
	"github.com/carolsimone/continuo/release-controller/config"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/carolsimone/continuo/release-controller/service/uow"
)

// consumerHandlerTimeout bounds each message handler invocation with a context
// deadline, so a genuinely-hung handler eventually returns control to the read
// loop. These handlers do short DB writes and S3 object work, so 60s far
// exceeds any legitimate invocation while still bounding a wedge.
const consumerHandlerTimeout = 60 * time.Second

// dbPool bounds the Postgres pool when DB_MAX_OPEN_CONNS / DB_MAX_IDLE_CONNS
// are unset: two connections per stream consumer (a handler's transaction and
// a read outside it), plus the outbox relay, background loops and request
// handlers.
var dbPool = pkgconfig.PoolConfig{MaxOpenConns: 15, MaxIdleConns: 5}

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("shutdown signal received")
		cancel()
	}()

	// Health registry feeding /healthz (readiness) and /livez (liveness) —
	// deploy config points the two Kubernetes probes at those DIFFERENT paths
	// (see deploy/continuo/values.yaml). Worker registrations and consumer
	// heartbeats feed both checks (a dead/wedged consumer restarts the pod);
	// dependency probes (Redis/Postgres) feed readiness ONLY (a backing-store
	// outage stops traffic but must not restart a pod whose consumers are
	// already retrying).
	liveReg := liveness.NewRegistry()
	metricsReg := pkgmetrics.New(config.ServiceName)

	// runConsumer starts a tracked stream consumer: a bounded handler deadline
	// so a hung handler eventually returns; RegisterWorker before launch so a
	// missing worker is observable; WorkerExited when Start returns (on a
	// permanent consumer-group bootstrap error as well as on a clean shutdown);
	// and a worker heartbeat probe so a wedged-but-not-exited loop is caught.
	runConsumer := func(name string, consumer *pkgredis.StreamConsumer) {
		consumer.SetService(config.ServiceName)
		consumer.SetObserver(metricsReg.Consumers())
		consumer.SetHandlerTimeout(consumerHandlerTimeout)
		liveReg.RegisterWorker(name)
		liveReg.AddWorkerProbe(name+"_heartbeat", 10*time.Second, func(context.Context) error {
			return consumer.Healthy(consumer.HeartbeatBudget())
		})
		go func() {
			err := consumer.Start(ctx)
			liveReg.WorkerExited(name, err)
			if err != nil && ctx.Err() == nil {
				logger.Error("consumer stopped", "consumer", name, "error", err)
			}
		}()
	}

	// The Prometheus listener comes up before any dependency is reachable so a
	// dependency outage stays observable. Serve runs in a plain goroutine; it
	// is stopped when ctx ends.
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
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsServer.Shutdown(shutdownCtx)
	}()

	db, err := pkgdb.Open(ctx, cfg.Postgres, dbPool)
	if err != nil {
		logger.Error("Failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()
	liveReg.AddProbe("postgres", 5*time.Second, func(ctx context.Context) error {
		return db.PingContext(ctx)
	})
	metricsReg.WatchDB(db.DB, cfg.Postgres.DB)

	// S3 client for pruning candidate-SQL objects when releases are deleted.
	s3Client := s3adapter.NewS3Client(
		cfg.S3.EndpointURL,
		cfg.S3.Bucket,
		cfg.S3.Region,
		cfg.S3.AccessKeyID,
		cfg.S3.SecretAccessKey,
		logger,
	)

	// Dial agent-remediation once for the RetryRemediation handler, which reads
	// a release's remediation attempts before starting another round.
	remediationConn, err := grpc.NewClient(cfg.AgentRemediationGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("grpc agent-remediation client dial", "error", err)
		os.Exit(1)
	}
	defer func() { _ = remediationConn.Close() }()
	proposalsClient := rcgrpc.NewProposalsClient(remediationv1.NewRemediationProposalsClient(remediationConn))

	deps := &handlers.Deps{
		NewUoW:    func() uow.UnitOfWork { return postgres.NewUnitOfWork(db, logger, s3Client) },
		Clock:     ports.SystemClock{},
		Telemetry: ports.NoOpTelemetry{},
		Logger:    logger,
		Bucket:    cfg.S3.Bucket,
		Proposals: proposalsClient,

		Rejections: serialization.ReleaseRejectedJSON{},
	}

	// The HTTP server (API plus /healthz and /livez) comes up before Redis is
	// reachable so /livez keeps answering while the boot waits for it; the
	// startup gate holds /healthz false, keeping the pod out of its Service
	// until the outbox publisher and consumers below are running. The API
	// needs only Postgres and S3: a request accepted before then records its
	// events in the outbox, which the publisher drains once it starts. The
	// server blocks until ctx is cancelled (graceful 5-second shutdown).
	startup := liveReg.AddStartupGate()
	srv := httpinfra.NewServer(deps, liveReg, cfg.HTTPPort, logger)
	srvDone := make(chan struct{})
	go func() {
		defer close(srvDone)
		if err := srv.Start(ctx); err != nil {
			logger.Error("http server error", "error", err)
			os.Exit(1)
		}
	}()

	// Redis is a separate workload that may still be starting (or its Service
	// not yet resolvable) when this process boots, so wait for it with backoff
	// instead of exiting on the first refused dial.
	rc, err := redisadapter.NewClient(ctx, redisadapter.Config{
		Host:     cfg.Redis.Host,
		Port:     cfg.Redis.Port,
		Password: cfg.Redis.Password,
	}, pkgredis.DefaultStartupPolicy(), logger)
	if errors.Is(err, context.Canceled) {
		logger.Info("shutdown requested while waiting for redis")
		os.Exit(0)
	}
	if err != nil {
		logger.Error("redis connect", "error", err)
		os.Exit(1)
	}
	defer func() { _ = rc.Close() }()
	liveReg.AddProbe("redis", 5*time.Second, func(ctx context.Context) error {
		return rc.Ping(ctx).Err()
	})
	metricsReg.WatchRedis(rc)

	// The outbox relay wakes on the notification a committed insert into its
	// table sends.
	outboxWaker, err := pkgoutbox.NewPostgresWaker(ctx, cfg.Postgres.DSN(), postgres.OutboxTable, logger)
	if errors.Is(err, context.Canceled) {
		logger.Info("shutdown requested while waiting for the outbox listener")
		os.Exit(0)
	}
	if err != nil {
		logger.Error("Failed to listen for outbox notifications", "error", err)
		os.Exit(1)
	}
	// Closing the listener can wait on a connection attempt in progress, so
	// shutdown stops waiting for it after 5 seconds.
	defer func() {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelClose()
		_ = outboxWaker.CloseContext(closeCtx)
	}()

	// Start outbox publisher — spawns its own goroutine internally and runs until
	// ctx is cancelled.
	outboxProc := redisadapter.StartOutboxPublisher(ctx, db, rc, outboxWaker, metricsReg.Outbox(), liveReg, logger)
	metricsReg.WatchOutbox(outboxProc)

	// Start stream consumers in goroutines; each blocks until ctx is cancelled.
	runConsumer("manifest_loaded_candidate", redisadapter.NewManifestLoadedCandidateConsumer(rc, deps, logger))
	runConsumer("validation_result", redisadapter.NewValidationResultConsumer(rc, deps, logger))
	runConsumer("seed_build_completed", redisadapter.NewSeedBuildCompletedConsumer(rc, deps, logger))
	runConsumer("compile_completed", redisadapter.NewCompileCompletedConsumer(rc, deps, logger))

	// Retention loop: prune terminal releases older than the retention window on
	// the janitor interval. current_prod is never pruned.
	retentionDays, err := strconv.Atoi(cfg.RetentionDays)
	if err != nil {
		logger.Error("invalid RELEASE_RETENTION_DAYS", "value", cfg.RetentionDays)
		os.Exit(1)
	}
	janitorEvery, err := time.ParseDuration(cfg.JanitorInterval)
	if err != nil {
		logger.Error("invalid RELEASE_JANITOR_INTERVAL", "value", cfg.JanitorInterval)
		os.Exit(1)
	}
	go func() {
		ticker := time.NewTicker(janitorEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n, err := handlers.PruneFinishedRuns(ctx, deps, retentionDays)
				if err != nil {
					logger.Error("retention prune failed", "error", err)
					continue
				}
				if n > 0 {
					logger.Info("pruned finished pipeline runs", "count", n)
				}
			}
		}
	}()

	// Everything is initialised and running: readiness may now turn true.
	startup.Done()

	<-srvDone
}
