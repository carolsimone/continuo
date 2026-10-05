package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/pkg/liveness"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/remediation/adapters/postgres"
	rredis "github.com/carolsimone/continuo/remediation/adapters/redis"
	rs3 "github.com/carolsimone/continuo/remediation/adapters/s3"
	"github.com/carolsimone/continuo/remediation/config"
	"github.com/carolsimone/continuo/remediation/service/handlers"
	"github.com/carolsimone/continuo/remediation/service/ports"
	"github.com/carolsimone/continuo/remediation/service/uow"
)

// consumerHandlerTimeout bounds each message handler invocation with a context
// deadline, so a genuinely-hung handler eventually returns control to the read
// loop. This handler does short DB writes and S3 log reads, so 60s far exceeds
// any legitimate invocation while still bounding a wedge.
const consumerHandlerTimeout = 60 * time.Second

// dbPool bounds the Postgres pool when DB_MAX_OPEN_CONNS / DB_MAX_IDLE_CONNS
// are unset: two connections per stream consumer (a handler's transaction and
// a read outside it), plus the outbox relay, background loops and request
// handlers.
var dbPool = pkgconfig.PoolConfig{MaxOpenConns: 10, MaxIdleConns: 5}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	v := &pkgconfig.Validator{}
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
	// outage stops traffic but must not restart a pod whose consumer is already
	// retrying).
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

	mux := http.NewServeMux()
	// Two health paths with different semantics, both registry-backed. Deploy
	// config points the Kubernetes readinessProbe at /healthz and the
	// livenessProbe at /livez (see deploy/continuo/values.yaml): /healthz
	// (readiness) reflects workers + heartbeats + dependency probes, so a Redis/
	// Postgres outage pulls the pod from Service endpoints; /livez (liveness)
	// reflects workers + heartbeats ONLY, so a dependency outage does NOT restart
	// a pod whose consumer is already retrying, while a dead/wedged consumer does.
	// The server comes up before any dependency is reachable so /livez keeps
	// answering while the boot waits for Redis; the startup gate holds /healthz
	// false until the consumers are running.
	startup := liveReg.AddStartupGate()
	mux.HandleFunc("/healthz", liveness.Handler("readiness", liveReg.Check, logger))
	mux.HandleFunc("/livez", liveness.Handler("liveness", liveReg.LivenessCheck, logger))
	srv := &http.Server{Addr: ":" + cfg.HTTPPort, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.ListenAndServe() }()

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

	// Redis is a separate workload that may still be starting (or its Service
	// not yet resolvable) when this process boots, so wait for it with backoff
	// instead of exiting on the first refused dial.
	rc, err := rredis.NewClient(ctx, rredis.Config{
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

	logReader := rs3.NewLogReader(
		cfg.S3.EndpointURL,
		cfg.S3.Bucket,
		cfg.S3.Region,
		cfg.S3.AccessKeyID,
		cfg.S3.SecretAccessKey,
	)

	deps := handlers.Deps{
		NewUoW:    func() uow.UnitOfWork { return postgres.NewUnitOfWork(db, logger) },
		LogReader: logReader,
		Clock:     ports.SystemClock{},
		Logger:    logger,
	}

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
	outboxProc := rredis.StartOutboxPublisher(ctx, db, rc, outboxWaker, metricsReg.Outbox(), liveReg, logger)
	metricsReg.WatchOutbox(outboxProc)

	// Start the release.rejected consumer in a goroutine; blocks until ctx is
	// cancelled.
	runConsumer("release_rejected", rredis.NewReleaseRejectedConsumer(rc, deps, logger))
	// Start the remediation.retry_requested consumer — a human's "try again"
	// replay of a rejected release's stored rejection — in a goroutine; blocks
	// until ctx is cancelled.
	runConsumer("remediation_retry", rredis.NewRemediationRetryConsumer(rc, deps, logger))

	// Everything is initialised and running: readiness may now turn true.
	startup.Done()

	logger.Info("remediation service started", "http_port", cfg.HTTPPort)
	<-ctx.Done()
	_ = srv.Shutdown(context.Background())
	logger.Info("remediation service stopped")
}
