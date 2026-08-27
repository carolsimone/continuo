package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/liveness"
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

// consumerHeartbeatStale is the liveness heartbeat budget: how long the
// consumer's read loop may make no progress before the liveness probe restarts
// the pod. It MUST exceed consumerHandlerTimeout plus a margin so a legitimately
// in-flight handler never trips liveness (the heartbeat advances per handler
// attempt; see pkg/redis.StreamConsumer.safeInvoke), while a true wedge still
// trips within budget.
const consumerHeartbeatStale = 3 * time.Minute

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

	// runConsumer starts a tracked stream consumer: a bounded handler deadline
	// so a hung handler eventually returns; RegisterWorker before launch so a
	// missing worker is observable; WorkerExited when Start returns (which now
	// happens on a permanent bootstrap error too, not only clean shutdown); and
	// a worker heartbeat probe so a wedged-but-not-exited loop is caught.
	runConsumer := func(name string, consumer *pkgredis.StreamConsumer) {
		consumer.SetHandlerTimeout(consumerHandlerTimeout)
		liveReg.RegisterWorker(name)
		liveReg.AddWorkerProbe(name+"_heartbeat", 10*time.Second, func(context.Context) error {
			return consumer.Healthy(consumerHeartbeatStale)
		})
		go func() {
			err := consumer.Start(ctx)
			liveReg.WorkerExited(name, err)
			if err != nil && ctx.Err() == nil {
				logger.Error("consumer stopped", "consumer", name, "error", err)
			}
		}()
	}

	db, err := postgres.NewDB(postgres.Config{
		Host:     cfg.Postgres.Host,
		Port:     cfg.Postgres.Port,
		User:     cfg.Postgres.User,
		Password: cfg.Postgres.Password,
		DB:       cfg.Postgres.DB,
	})
	if err != nil {
		logger.Error("postgres connect", "error", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()
	liveReg.AddProbe("postgres", 5*time.Second, func(ctx context.Context) error {
		return db.PingContext(ctx)
	})

	rc, err := rredis.NewClient(ctx, rredis.Config{
		Host:     cfg.Redis.Host,
		Port:     cfg.Redis.Port,
		Password: cfg.Redis.Password,
	})
	if err != nil {
		logger.Error("redis connect", "error", err)
		os.Exit(1)
	}
	defer func() { _ = rc.Close() }()
	liveReg.AddProbe("redis", 5*time.Second, func(ctx context.Context) error {
		return rc.Ping(ctx).Err()
	})

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

	// Start outbox publisher — spawns its own goroutine internally and runs until
	// ctx is cancelled.
	rredis.StartOutboxPublisher(ctx, db, rc, liveReg, logger)

	// Start the release.rejected consumer in a goroutine; blocks until ctx is
	// cancelled.
	runConsumer("release_rejected", rredis.NewReleaseRejectedConsumer(rc, deps, logger))
	// Start the remediation.retry_requested consumer — a human's "try again"
	// replay of a rejected release's stored rejection — in a goroutine; blocks
	// until ctx is cancelled.
	runConsumer("remediation_retry", rredis.NewRemediationRetryConsumer(rc, deps, logger))

	mux := http.NewServeMux()
	// Two health paths with different semantics, both registry-backed. Deploy
	// config points the Kubernetes readinessProbe at /healthz and the
	// livenessProbe at /livez (see deploy/continuo/values.yaml): /healthz
	// (readiness) reflects workers + heartbeats + dependency probes, so a Redis/
	// Postgres outage pulls the pod from Service endpoints; /livez (liveness)
	// reflects workers + heartbeats ONLY, so a dependency outage does NOT restart
	// a pod whose consumer is already retrying, while a dead/wedged consumer does.
	mux.HandleFunc("/healthz", liveness.Handler("readiness", liveReg.Check, logger))
	mux.HandleFunc("/livez", liveness.Handler("liveness", liveReg.LivenessCheck, logger))
	srv := &http.Server{Addr: ":" + cfg.HTTPPort, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.ListenAndServe() }()

	logger.Info("remediation service started", "http_port", cfg.HTTPPort)
	<-ctx.Done()
	_ = srv.Shutdown(context.Background())
	logger.Info("remediation service stopped")
}
