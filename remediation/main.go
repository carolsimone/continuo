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

// consumerHeartbeatStale is how long a stream consumer's read loop may go
// without an iteration before the readiness probe considers it stalled — not
// "erroring while it retries" (that path advances the heartbeat every
// iteration; see pkg/redis.StreamConsumer.Healthy) but genuinely wedged.
const consumerHeartbeatStale = 30 * time.Second

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

	// Liveness registry feeding /healthz (deploy config points BOTH the
	// readiness AND liveness Kubernetes probes at it — probePath: /healthz in
	// deploy/continuo/values.yaml). Tracks background workers (the
	// release.rejected consumer, outbox publisher) plus cached dependency
	// probes, so a wedged or exited consumer restarts the pod instead of
	// leaving it at 1/1 Running with a dead background loop nothing else can
	// see.
	liveReg := liveness.NewRegistry()

	// runConsumer starts a tracked stream consumer: RegisterWorker before
	// launch so a missing worker is observable from the first probe,
	// WorkerExited when Start returns (a non-nil error is a genuine unhandled
	// exit — Start's own retry loop already absorbs transient Redis errors),
	// and a heartbeat probe so a wedged-but-not-exited loop is caught too.
	runConsumer := func(name string, consumer *pkgredis.StreamConsumer) {
		liveReg.RegisterWorker(name)
		liveReg.AddProbe(name+"_heartbeat", 10*time.Second, func(context.Context) error {
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

	mux := http.NewServeMux()
	// /healthz is backed by the liveness registry — deploy config points both
	// the readiness AND liveness Kubernetes probes at it (probePath: /healthz
	// in deploy/continuo/values.yaml) — so a consumer that exits, or whose
	// read-loop heartbeat goes stale, actually restarts the pod instead of
	// leaving a dead background loop running silently inside a 1/1 pod.
	mux.HandleFunc("/healthz", healthzHandler(liveReg, logger))
	srv := &http.Server{Addr: ":" + cfg.HTTPPort, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.ListenAndServe() }()

	logger.Info("remediation service started", "http_port", cfg.HTTPPort)
	<-ctx.Done()
	_ = srv.Shutdown(context.Background())
	logger.Info("remediation service stopped")
}

// healthzHandler backs /healthz with the liveness registry: 200 when every
// registered worker and dependency probe is healthy, 503 otherwise. Extracted
// from main so it is directly unit-testable (see main_test.go) — the
// regression coverage that pins "a dead consumer must flip this to 503"
// rather than the old hardcoded-200 behaviour.
func healthzHandler(reg *liveness.Registry, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		failures := reg.Check(r.Context())
		if len(failures) == 0 {
			w.WriteHeader(http.StatusOK)
			return
		}
		for _, f := range failures {
			logger.Warn("Readiness check failed", "component", f.Name, "error", f.Err)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}
