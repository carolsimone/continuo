package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/liveness"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	httpinfra "github.com/carolsimone/continuo/release-controller/adapters/http"
	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
	redisadapter "github.com/carolsimone/continuo/release-controller/adapters/redis"
	s3adapter "github.com/carolsimone/continuo/release-controller/adapters/s3"
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

// consumerHeartbeatStale is the liveness heartbeat budget: how long a consumer's
// read loop may make no progress before the liveness probe restarts the pod. It
// MUST exceed consumerHandlerTimeout plus a margin so a legitimately in-flight
// handler never trips liveness (the heartbeat advances per handler attempt; see
// pkg/redis.StreamConsumer.safeInvoke), while a true wedge still trips within
// budget.
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
	// outage stops traffic but must not restart a pod whose consumers are
	// already retrying).
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

	rc, err := redisadapter.NewClient(ctx, redisadapter.Config{
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

	// S3 client for pruning candidate-SQL objects when releases are deleted.
	s3Client := s3adapter.NewS3Client(
		cfg.S3.EndpointURL,
		cfg.S3.Bucket,
		cfg.S3.Region,
		cfg.S3.AccessKeyID,
		cfg.S3.SecretAccessKey,
		logger,
	)

	deps := &handlers.Deps{
		NewUoW:    func() uow.UnitOfWork { return postgres.NewUnitOfWork(db, logger, s3Client) },
		Clock:     ports.SystemClock{},
		Telemetry: ports.NoOpTelemetry{},
		Logger:    logger,
		Bucket:    cfg.S3.Bucket,
	}

	// Start outbox publisher — spawns its own goroutine internally and runs until
	// ctx is cancelled.
	redisadapter.StartOutboxPublisher(ctx, db, rc, liveReg, logger)

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
				n, err := handlers.PruneResolvedReleases(ctx, deps, retentionDays)
				if err != nil {
					logger.Error("retention prune failed", "error", err)
					continue
				}
				if n > 0 {
					logger.Info("pruned resolved releases", "count", n)
				}
			}
		}
	}()

	// HTTP server blocks until ctx is cancelled (graceful 5-second shutdown).
	srv := httpinfra.NewServer(deps, liveReg, cfg.HTTPPort, logger)
	if err := srv.Start(ctx); err != nil {
		logger.Error("http server error", "error", err)
		os.Exit(1)
	}
}
