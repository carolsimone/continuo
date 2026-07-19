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
	// deploy/continuo/values.yaml). Tracks background workers (consumers,
	// outbox publisher) plus cached dependency probes, so a wedged or exited
	// consumer restarts the pod instead of leaving it at 1/1 Running with a
	// dead background loop nothing else can see.
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
