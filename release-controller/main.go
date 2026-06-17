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
	httpinfra "github.com/carolsimone/continuo/release-controller/adapters/http"
	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
	redisadapter "github.com/carolsimone/continuo/release-controller/adapters/redis"
	s3adapter "github.com/carolsimone/continuo/release-controller/adapters/s3"
	"github.com/carolsimone/continuo/release-controller/config"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/carolsimone/continuo/release-controller/service/uow"
)

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
	defer db.Close()

	rc, err := redisadapter.NewClient(ctx, redisadapter.Config{
		Host:     cfg.Redis.Host,
		Port:     cfg.Redis.Port,
		Password: cfg.Redis.Password,
	})
	if err != nil {
		logger.Error("redis connect", "error", err)
		os.Exit(1)
	}
	defer rc.Close()

	// S3 client for pruning candidate-SQL objects when releases are deleted.
	// Credentials are optional: when running on a cloud instance with an IAM
	// role the static provider is a no-op, so plain os.Getenv is used here
	// rather than the required-var validator.
	s3Client := s3adapter.NewS3Client(
		os.Getenv("S3_ENDPOINT_URL"),
		cfg.S3Bucket,
		os.Getenv("AWS_DEFAULT_REGION"),
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		logger,
	)

	deps := &handlers.Deps{
		NewUoW:    func() uow.UnitOfWork { return postgres.NewUnitOfWork(db, logger, s3Client) },
		Clock:     ports.SystemClock{},
		Telemetry: ports.NoOpTelemetry{},
		Logger:    logger,
		Bucket:    cfg.S3Bucket,
	}

	// Start outbox publisher — spawns its own goroutine internally and runs until
	// ctx is cancelled.
	redisadapter.StartOutboxPublisher(ctx, db, rc, logger)

	// Start stream consumers in goroutines; each blocks until ctx is cancelled.
	manifestConsumer := redisadapter.NewManifestLoadedCandidateConsumer(rc, deps, logger)
	go func() {
		if err := manifestConsumer.Start(ctx); err != nil && ctx.Err() == nil {
			logger.Error("manifest.loaded.candidate consumer stopped", "error", err)
		}
	}()

	validationConsumer := redisadapter.NewValidationCompletedConsumer(rc, deps, logger)
	go func() {
		if err := validationConsumer.Start(ctx); err != nil && ctx.Err() == nil {
			logger.Error("validation.completed consumer stopped", "error", err)
		}
	}()

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
	srv := httpinfra.NewServer(deps, cfg.HTTPPort, logger)
	if err := srv.Start(ctx); err != nil {
		logger.Error("http server error", "error", err)
		os.Exit(1)
	}
}
