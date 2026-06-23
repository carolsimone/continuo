package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/remediation/adapters/postgres"
	rredis "github.com/carolsimone/continuo/remediation/adapters/redis"
	rs3 "github.com/carolsimone/continuo/remediation/adapters/s3"
	"github.com/carolsimone/continuo/remediation/config"
	"github.com/carolsimone/continuo/remediation/service/handlers"
	"github.com/carolsimone/continuo/remediation/service/ports"
	"github.com/carolsimone/continuo/remediation/service/uow"
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

	rc, err := rredis.NewClient(ctx, rredis.Config{
		Host:     cfg.Redis.Host,
		Port:     cfg.Redis.Port,
		Password: cfg.Redis.Password,
	})
	if err != nil {
		logger.Error("redis connect", "error", err)
		os.Exit(1)
	}
	defer rc.Close()

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
	rredis.StartOutboxPublisher(ctx, db, rc, logger)

	// Start the release.rejected consumer in a goroutine; blocks until ctx is
	// cancelled.
	consumer := rredis.NewReleaseRejectedConsumer(rc, deps, logger)
	go func() {
		if err := consumer.Start(ctx); err != nil && ctx.Err() == nil {
			logger.Error("release.rejected consumer stopped", "error", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{Addr: ":" + cfg.HTTPPort, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()

	logger.Info("remediation service started", "http_port", cfg.HTTPPort)
	<-ctx.Done()
	_ = srv.Shutdown(context.Background())
	logger.Info("remediation service stopped")
}
