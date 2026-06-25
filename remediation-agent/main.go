package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	ragithub "github.com/carolsimone/continuo/remediation-agent/adapters/github"
	grpcadapter "github.com/carolsimone/continuo/remediation-agent/adapters/grpc"
	"github.com/carolsimone/continuo/remediation-agent/adapters/llm"
	"github.com/carolsimone/continuo/remediation-agent/adapters/postgres"
	rredis "github.com/carolsimone/continuo/remediation-agent/adapters/redis"
	"github.com/carolsimone/continuo/remediation-agent/adapters/s3"
	"github.com/carolsimone/continuo/remediation-agent/adapters/sanitizer"
	remediationv1 "github.com/carolsimone/continuo/remediation-agent/api/remediation/v1"
	"github.com/carolsimone/continuo/remediation-agent/config"
	"github.com/carolsimone/continuo/remediation-agent/service/handlers"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
	"github.com/carolsimone/continuo/remediation-agent/service/proposals"
	"github.com/carolsimone/continuo/remediation-agent/service/uow"
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

	db, err := postgres.NewDB(cfg.Postgres)
	if err != nil {
		logger.Error("postgres connect", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	rc, err := rredis.NewClient(ctx, rredis.Config{
		Host:     cfg.Redis.Host,
		Port:     strconv.Itoa(cfg.Redis.Port),
		Password: cfg.Redis.Password,
	})
	if err != nil {
		logger.Error("redis connect", "error", err)
		os.Exit(1)
	}
	defer rc.Close()

	store := s3.NewS3(
		cfg.S3.EndpointURL,
		cfg.S3.Bucket,
		cfg.S3.Region,
		cfg.S3.AccessKeyID,
		cfg.S3.SecretAccessKey,
	)

	ancestryClient, err := grpcadapter.NewAncestryClient(cfg.OrchestratorAddr)
	if err != nil {
		logger.Error("grpc ancestry client dial", "error", err)
		os.Exit(1)
	}

	llmProvider, err := llm.NewProvider(cfg.LLMProvider, cfg.LLMAPIKey, cfg.LLMModel, cfg.LLMBaseURL, nil)
	if err != nil {
		logger.Error("llm provider init", "error", err)
		os.Exit(1)
	}

	deps := handlers.Deps{
		NewUoW:           func() uow.UnitOfWork { return postgres.NewUnitOfWork(db, logger) },
		LLM:              llmProvider,
		Evidence:         store,
		Ancestry:         ancestryClient,
		Source:           ragithub.NewSourceReader(cfg.GitHubBaseURL, cfg.GitHubToken, http.DefaultClient),
		Sanitizer:        sanitizer.Passthrough{},
		Artifacts:        store,
		Clock:            ports.SystemClock{},
		Logger:           logger,
		MaxAttempts:      cfg.MaxAttempts,
		Bucket:           cfg.S3.Bucket,
		ServiceRepoPaths: cfg.ServiceRepoPaths,
	}

	// Start the outbox publisher; spawns its own goroutine internally and runs
	// until ctx is cancelled.
	rredis.StartOutboxPublisher(ctx, db, rc, logger)

	// Start the remediation.requested consumer in a goroutine; blocks until ctx
	// is cancelled.
	consumer := rredis.NewRemediationRequestedConsumer(rc, deps, logger)
	go func() {
		if err := consumer.Start(ctx); err != nil && ctx.Err() == nil {
			logger.Error("remediation.requested consumer stopped", "error", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{Addr: ":" + cfg.HTTPPort, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()

	// Start the RemediationProposals gRPC server. The proposal service uses a
	// DB-bound (non-transactional) repository for reads and the UoW factory for
	// write operations, matching the consumer's wiring above.
	proposalRepo := postgres.NewProposalRepository(db)
	proposalSvc := proposals.New(proposals.Deps{
		Repo:   proposalRepo,
		NewUoW: func() uow.UnitOfWork { return postgres.NewUnitOfWork(db, logger) },
		Clock:  ports.SystemClock{},
	})
	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		logger.Error("grpc listen", "error", err)
		os.Exit(1)
	}
	grpcSrv := grpc.NewServer()
	remediationv1.RegisterRemediationProposalsServer(grpcSrv, grpcadapter.NewProposalsServer(proposalSvc))
	go func() {
		if err := grpcSrv.Serve(lis); err != nil && ctx.Err() == nil {
			logger.Error("grpc server stopped", "error", err)
		}
	}()

	logger.Info("remediation-agent started", "http_port", cfg.HTTPPort, "grpc_port", cfg.GRPCPort)
	<-ctx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	grpcSrv.GracefulStop()
	_ = srv.Shutdown(shutdownCtx)
	logger.Info("remediation-agent stopped")
}
