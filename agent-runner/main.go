package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/carolsimone/continuo/agent-runner/adapters/anthropic"
	"github.com/carolsimone/continuo/agent-runner/adapters/cliexec"
	"github.com/carolsimone/continuo/agent-runner/adapters/grpcserver"
	adapteropenai "github.com/carolsimone/continuo/agent-runner/adapters/openai"
	"github.com/carolsimone/continuo/agent-runner/adapters/postgres"
	"github.com/carolsimone/continuo/agent-runner/adapters/s3"
	"github.com/carolsimone/continuo/agent-runner/config"
	agentchatv1 "github.com/carolsimone/continuo/agent-runner/proto/agentchat/v1"
	"github.com/carolsimone/continuo/agent-runner/service/chat"
	"github.com/carolsimone/continuo/agent-runner/service/ports"
	"github.com/carolsimone/continuo/agent-runner/service/retention"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"google.golang.org/grpc"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	v := &pkgconfig.Validator{}
	cfg := config.Load(v)
	if missing := v.Missing(); len(missing) > 0 {
		logger.Error("missing required configuration", "vars", strings.Join(missing, ", "))
		os.Exit(1)
	}

	// Root context cancelled on SIGTERM/SIGINT so background jobs stop cleanly.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
		<-ch
		logger.Info("shutdown signal received")
		cancel()
	}()

	// Postgres connection.
	db, err := sqlx.Connect("postgres", cfg.Postgres.DSN())
	if err != nil {
		logger.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	logger.Info("postgres connection established")

	// CLI catalog: runs `continuo describe` to discover available tools.
	catalog, err := cliexec.LoadCatalog(ctx, cfg.CLIPath)
	if err != nil {
		logger.Error("failed to load CLI catalog", "cli_path", cfg.CLIPath, "error", err)
		os.Exit(1)
	}
	logger.Info("CLI catalog loaded", "tools", len(catalog.Tools()))

	// The CLI executor inherits the process environment plus service addresses so
	// the continuo binary can reach state and orchestrator.
	cliEnv := append(
		os.Environ(),
		"CONTINUO_STATE_ADDR="+cfg.StateAddr,
		"CONTINUO_ORCHESTRATOR_ADDR="+cfg.OrchestratorAddr,
	)
	executor := cliexec.NewExecutor(catalog, cfg.CLIPath, cliEnv, cfg.ToolTimeout, cfg.ToolResultMaxBytes, logger)

	// LLM provider selection.
	var provider ports.LLMProvider
	switch cfg.LLMProvider {
	case "anthropic":
		provider = anthropic.NewProvider("https://api.anthropic.com", cfg.LLMAPIKey, cfg.LLMModel, nil)
	case "openai":
		provider = adapteropenai.NewProvider("https://api.openai.com", cfg.LLMAPIKey, cfg.LLMModel, nil)
	case "openai-compatible":
		provider = adapteropenai.NewProvider(cfg.LLMBaseURL, cfg.LLMAPIKey, cfg.LLMModel, nil)
	default:
		logger.Error("unsupported LLM provider", "provider", cfg.LLMProvider)
		os.Exit(1)
	}

	repo := postgres.NewThreadRepository(db)

	// Optional S3 archiver for thread retention.
	var archiver ports.Archiver
	if cfg.RetentionArchiveS3 {
		sv := &pkgconfig.Validator{}
		s3cfg := pkgconfig.LoadS3(sv)
		if missing := sv.Missing(); len(missing) > 0 {
			logger.Error("S3 archive enabled but configuration missing", "vars", strings.Join(missing, ", "))
			os.Exit(1)
		}
		archiver = s3.NewArchiver(s3cfg.EndpointURL, s3cfg.Bucket, s3cfg.Region, s3cfg.AccessKeyID, s3cfg.SecretAccessKey)
	}

	// Retention job sweeps idle threads on a configurable interval.
	go retention.NewJob(repo, archiver, cfg.RetentionDays, logger).Run(ctx, cfg.RetentionSweepEvery)

	deps := chat.Deps{
		Provider: provider,
		Catalog:  catalog,
		Executor: executor,
		Repo:     repo,
		Limiter:  chat.NewRateLimiter(cfg.RateLimitPerMinute),
		Logger:   logger,
		Cfg: chat.Config{
			SystemPrompt:  cfg.SystemPrompt,
			MaxIterations: cfg.MaxIterations,
			MaxTurnTokens: cfg.MaxTurnTokens,
			WindowTokens:  cfg.WindowTokens,
			ConfirmTTL:    cfg.ConfirmTTL,
			CLIName:       "continuo",
		},
	}

	// HTTP health endpoint.
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		addr := fmt.Sprintf(":%d", cfg.HealthPort)
		logger.Info("starting health server", "addr", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			logger.Error("health server error", "error", err)
		}
	}()

	// gRPC server.
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		logger.Error("failed to listen for gRPC", "port", cfg.GRPCPort, "error", err)
		os.Exit(1)
	}

	grpcSrv := grpc.NewServer()
	agentchatv1.RegisterAgentChatServer(grpcSrv, grpcserver.NewServer(deps, logger))

	// Graceful shutdown: wait for root context to be cancelled, then allow
	// in-flight RPCs cfg.ShutdownGrace to drain before force-stopping.
	go func() {
		<-ctx.Done()
		logger.Info("initiating graceful gRPC shutdown")
		shutdownDone := make(chan struct{})
		go func() {
			grpcSrv.GracefulStop()
			close(shutdownDone)
		}()
		select {
		case <-shutdownDone:
		case <-time.After(cfg.ShutdownGrace):
			logger.Warn("shutdown grace period exceeded; forcing stop")
			grpcSrv.Stop()
		}
	}()

	logger.Info("agent-runner started",
		"grpc_port", cfg.GRPCPort,
		"health_port", cfg.HealthPort,
		"llm_provider", cfg.LLMProvider,
		"llm_model", cfg.LLMModel,
	)

	if err := grpcSrv.Serve(lis); err != nil {
		logger.Error("gRPC server error", "error", err)
		os.Exit(1)
	}
	logger.Info("agent-runner stopped")
}
