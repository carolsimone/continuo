package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/carolsimone/continuo/agent-runner/adapters/anthropic"
	"github.com/carolsimone/continuo/agent-runner/adapters/cliexec"
	"github.com/carolsimone/continuo/agent-runner/adapters/grpcserver"
	agenthttp "github.com/carolsimone/continuo/agent-runner/adapters/http"
	adapteropenai "github.com/carolsimone/continuo/agent-runner/adapters/openai"
	"github.com/carolsimone/continuo/agent-runner/adapters/postgres"
	"github.com/carolsimone/continuo/agent-runner/adapters/ratelimit"
	"github.com/carolsimone/continuo/agent-runner/adapters/s3"
	"github.com/carolsimone/continuo/agent-runner/config"
	"github.com/carolsimone/continuo/agent-runner/internal/lifecycle"
	agentchatv1 "github.com/carolsimone/continuo/agent-runner/proto/agentchat/v1"
	"github.com/carolsimone/continuo/agent-runner/service/chat"
	"github.com/carolsimone/continuo/agent-runner/service/ports"
	"github.com/carolsimone/continuo/agent-runner/service/retention"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/liveness"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	v := &pkgconfig.Validator{}
	cfg := config.Load(v)
	if missing := v.Missing(); len(missing) > 0 {
		logger.Error("missing required configuration", "vars", strings.Join(missing, ", "))
		os.Exit(1)
	}

	if cfg.LLMAPIKey == "" {
		logger.Warn("LLM_API_KEY is not set; agent-runner will start and serve gRPC, but chat sessions will fail until a key is supplied")
	}

	logger.Info("Starting agent-runner service")

	// Create root context cancelled by shutdown signal.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize lifecycle manager and liveness registry. The lifecycle manager
	// drives the ordered graceful shutdown (stop intake → drain → close infra);
	// the registry tracks dependency probes and feeds the /ready probe.
	lifecycleManager := lifecycle.NewApplicationLifecycle(logger)
	lifecycleManager.SetupSignalHandlers(cancel, cfg.ShutdownGrace)

	liveReg := liveness.NewRegistry()

	// Postgres connection.
	db, err := sqlx.Connect("postgres", cfg.Postgres.DSN())
	if err != nil {
		logger.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	logger.Info("postgres connection established")

	// Register database cleanup (LIFO — runs after gRPC and health servers close).
	lifecycleManager.RegisterShutdownHandler(func(_ context.Context) error {
		logger.Info("Closing database connection")
		return db.Close()
	})

	// Postgres readiness probe: reports NOT ready when Postgres is unreachable.
	liveReg.AddProbe("postgres", 5*time.Second, func(ctx context.Context) error {
		return db.PingContext(ctx)
	})

	// CLI catalog: runs `continuo describe` to discover available tools.
	catalog, err := cliexec.LoadCatalog(ctx, cfg.CLIPath)
	if err != nil {
		logger.Error("failed to load CLI catalog", "cli_path", cfg.CLIPath, "error", err)
		os.Exit(1)
	}
	logger.Info("CLI catalog loaded", "tools", len(catalog.Tools()))

	// os.Environ() returns a fresh copy of the environment slice, so the append
	// below is safe and does not alias the process environment. The initiating
	// identity is not baked in here: Execute stamps CONTINUO_ACTOR per call from
	// the authenticated chat operator, so each action is attributed to the human
	// who approved it (empty → state's system sentinel).
	cliEnv := append(
		os.Environ(),
		"CONTINUO_STATE_ADDR="+cfg.StateAddr,
		"CONTINUO_ORCHESTRATOR_ADDR="+cfg.OrchestratorAddr,
	)
	executor := cliexec.NewExecutor(catalog, cfg.CLIPath, cliEnv, cfg.ToolTimeout, cfg.ToolResultMaxBytes, logger)

	// Shared HTTP client for LLM providers. No overall Timeout is set because
	// streaming responses can run for minutes; context cancellation (interrupt /
	// turn ctx) is the primary backstop. Transport-level timeouts guard the
	// socket layer so a dead connection does not hang indefinitely.
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext:           (&net.Dialer{Timeout: 30 * time.Second}).DialContext,
			ResponseHeaderTimeout: 30 * time.Second,
		},
	}

	// LLM provider selection.
	var provider ports.LLMProvider
	switch cfg.LLMProvider {
	case "anthropic":
		provider = anthropic.NewProvider("https://api.anthropic.com", cfg.LLMAPIKey, cfg.LLMModel, httpClient)
	case "openai":
		provider = adapteropenai.NewProvider("https://api.openai.com", cfg.LLMAPIKey, cfg.LLMModel, httpClient)
	case "openai-compatible":
		provider = adapteropenai.NewProvider(cfg.LLMBaseURL, cfg.LLMAPIKey, cfg.LLMModel, httpClient)
	default:
		logger.Error("unsupported LLM provider", "provider", cfg.LLMProvider)
		os.Exit(1)
	}

	repo := postgres.NewThreadRepository(db)

	// Rate limiter. With more than one replica the per-user limit must be
	// enforced from a shared store, otherwise it degrades to per-replica. When
	// REDIS_ADDR is set we use the Redis-backed limiter (global across
	// replicas); otherwise the process-local in-memory limiter (single replica).
	var limiter ports.RateLimiter
	if cfg.RedisAddr != "" {
		redisClient := goredis.NewClient(&goredis.Options{
			Addr:     cfg.RedisAddr,
			Password: cfg.RedisPassword,
			DB:       0,
		})
		if err := redisClient.Ping(ctx).Err(); err != nil {
			logger.Error("failed to connect to Redis for rate limiting", "addr", cfg.RedisAddr, "error", err)
			os.Exit(1)
		}
		logger.Info("Redis connection established for shared rate limiting", "addr", cfg.RedisAddr)
		lifecycleManager.RegisterShutdownHandler(func(_ context.Context) error {
			logger.Info("Closing Redis connection")
			return redisClient.Close()
		})
		limiter = ratelimit.NewRedis(redisClient, cfg.RateLimitPerMinute)
	} else {
		logger.Info("REDIS_ADDR unset; using in-memory rate limiter (single replica)")
		limiter = ratelimit.NewMemory(cfg.RateLimitPerMinute)
	}

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

	// Retention job sweeps idle threads on a configurable interval. It stops
	// when the root context is cancelled.
	go retention.NewJob(repo, archiver, cfg.RetentionDays, logger).Run(ctx, cfg.RetentionSweepEvery)

	deps := chat.Deps{
		Provider: provider,
		Catalog:  catalog,
		Executor: executor,
		Repo:     repo,
		Limiter:  limiter,
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

	// Stream panic-recovery interceptor: a panic inside a streaming session
	// handler must not crash the process. It logs the stack, then returns
	// codes.Internal to the client so the stream is cleanly terminated.
	streamRecovery := func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("panic in gRPC stream handler", "panic", fmt.Sprintf("%v", r), "stack", string(debug.Stack()))
				err = status.Errorf(codes.Internal, "internal server error")
			}
		}()
		return handler(srv, ss)
	}

	// gRPC server with the stream panic-recovery interceptor.
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		logger.Error("failed to listen for gRPC", "port", cfg.GRPCPort, "error", err)
		os.Exit(1)
	}
	grpcSrv := grpc.NewServer(grpc.StreamInterceptor(streamRecovery))
	agentchatv1.RegisterAgentChatServer(grpcSrv, grpcserver.NewServer(deps, logger, cfg.MaxConcurrentSessions))

	// Register gRPC server shutdown (LIFO order: gRPC first, then health, then DB).
	lifecycleManager.RegisterShutdownHandler(func(_ context.Context) error {
		logger.Info("initiating graceful gRPC shutdown")
		shutdownDone := make(chan struct{})
		go func() {
			grpcSrv.GracefulStop()
			close(shutdownDone)
		}()
		select {
		case <-shutdownDone:
		case <-time.After(cfg.ShutdownGrace):
			logger.Warn("gRPC shutdown grace period exceeded; forcing stop")
			grpcSrv.Stop()
		}
		return nil
	})

	// Start gRPC server in a goroutine; errors are logged but the process lets
	// the lifecycle manager drive the exit.
	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			logger.Error("gRPC server error", "error", err)
		}
	}()

	// HTTP health server: /health is a plain process-up probe. /ready is backed
	// by the liveness registry so traffic stops when Postgres is unreachable;
	// /livez is the liveness probe and has no workers or heartbeats registered
	// here, so it stays 200.
	healthServer := agenthttp.NewServer(cfg.HealthPort, liveReg, logger)

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		return healthServer.Shutdown(ctx)
	})

	go func() {
		if err := healthServer.Start(); err != nil {
			logger.Error("health server error", "error", err)
		}
	}()

	logger.Info("agent-runner started",
		"grpc_port", cfg.GRPCPort,
		"health_port", cfg.HealthPort,
		"llm_provider", cfg.LLMProvider,
		"llm_model", cfg.LLMModel,
	)

	// Block until the graceful-shutdown sequence has fully completed. The signal
	// handler cancels ctx (stops intake), drains in-flight goroutines, then runs
	// the infra-close handlers; Done() closes only after that sequence finishes.
	<-lifecycleManager.Done()
	logger.Info("agent-runner stopped")
}
