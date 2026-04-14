package main

import (
	"context"
	"log/slog"
	"os"
	"strings"

	httpadapter "github.com/carolsimone/continuo/graph/adapters/http"
	neo4jadapter "github.com/carolsimone/continuo/graph/adapters/neo4j"
	"github.com/carolsimone/continuo/graph/config"
	grpcserver "github.com/carolsimone/continuo/graph/internal/grpc"
	"github.com/carolsimone/continuo/graph/internal/grpc/handlers"
	"github.com/carolsimone/continuo/graph/internal/lifecycle"
	"github.com/carolsimone/continuo/graph/internal/sweeper"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	v := &pkgconfig.Validator{}
	cfg := config.Load(v)
	if missing := v.Missing(); len(missing) > 0 {
		logger.Error("missing required env vars", "vars", strings.Join(missing, ", "))
		os.Exit(1)
	}

	logger.Info("Starting graph service")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lifecycleManager := lifecycle.NewApplicationLifecycle(logger)
	lifecycleManager.SetupSignalHandlers(ctx, cancel)

	// Neo4j client
	neo4jClient, err := neo4jadapter.NewNeo4jClient(
		cfg.Neo4j.URI,
		cfg.Neo4j.User,
		cfg.Neo4j.Password,
		logger,
	)
	if err != nil {
		logger.Error("Failed to create Neo4j client", "error", err)
		os.Exit(1)
	}

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Closing Neo4j connection")
		return neo4jClient.Close(ctx)
	})

	// Repository and handler
	graphRepo := neo4jadapter.NewGraphRepository(neo4jClient, logger)
	graphHandler := handlers.NewGraphHandler(graphRepo, logger)

	// gRPC server
	grpcSrv, err := grpcserver.NewServer(cfg.GRPCPort, graphHandler, logger)
	if err != nil {
		logger.Error("Failed to create gRPC server", "error", err)
		os.Exit(1)
	}

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		return grpcSrv.Shutdown(ctx)
	})

	// HTTP health server
	healthSrv := httpadapter.NewServer(cfg.HealthPort, logger)

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		return healthSrv.Shutdown(ctx)
	})

	go func() {
		if err := healthSrv.Start(); err != nil {
			logger.Error("Health server error", "error", err)
		}
	}()

	runSweeper := sweeper.New(
		graphRepo,
		cfg.RunHistoryRetentionDays,
		cfg.RunSweeperIntervalMinutes,
		logger,
	)
	go runSweeper.Start(ctx)

	logger.Info("Graph service initialized, starting gRPC server",
		"grpc_port", cfg.GRPCPort,
		"health_port", cfg.HealthPort,
	)

	if err := grpcSrv.Start(); err != nil {
		logger.Error("gRPC server error", "error", err)
		os.Exit(1)
	}

	logger.Info("Graph service stopped")
}
