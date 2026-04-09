package main

import (
	"context"
	"log/slog"
	"os"

	httpadapter "github.com/carolsimone/continuo/graph/adapters/http"
	neo4jadapter "github.com/carolsimone/continuo/graph/adapters/neo4j"
	"github.com/carolsimone/continuo/graph/config"
	grpcserver "github.com/carolsimone/continuo/graph/internal/grpc"
	"github.com/carolsimone/continuo/graph/internal/grpc/handlers"
	"github.com/carolsimone/continuo/graph/internal/lifecycle"
	"github.com/carolsimone/continuo/graph/internal/sweeper"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	logger.Info("Starting graph service")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lifecycleManager := lifecycle.NewApplicationLifecycle(logger)
	lifecycleManager.SetupSignalHandlers(ctx, cancel)

	// Neo4j client
	neo4jClient, err := neo4jadapter.NewNeo4jClient(
		config.GetNeo4jURI(),
		config.GetNeo4jUser(),
		config.GetNeo4jPassword(),
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
	grpcSrv, err := grpcserver.NewServer(config.GetGRPCPort(), graphHandler, logger)
	if err != nil {
		logger.Error("Failed to create gRPC server", "error", err)
		os.Exit(1)
	}

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		return grpcSrv.Shutdown(ctx)
	})

	// HTTP health server
	healthSrv := httpadapter.NewServer(config.GetHealthPort(), logger)

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
		config.GetRunHistoryRetentionDays(),
		config.GetRunSweeperIntervalMinutes(),
		logger,
	)
	go runSweeper.Start(ctx)

	logger.Info("Graph service initialized, starting gRPC server",
		"grpc_port", config.GetGRPCPort(),
		"health_port", config.GetHealthPort(),
	)

	if err := grpcSrv.Start(); err != nil {
		logger.Error("gRPC server error", "error", err)
		os.Exit(1)
	}

	logger.Info("Graph service stopped")
}
