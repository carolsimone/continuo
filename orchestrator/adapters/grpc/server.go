package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	orchestratorv1 "github.com/carolsimone/continuo/orchestrator/api/orchestrator/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Server wraps the gRPC server with graceful shutdown support.
type Server struct {
	orchestratorv1.UnimplementedOrchestratorQueryServer
	grpcServer   *grpc.Server
	listener     net.Listener
	logger       *slog.Logger
	queryHandler *QueryHandler
}

// NewServer creates and configures a new gRPC server.
func NewServer(port int, queryHandler *QueryHandler, logger *slog.Logger) (*Server, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(loggingInterceptor(logger)))

	server := &Server{
		grpcServer:   grpcServer,
		listener:     listener,
		logger:       logger,
		queryHandler: queryHandler,
	}

	orchestratorv1.RegisterOrchestratorQueryServer(grpcServer, server)
	reflection.Register(grpcServer)

	return server, nil
}

// Start starts the gRPC server and blocks until stopped.
func (s *Server) Start() error {
	s.logger.Info("Starting gRPC server", "addr", s.listener.Addr().String())
	if err := s.grpcServer.Serve(s.listener); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}
	return nil
}

// Shutdown gracefully stops the gRPC server.
func (s *Server) Shutdown(_ context.Context) error {
	s.logger.Info("Shutting down gRPC server")
	s.grpcServer.GracefulStop()
	return nil
}

// Addr returns the server's listening address.
func (s *Server) Addr() string {
	return s.listener.Addr().String()
}

// ============================================================================
// IMPLEMENT OrchestratorQueryServer INTERFACE
// ============================================================================

// GetScheduleGraph delegates to the query handler.
func (s *Server) GetScheduleGraph(ctx context.Context, req *orchestratorv1.GetScheduleGraphRequest) (*orchestratorv1.GetScheduleGraphResponse, error) {
	return s.queryHandler.GetScheduleGraph(ctx, req)
}

// ListRuns delegates to the query handler.
func (s *Server) ListRuns(ctx context.Context, req *orchestratorv1.ListRunsRequest) (*orchestratorv1.ListRunsResponse, error) {
	return s.queryHandler.ListRuns(ctx, req)
}

// GetRunGraph delegates to the query handler.
func (s *Server) GetRunGraph(ctx context.Context, req *orchestratorv1.GetRunGraphRequest) (*orchestratorv1.GetRunGraphResponse, error) {
	return s.queryHandler.GetRunGraph(ctx, req)
}

// ============================================================================
// INTERCEPTORS
// ============================================================================

// loggingInterceptor logs all incoming gRPC requests and any errors.
func loggingInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		logger.Info("gRPC request", "method", info.FullMethod)
		resp, err := handler(ctx, req)
		if err != nil {
			logger.Error("gRPC error", "method", info.FullMethod, "error", err)
		}
		return resp, err
	}
}
