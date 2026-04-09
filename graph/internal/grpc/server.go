package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	graphv1 "github.com/carolsimone/continuo/graph/api/graph/v1"
	"github.com/carolsimone/continuo/graph/internal/grpc/handlers"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Server wraps gRPC server with graceful shutdown
type Server struct {
	graphv1.UnimplementedGraphServiceServer
	grpcServer   *grpc.Server
	listener     net.Listener
	logger       *slog.Logger
	graphHandler *handlers.GraphHandler
}

// NewServer creates a new gRPC server
func NewServer(
	port int,
	graphHandler *handlers.GraphHandler,
	logger *slog.Logger,
) (*Server, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(loggingInterceptor(logger)),
	)

	// Register service implementation
	server := &Server{
		grpcServer:   grpcServer,
		listener:     listener,
		logger:       logger,
		graphHandler: graphHandler,
	}

	graphv1.RegisterGraphServiceServer(grpcServer, server)

	// Enable reflection for grpcurl/testing
	reflection.Register(grpcServer)

	return server, nil
}

// Start starts the gRPC server
func (s *Server) Start() error {
	s.logger.Info("Starting gRPC server", "addr", s.listener.Addr().String())
	if err := s.grpcServer.Serve(s.listener); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}
	return nil
}

// Shutdown gracefully shuts down the gRPC server
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("Shutting down gRPC server")
	s.grpcServer.GracefulStop()
	return nil
}

// Addr returns the server's listening address
func (s *Server) Addr() string {
	return s.listener.Addr().String()
}

// ============================================================================
// IMPLEMENT GRAPH SERVICE SERVER INTERFACE
// ============================================================================

// CreateNode delegates to graph handler
func (s *Server) CreateNode(ctx context.Context, req *graphv1.CreateNodeRequest) (*graphv1.NodeResponse, error) {
	return s.graphHandler.CreateNode(ctx, req)
}

// UpdateNodeTimestamp delegates to graph handler
func (s *Server) UpdateNodeTimestamp(ctx context.Context, req *graphv1.UpdateNodeTimestampRequest) (*graphv1.NodeResponse, error) {
	return s.graphHandler.UpdateNodeTimestamp(ctx, req)
}

// GetStaleRootNodes delegates to graph handler
func (s *Server) GetStaleRootNodes(ctx context.Context, req *graphv1.GetStaleRootNodesRequest) (*graphv1.GetStaleRootNodesResponse, error) {
	return s.graphHandler.GetStaleRootNodes(ctx, req)
}

// GetDownstreamDependencies delegates to graph handler
func (s *Server) GetDownstreamDependencies(ctx context.Context, req *graphv1.GetDownstreamDependenciesRequest) (*graphv1.GetDownstreamDependenciesResponse, error) {
	return s.graphHandler.GetDownstreamDependencies(ctx, req)
}

// CheckUpstreamFreshness delegates to graph handler
func (s *Server) CheckUpstreamFreshness(ctx context.Context, req *graphv1.CheckUpstreamFreshnessRequest) (*graphv1.CheckUpstreamFreshnessResponse, error) {
	return s.graphHandler.CheckUpstreamFreshness(ctx, req)
}

// GetScheduleGraph delegates to graph handler
func (s *Server) GetScheduleGraph(ctx context.Context, req *graphv1.GetScheduleGraphRequest) (*graphv1.GetScheduleGraphResponse, error) {
	return s.graphHandler.GetScheduleGraph(ctx, req)
}

// GetScheduleInitNodes delegates to graph handler
func (s *Server) GetScheduleInitNodes(ctx context.Context, req *graphv1.GetScheduleInitNodesRequest) (*graphv1.GetScheduleInitNodesResponse, error) {
	return s.graphHandler.GetScheduleInitNodes(ctx, req)
}

// UpdateNodeStatus delegates to graph handler
func (s *Server) UpdateNodeStatus(ctx context.Context, req *graphv1.UpdateNodeStatusRequest) (*graphv1.UpdateNodeStatusResponse, error) {
	return s.graphHandler.UpdateNodeStatus(ctx, req)
}

// GetReadyDownstream delegates to graph handler
func (s *Server) GetReadyDownstream(ctx context.Context, req *graphv1.GetReadyDownstreamRequest) (*graphv1.GetReadyDownstreamResponse, error) {
	return s.graphHandler.GetReadyDownstream(ctx, req)
}

// CheckScheduleCompletion delegates to graph handler
func (s *Server) CheckScheduleCompletion(ctx context.Context, req *graphv1.CheckScheduleCompletionRequest) (*graphv1.CheckScheduleCompletionResponse, error) {
	return s.graphHandler.CheckScheduleCompletion(ctx, req)
}

// SnapshotGraph delegates to graph handler
func (s *Server) SnapshotGraph(ctx context.Context, req *graphv1.SnapshotGraphRequest) (*graphv1.SnapshotGraphResponse, error) {
	return s.graphHandler.SnapshotGraph(ctx, req)
}

func (s *Server) FinalizeRun(ctx context.Context, req *graphv1.FinalizeRunRequest) (*graphv1.FinalizeRunResponse, error) {
	return s.graphHandler.FinalizeRun(ctx, req)
}

func (s *Server) ListRuns(ctx context.Context, req *graphv1.ListRunsRequest) (*graphv1.ListRunsResponse, error) {
	return s.graphHandler.ListRuns(ctx, req)
}

func (s *Server) GetRunGraph(ctx context.Context, req *graphv1.GetRunGraphRequest) (*graphv1.GetRunGraphResponse, error) {
	return s.graphHandler.GetRunGraph(ctx, req)
}

// GetTransitiveDownstream delegates to graph handler
func (s *Server) GetTransitiveDownstream(ctx context.Context, req *graphv1.GetTransitiveDownstreamRequest) (*graphv1.GetTransitiveDownstreamResponse, error) {
	return s.graphHandler.GetTransitiveDownstream(ctx, req)
}

// ============================================================================
// INTERCEPTORS
// ============================================================================

// loggingInterceptor logs all gRPC requests
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
