package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/pkg/maintenance/grpcgate"
	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Server serves run.v1.RunService on one listener, with graceful shutdown.
type Server struct {
	grpcServer *grpc.Server
	listener   net.Listener
	logger     *slog.Logger
}

// MaintenanceGatedMethods are the RPCs that start a run. While maintenance mode
// is on they are refused before their handler runs; cancels and reads,
// PreviewRebase among them, are not.
var MaintenanceGatedMethods = []string{
	runv1.RunService_StartRun_FullMethodName,
	runv1.RunService_TriggerRerun_FullMethodName,
	runv1.RunService_TriggerRebase_FullMethodName,
	runv1.RunService_TriggerSingleNodeRun_FullMethodName,
}

// NewServer creates a gRPC server on port that serves runService. With
// maintenance on it refuses every RPC MaintenanceGatedMethods names.
func NewServer(port int, runService runv1.RunServiceServer, logger *slog.Logger, maintenance bool) (*Server, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}

	// The identity interceptor runs first so it converts the initiating-user
	// metadata header into an identity.Identity on the context before any
	// handler (or the logging interceptor) observes the request. The
	// maintenance gate runs last so a refused call is still logged.
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			identity.UnaryServerInterceptor(),
			loggingInterceptor(logger),
			grpcgate.UnaryServerInterceptor(maintenance, MaintenanceGatedMethods...),
		),
	)

	server := &Server{grpcServer: grpcServer, listener: listener, logger: logger}

	runv1.RegisterRunServiceServer(grpcServer, runService)

	// Enable reflection for grpcurl/testing
	reflection.Register(grpcServer)

	return server, nil
}

// Addr returns the address the server is listening on
func (s *Server) Addr() string {
	return s.listener.Addr().String()
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
