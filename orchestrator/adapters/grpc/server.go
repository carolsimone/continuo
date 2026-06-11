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

// Server wraps the gRPC server with graceful shutdown support. It owns the
// listener and *grpc.Server lifecycle only; the OrchestratorQuery RPCs are
// served directly by the *QueryHandler registered in NewServer, so adding a
// new RPC requires implementing it once on QueryHandler — there is no second
// delegate layer to keep in sync.
type Server struct {
	grpcServer *grpc.Server
	listener   net.Listener
	logger     *slog.Logger
}

// NewServer creates and configures a new gRPC server, registering the
// QueryHandler as the OrchestratorQuery service implementation.
func NewServer(port int, queryHandler *QueryHandler, logger *slog.Logger) (*Server, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(loggingInterceptor(logger)))

	server := &Server{
		grpcServer: grpcServer,
		listener:   listener,
		logger:     logger,
	}

	orchestratorv1.RegisterOrchestratorQueryServer(grpcServer, queryHandler)
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
