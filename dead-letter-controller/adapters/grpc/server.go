// Package grpc serves the DeadLetterService gRPC API over the dead-letter
// application handlers.
package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"

	deadletterv1 "github.com/carolsimone/continuo/dead-letter-controller/api/deadletter/v1"
	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/dead-letter-controller/service/handlers"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// Server serves DeadLetterService. Start blocks serving the listener and
// Shutdown stops it gracefully, so a caller can run Start in one goroutine and
// Shutdown from a shutdown handler.
type Server struct {
	deadletterv1.UnimplementedDeadLetterServiceServer
	grpcServer *grpc.Server
	listener   net.Listener
	logger     *slog.Logger
	query      *handlers.Query
	redriver   *handlers.Redriver
}

// NewServer listens on the TCP port (0 picks a free one) and returns a server
// ready to Start.
func NewServer(port int, query *handlers.Query, redriver *handlers.Redriver, logger *slog.Logger) (*Server, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}
	return newServer(lis, query, redriver, logger), nil
}

// newServer builds a server over an existing listener.
func newServer(lis net.Listener, query *handlers.Query, redriver *handlers.Redriver, logger *slog.Logger) *Server {
	// The identity interceptor runs first so the caller's user id is on the
	// context before the logging interceptor or any handler reads it.
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			identity.UnaryServerInterceptor(),
			loggingInterceptor(logger),
		),
	)
	s := &Server{grpcServer: grpcServer, listener: lis, logger: logger, query: query, redriver: redriver}
	deadletterv1.RegisterDeadLetterServiceServer(grpcServer, s)
	reflection.Register(grpcServer)
	return s
}

// Addr returns the address the server is listening on.
func (s *Server) Addr() string { return s.listener.Addr().String() }

// Start serves requests until Shutdown is called, then returns nil.
func (s *Server) Start() error {
	s.logger.Info("Starting gRPC server", "addr", s.Addr())
	if err := s.grpcServer.Serve(s.listener); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}
	return nil
}

// Shutdown stops accepting calls and waits for in-flight ones to finish, or
// stops them when ctx ends first.
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("Shutting down gRPC server")
	done := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		s.grpcServer.Stop()
		<-done
	}
	return nil
}

func (s *Server) ListDeadLetters(ctx context.Context, req *deadletterv1.ListDeadLettersRequest) (*deadletterv1.ListDeadLettersResponse, error) {
	rows, open, err := s.query.List(ctx, deadletter.Filter{
		Source: deadletter.Source(req.GetSource()), Stream: req.GetStream(),
		Status: deadletter.Status(req.GetStatus()), Limit: int(req.GetLimit()),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &deadletterv1.ListDeadLettersResponse{TotalOpen: open}
	for _, dl := range rows {
		resp.DeadLetters = append(resp.DeadLetters, toProto(dl))
	}
	return resp, nil
}

func (s *Server) GetDeadLetter(ctx context.Context, req *deadletterv1.GetDeadLetterRequest) (*deadletterv1.GetDeadLetterResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id %q is not a dead letter id", req.GetId())
	}
	dl, err := s.query.Get(ctx, id)
	if err != nil {
		return nil, toStatus(err)
	}
	return &deadletterv1.GetDeadLetterResponse{
		DeadLetter: toProto(dl), Fields: dl.Fields,
		OriginalEventType: dl.OriginalEventType, FailedOutboxId: dl.FailedOutboxID,
	}, nil
}

func (s *Server) RedriveDeadLetters(ctx context.Context, req *deadletterv1.RedriveDeadLettersRequest) (*deadletterv1.RedriveDeadLettersResponse, error) {
	ids := make([]uuid.UUID, 0, len(req.GetIds()))
	for _, raw := range req.GetIds() {
		id, err := uuid.Parse(strings.TrimSpace(raw))
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "id %q is not a dead letter id", raw)
		}
		ids = append(ids, id)
	}
	rows, err := s.redriver.Redrive(ctx, ids, identity.FromContext(ctx).UserID, req.GetReason())
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &deadletterv1.RedriveDeadLettersResponse{}
	for _, dl := range rows {
		resp.DeadLetters = append(resp.DeadLetters, toProto(dl))
	}
	return resp, nil
}

// loggingInterceptor logs every unary call; a failed call is logged at error
// level only when the failure is the server's, not the caller's.
func loggingInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		logger.Info("gRPC request", "method", info.FullMethod)
		resp, err := handler(ctx, req)
		if err != nil {
			level := slog.LevelWarn
			if status.Code(err) == codes.Internal || status.Code(err) == codes.Unknown {
				level = slog.LevelError
			}
			logger.Log(ctx, level, "gRPC error", "method", info.FullMethod, "error", err)
		}
		return resp, err
	}
}
