package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/release-controller/adapters/serialization"
	releasev1 "github.com/carolsimone/continuo/release-controller/api/release/v1"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/service/uow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// ReleaseQueryServer serves release.v1.ReleaseQuery, the read-only gRPC view of
// a candidate release. Start blocks serving the listener and Shutdown stops it
// gracefully, so a caller can run Start in one goroutine and Shutdown from a
// shutdown handler.
type ReleaseQueryServer struct {
	releasev1.UnimplementedReleaseQueryServer
	grpcServer *grpc.Server
	listener   net.Listener
	logger     *slog.Logger
	newUoW     func() uow.UnitOfWork
}

// NewReleaseQueryServer listens on the TCP port (0 picks a free one) and
// returns a server ready to Start. Each call reads through a unit of work of
// its own, which is never begun: the read uses the connection pool directly.
func NewReleaseQueryServer(port int, newUoW func() uow.UnitOfWork, logger *slog.Logger) (*ReleaseQueryServer, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}
	// The identity interceptor runs first so the caller's user id is on the
	// context before the logging interceptor or any handler reads it.
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(identity.UnaryServerInterceptor(), loggingInterceptor(logger)),
	)
	s := &ReleaseQueryServer{grpcServer: grpcServer, listener: lis, logger: logger, newUoW: newUoW}
	releasev1.RegisterReleaseQueryServer(grpcServer, s)
	reflection.Register(grpcServer)
	return s, nil
}

// Addr returns the address the server is listening on.
func (s *ReleaseQueryServer) Addr() string { return s.listener.Addr().String() }

// Start serves requests until Shutdown is called, then returns nil.
func (s *ReleaseQueryServer) Start() error {
	s.logger.Info("Starting gRPC server", "addr", s.Addr())
	if err := s.grpcServer.Serve(s.listener); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}
	return nil
}

// Shutdown stops accepting calls and waits for in-flight ones to finish, or
// stops them when ctx ends first.
func (s *ReleaseQueryServer) Shutdown(ctx context.Context) error {
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

// GetReleaseNotices returns the notices of a candidate release. A verification
// run's id is unknown here, as it is to GET /releases/{id}.
func (s *ReleaseQueryServer) GetReleaseNotices(ctx context.Context, req *releasev1.GetReleaseNoticesRequest) (*releasev1.GetReleaseNoticesResponse, error) {
	id := req.GetReleaseId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "release_id is required")
	}
	rel, err := s.newUoW().RunRepo().Get(ctx, id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read release %q: %v", id, err)
	}
	if rel == nil || rel.Kind() != pipeline.KindCandidate {
		return nil, status.Errorf(codes.NotFound, "release %q not found", id)
	}
	return &releasev1.GetReleaseNoticesResponse{
		ReleaseId: rel.ID(),
		Status:    string(rel.Status()),
		Notices:   noticesToProto(serialization.NoticesFromDomain(rel.Notices())),
	}, nil
}

// noticesToProto maps the notices' wire DTOs, which already carry the wire
// rules (the run operation spelled out, no nil list), onto the proto messages.
func noticesToProto(in []serialization.NoticeDTO) []*releasev1.ReleaseNotice {
	out := make([]*releasev1.ReleaseNotice, 0, len(in))
	for _, n := range in {
		rebuilds := make([]*releasev1.Rebuild, 0, len(n.Rebuilds))
		for _, r := range n.Rebuilds {
			rebuilds = append(rebuilds, &releasev1.Rebuild{NodeId: r.NodeID, Materialized: r.Materialized, Operation: r.Operation, Schedule: r.Schedule})
		}
		refresh := make([]*releasev1.ScheduledRefresh, 0, len(n.RefreshOnSchedule))
		for _, b := range n.RefreshOnSchedule {
			refresh = append(refresh, &releasev1.ScheduledRefresh{NodeId: b.NodeID, Materialized: b.Materialized, Schedule: b.Schedule})
		}
		outside := make([]*releasev1.OutsideObject, 0, len(n.Warehouse.OutsideContinuo))
		for _, o := range n.Warehouse.OutsideContinuo {
			outside = append(outside, &releasev1.OutsideObject{Object: o.Object, Kind: o.Kind, Owner: o.Owner})
		}
		out = append(out, &releasev1.ReleaseNotice{
			Kind:              n.Kind,
			NodeId:            n.NodeID,
			Service:           n.Service,
			EngineFamily:      n.EngineFamily,
			Change:            n.Change,
			Columns:           &releasev1.ColumnChanges{Added: n.Columns.Added, Removed: n.Columns.Removed, Retyped: n.Columns.Retyped},
			Load:              &releasev1.SeedLoad{Operation: n.Load.Operation, DropsTable: n.Load.DropsTable, Reason: n.Load.Reason},
			Rebuilds:          rebuilds,
			RefreshOnSchedule: refresh,
			Warehouse: &releasev1.WarehouseFindings{
				Checked: n.Warehouse.Checked, Completeness: n.Warehouse.Completeness, OutsideContinuo: outside,
			},
			Prediction:  n.Prediction,
			StopReason:  n.StopReason,
			Unvalidated: n.Unvalidated,
		})
	}
	return out
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
