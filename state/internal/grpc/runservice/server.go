// Package runservice is the gRPC edge of run.v1.RunService. Each handler
// parses its request, takes the tenant from the context, calls a read service
// of state/service/runreads and maps the result to the response; it holds no
// query and reaches no adapter.
package runservice

import (
	"context"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/carolsimone/continuo/pkg/identity"
	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
	"github.com/carolsimone/continuo/state/service/runreads"
)

// Reads are the read services the RunService reads are served from.
type Reads struct {
	// Runs serves GetRun and ListTasks.
	Runs *runreads.RunDetails
	// Attempts serves ListRunAttempts.
	Attempts *runreads.Attempts
	// Nodes serves ListNodeTasks, ListNodes, ListNodeNames and
	// ListNodeServices.
	Nodes *runreads.Nodes
	// Schedules serves ListSchedules.
	Schedules *runreads.Schedules
	// Graphs serves GetRunGraph, ListRuns and ListActiveRunDrifts.
	Graphs *runreads.Service
	// ScheduleGraphs serves GetScheduleGraph and ListScheduleTopologies.
	ScheduleGraphs *runreads.ScheduleGraphs
}

// Server serves the read RPCs of RunService. Every other RPC answers
// Unimplemented.
type Server struct {
	runv1.UnimplementedRunServiceServer
	reads  Reads
	logger *slog.Logger
}

var _ runv1.RunServiceServer = (*Server)(nil)

// NewServer returns a Server that serves its reads from reads.
func NewServer(reads Reads, logger *slog.Logger) *Server {
	return &Server{reads: reads, logger: logger}
}

// requestTenant returns the tenant the request acts for, which the identity
// interceptor at the gRPC edge placed on ctx. A context that carries no tenant
// is refused with Internal before anything is read: the server serving it was
// built without the interceptor, and no handler acts for a tenant it guessed.
func requestTenant(ctx context.Context) (string, error) {
	tenantID := identity.TenantFromContext(ctx)
	if tenantID == "" {
		return "", status.Error(codes.Internal, "the request carries no tenant")
	}
	return tenantID, nil
}

// internalError logs a failed read and returns it as an Internal status that
// names the RPC.
func (s *Server) internalError(rpc string, err error, attrs ...any) error {
	s.logger.Error(rpc+" failed", append(attrs, "error", err)...)
	return status.Errorf(codes.Internal, "%s: %v", rpc, err)
}
