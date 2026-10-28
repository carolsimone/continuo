// Package runservice is the gRPC edge of run.v1.RunService. Each handler
// parses its request, takes the tenant and identity from the context, calls a
// read service of state/service/runreads, a command of state/service/handlers
// or the schedules service of state/service/schedules, and maps the result to
// the response; it holds no query and reaches no adapter.
package runservice

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/domain/schedule"
	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/runreads"
	"github.com/carolsimone/continuo/state/service/schedules"
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
	// Graphs serves GetRunGraph, ListRuns and ListActiveRunDrifts.
	Graphs *runreads.Service
	// ScheduleGraphs serves GetScheduleGraph and ListScheduleTopologies.
	ScheduleGraphs *runreads.ScheduleGraphs
	// Rebases serves PreviewRebase.
	Rebases RebasePreviewer
}

// RebasePreviewer previews a rebase of a tenant's run: what TriggerRebase
// would run, without creating a run.
type RebasePreviewer interface {
	Preview(ctx context.Context, tenantID string, sourceID uuid.UUID, includeChangedUpstream bool) (runreads.RebasePreview, error)
}

// Commands are the application commands behind the RunService commands:
// each that takes a request id acts at most once per request id of its
// tenant.
type Commands interface {
	StartRun(ctx context.Context, cmd svchandlers.StartRunCommand) (svchandlers.CommandResult, error)
	TriggerRerun(ctx context.Context, cmd svchandlers.TriggerRerunCommand) (svchandlers.CommandResult, error)
	TriggerRebase(ctx context.Context, cmd svchandlers.TriggerRebaseCommand) (svchandlers.CommandResult, error)
	TriggerSingleNodeRun(ctx context.Context, cmd svchandlers.TriggerSingleNodeRunCommand) (svchandlers.CommandResult, error)
	CancelActiveRun(ctx context.Context, cmd svchandlers.CancelActiveRunCommand) (svchandlers.CommandResult, error)
	CancelRun(ctx context.Context, cmd svchandlers.CancelRunCommand) (uuid.UUID, error)
}

var _ Commands = (*svchandlers.RunCommands)(nil)

// Schedules edits, previews and lists the crons of a tenant's schedules. It
// serves UpsertSchedule, DeleteSchedule, PreviewSchedule and ListSchedules.
type Schedules interface {
	Upsert(ctx context.Context, tenantID string, in schedules.UpsertInput, by string) (*schedule.Schedule, []time.Time, error)
	Delete(ctx context.Context, tenantID, name string) error
	Preview(ctx context.Context, cron, timezone string) (words string, next []time.Time, err error)
	List(ctx context.Context, tenantID string) ([]schedules.Summary, error)
}

var _ Schedules = (*schedules.Service)(nil)

// Server serves RunService: its reads from Reads, its run commands from
// Commands and its schedule calls from Schedules.
type Server struct {
	runv1.UnimplementedRunServiceServer
	reads     Reads
	commands  Commands
	schedules Schedules
	logger    *slog.Logger
}

var _ runv1.RunServiceServer = (*Server)(nil)

// NewServer returns a Server that serves its reads from reads, its run
// commands from commands and its schedule calls from schedules.
func NewServer(reads Reads, commands Commands, schedules Schedules, logger *slog.Logger) *Server {
	return &Server{reads: reads, commands: commands, schedules: schedules, logger: logger}
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

// internalError logs a failed call and returns it as an Internal status that
// names the RPC.
func (s *Server) internalError(rpc string, err error, attrs ...any) error {
	s.logger.Error(rpc+" failed", append(attrs, "error", err)...)
	return status.Errorf(codes.Internal, "%s: %v", rpc, err)
}
