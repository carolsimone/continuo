package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/carolsimone/continuo/state/internal/grpc/handlers"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Server wraps gRPC server with graceful shutdown
type Server struct {
	statev1.UnimplementedStateServiceServer
	grpcServer           *grpc.Server
	listener             net.Listener
	logger               *slog.Logger
	schedulerHandler     *handlers.SchedulerHandler
	taskHandler          *handlers.TaskHandler
	taskExecutionHandler *handlers.TaskExecutionHandler
	rerunHandler         *handlers.RerunHandler
	singleNodeRunHandler *handlers.SingleNodeRunHandler
	rebaseHandler        *handlers.RebaseHandler
	nodeRunHandler       *handlers.NodeRunHandler
}

// NewServer creates a new gRPC server
func NewServer(
	port int,
	schedulerHandler *handlers.SchedulerHandler,
	taskHandler *handlers.TaskHandler,
	taskExecutionHandler *handlers.TaskExecutionHandler,
	rerunHandler *handlers.RerunHandler,
	singleNodeRunHandler *handlers.SingleNodeRunHandler,
	rebaseHandler *handlers.RebaseHandler,
	nodeRunHandler *handlers.NodeRunHandler,
	logger *slog.Logger,
) (*Server, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(loggingInterceptor(logger)),
	)

	server := &Server{
		grpcServer:           grpcServer,
		listener:             listener,
		logger:               logger,
		schedulerHandler:     schedulerHandler,
		taskHandler:          taskHandler,
		taskExecutionHandler: taskExecutionHandler,
		rerunHandler:         rerunHandler,
		singleNodeRunHandler: singleNodeRunHandler,
		rebaseHandler:        rebaseHandler,
		nodeRunHandler:       nodeRunHandler,
	}

	statev1.RegisterStateServiceServer(grpcServer, server)

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
// IMPLEMENT STATE SERVICE SERVER INTERFACE
// ============================================================================

// GetScheduler delegates to scheduler handler
func (s *Server) GetScheduler(ctx context.Context, req *statev1.GetSchedulerRequest) (*statev1.SchedulerResponse, error) {
	return s.schedulerHandler.GetScheduler(ctx, req)
}

// CancelScheduler delegates to scheduler handler
func (s *Server) CancelScheduler(ctx context.Context, req *statev1.CancelSchedulerRequest) (*statev1.SchedulerResponse, error) {
	return s.schedulerHandler.CancelScheduler(ctx, req)
}

// ActivateSchedule delegates to scheduler handler
func (s *Server) ActivateSchedule(ctx context.Context, req *statev1.ActivateScheduleRequest) (*statev1.ActivateScheduleResponse, error) {
	return s.schedulerHandler.ActivateSchedule(ctx, req)
}

// ListAllSchedules delegates to scheduler handler
func (s *Server) ListAllSchedules(ctx context.Context, req *statev1.ListAllSchedulesRequest) (*statev1.ListAllSchedulesResponse, error) {
	return s.schedulerHandler.ListAllSchedules(ctx, req)
}

// TriggerSchedule delegates to scheduler handler
func (s *Server) TriggerSchedule(ctx context.Context, req *statev1.TriggerScheduleRequest) (*statev1.TriggerScheduleResponse, error) {
	return s.schedulerHandler.TriggerSchedule(ctx, req)
}

// CancelSchedule delegates to scheduler handler
func (s *Server) CancelSchedule(ctx context.Context, req *statev1.CancelScheduleRequest) (*statev1.CancelScheduleResponse, error) {
	return s.schedulerHandler.CancelSchedule(ctx, req)
}

// ListStuckCandidates delegates to scheduler handler
func (s *Server) ListStuckCandidates(ctx context.Context, req *statev1.ListStuckCandidatesRequest) (*statev1.ListStuckCandidatesResponse, error) {
	return s.schedulerHandler.ListStuckCandidates(ctx, req)
}

// GetTask delegates to task handler
func (s *Server) GetTask(ctx context.Context, req *statev1.GetTaskRequest) (*statev1.TaskResponse, error) {
	return s.taskHandler.GetTask(ctx, req)
}

// GetTaskByScheduleAndNode delegates to task handler
func (s *Server) GetTaskByScheduleAndNode(ctx context.Context, req *statev1.GetTaskByScheduleAndNodeRequest) (*statev1.TaskResponse, error) {
	return s.taskHandler.GetTaskByScheduleAndNode(ctx, req)
}

// ListTasks delegates to task handler
func (s *Server) ListTasks(ctx context.Context, req *statev1.ListTasksRequest) (*statev1.ListTasksResponse, error) {
	return s.taskHandler.ListTasks(ctx, req)
}

// GetSchedulerInitStatus delegates to scheduler handler
func (s *Server) GetSchedulerInitStatus(ctx context.Context, req *statev1.GetSchedulerInitStatusRequest) (*statev1.GetSchedulerInitStatusResponse, error) {
	return s.schedulerHandler.GetSchedulerInitStatus(ctx, req)
}

// GetTaskExecution delegates to task execution handler
func (s *Server) GetTaskExecution(ctx context.Context, req *statev1.GetTaskExecutionRequest) (*statev1.TaskExecutionResponse, error) {
	return s.taskExecutionHandler.GetTaskExecution(ctx, req)
}

// ListTaskExecutions delegates to task execution handler
func (s *Server) ListTaskExecutions(ctx context.Context, req *statev1.ListTaskExecutionsRequest) (*statev1.ListTaskExecutionsResponse, error) {
	return s.taskExecutionHandler.ListTaskExecutions(ctx, req)
}

// TriggerRerun delegates to rerun handler
func (s *Server) TriggerRerun(ctx context.Context, req *statev1.TriggerRerunRequest) (*statev1.TriggerRerunResponse, error) {
	return s.rerunHandler.TriggerRerun(ctx, req)
}

// TriggerSingleNodeRun delegates to single node run handler
func (s *Server) TriggerSingleNodeRun(ctx context.Context, req *statev1.TriggerSingleNodeRunRequest) (*statev1.TriggerSingleNodeRunResponse, error) {
	return s.singleNodeRunHandler.TriggerSingleNodeRun(ctx, req)
}

// TriggerRebase delegates to rebase handler
func (s *Server) TriggerRebase(ctx context.Context, req *statev1.TriggerRebaseRequest) (*statev1.TriggerRebaseResponse, error) {
	return s.rebaseHandler.TriggerRebase(ctx, req)
}

// ListNodeRuns delegates to node run handler
func (s *Server) ListNodeRuns(ctx context.Context, req *statev1.ListNodeRunsRequest) (*statev1.ListNodeRunsResponse, error) {
	return s.nodeRunHandler.ListNodeRuns(ctx, req)
}

// ListNodes delegates to node run handler
func (s *Server) ListNodes(ctx context.Context, req *statev1.ListNodesRequest) (*statev1.ListNodesResponse, error) {
	return s.nodeRunHandler.ListNodes(ctx, req)
}

// ListNodeNames delegates to node run handler
func (s *Server) ListNodeNames(ctx context.Context, req *statev1.ListNodeNamesRequest) (*statev1.ListNodeNamesResponse, error) {
	return s.nodeRunHandler.ListNodeNames(ctx, req)
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
