package runservice

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/carolsimone/continuo/pkg/num"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/projection"
	"github.com/carolsimone/continuo/state/domain/repository"
	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
)

// runSummaryTimeLayout is the RFC3339 UTC, whole-second layout of a run
// summary's timestamps.
const runSummaryTimeLayout = "2006-01-02T15:04:05Z"

// GetRun returns one run of the request's tenant; another tenant's run is
// NotFound.
func (s *Server) GetRun(ctx context.Context, req *runv1.GetRunRequest) (*runv1.RunResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	runID, err := parseUUID("run_id", req.GetRunId())
	if err != nil {
		return nil, err
	}
	detail, err := s.reads.Runs.GetRun(ctx, tenantID, runID)
	if errors.Is(err, repository.ErrRunNotFound) {
		return nil, status.Error(codes.NotFound, "run not found")
	}
	if err != nil {
		return nil, s.internalError("GetRun", err, "run_id", runID)
	}
	return &runv1.RunResponse{Run: toProtoRun(detail)}, nil
}

func toProtoRun(d projection.RunDetail) *runv1.Run {
	return &runv1.Run{
		RunId:              d.RunID.String(),
		ScheduleName:       d.ScheduleName,
		Kind:               d.Kind,
		Operation:          d.Operation,
		Status:             string(d.Status),
		FailureReason:      d.FailureReason,
		ReleaseId:          d.ReleaseID,
		PromotionSeq:       d.PromotionSeq,
		InitiatedBy:        d.InitiatedBy,
		CreatedAt:          timestamppb.New(d.CreatedAt),
		StartedAt:          timestampOrNil(d.StartedAt),
		CompletedAt:        timestampOrNil(d.CompletedAt),
		LastProgressAt:     timestampOrNil(d.LastProgressAt),
		CancelledAt:        timestampOrNil(d.CancelledAt),
		CancelledBy:        d.CancelledBy,
		CancellationReason: d.CancellationReason,
	}
}

// ListTasks returns one page of a run's tasks. A run that does not exist, or
// that belongs to another tenant, has no tasks.
func (s *Server) ListTasks(ctx context.Context, req *runv1.ListTasksRequest) (*runv1.ListTasksResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	runID, err := parseUUID("run_id", req.GetRunId())
	if err != nil {
		return nil, err
	}
	var statusFilter *run.TaskStatus
	if req.GetStatus() != "" {
		st, perr := run.ParseTaskStatus(req.GetStatus())
		if perr != nil {
			return nil, status.Errorf(codes.InvalidArgument, "status is not a task status: %q", req.GetStatus())
		}
		statusFilter = &st
	}

	tasks, total, err := s.reads.Runs.ListTasks(ctx, tenantID, runID, statusFilter, int(req.GetPageSize()), int(req.GetPageOffset()))
	if err != nil {
		return nil, s.internalError("ListTasks", err, "run_id", runID)
	}
	resp := &runv1.ListTasksResponse{
		Tasks:      make([]*runv1.Task, 0, len(tasks)),
		TotalCount: num.ClampInt32(total),
	}
	for _, t := range tasks {
		resp.Tasks = append(resp.Tasks, toProtoTask(t))
	}
	return resp, nil
}

func toProtoTask(t projection.TaskDetail) *runv1.Task {
	out := &runv1.Task{
		TaskId:         t.TaskID.String(),
		RunId:          t.RunID.String(),
		CreatedAt:      timestamppb.New(t.CreatedAt),
		ServiceName:    t.ServiceName,
		SchemaName:     t.SchemaName,
		TableName:      t.TableName,
		JobName:        t.JobName,
		Status:         string(t.Status),
		Attempt:        num.ClampInt32(t.Attempt),
		MaxRetries:     num.ClampInt32(t.MaxRetries),
		WaitReason:     string(t.WaitReason),
		WaitingOnRunId: uuidOrEmpty(t.WaitingOnRunID),
		WaitingSince:   timestampOrNil(t.WaitingSince),
	}
	// A cancel stamps every task it asks to stop, so a stopping task that then
	// succeeds or fails still holds the stamps; only a cancelled task was
	// cancelled.
	if t.Status == run.TaskStatusCancelled {
		out.CancelledAt = timestampOrNil(t.CancelledAt)
		out.CancelledBy = t.CancelledBy
	}
	return out
}

// ListRunAttempts returns one page of a run's task attempts. A run that does
// not exist, or that belongs to another tenant, has no attempts.
func (s *Server) ListRunAttempts(ctx context.Context, req *runv1.ListRunAttemptsRequest) (*runv1.ListRunAttemptsResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	runID, err := parseUUID("run_id", req.GetRunId())
	if err != nil {
		return nil, err
	}
	var taskID *uuid.UUID
	if req.GetTaskId() != "" {
		id, perr := parseUUID("task_id", req.GetTaskId())
		if perr != nil {
			return nil, perr
		}
		taskID = &id
	}

	rows, total, err := s.reads.Attempts.ListRunAttempts(ctx, tenantID, runID, taskID, int(req.GetPageSize()), int(req.GetPageOffset()))
	if err != nil {
		return nil, s.internalError("ListRunAttempts", err, "run_id", runID, "task_id", req.GetTaskId())
	}
	resp := &runv1.ListRunAttemptsResponse{
		Attempts:   make([]*runv1.TaskAttempt, 0, len(rows)),
		TotalCount: num.ClampInt32(total),
	}
	for _, a := range rows {
		resp.Attempts = append(resp.Attempts, toProtoAttempt(a))
	}
	return resp, nil
}

func toProtoAttempt(a projection.TaskAttempt) *runv1.TaskAttempt {
	out := &runv1.TaskAttempt{
		TaskId:        a.TaskID.String(),
		Attempt:       num.ClampInt32(a.Attempt),
		JobName:       a.JobName,
		Status:        a.Status,
		FailureClass:  stringOrEmpty(a.FailureClass),
		FailureReason: stringOrEmpty(a.FailureReason),
		DispatchedAt:  timestamppb.New(a.DispatchedAt),
		QueuedAt:      timestampOrNil(a.QueuedAt),
		StartedAt:     timestampOrNil(a.StartedAt),
		FinishedAt:    timestampOrNil(a.FinishedAt),
		ErrorMessage:  stringOrEmpty(a.ErrorMessage),
		LogS3Key:      stringOrEmpty(a.LogS3Key),
		RunResultsUri: stringOrEmpty(a.RunResultsURI),
	}
	if a.ExitCode != nil {
		out.ExitCode = num.ClampInt32(*a.ExitCode)
	}
	if a.ExecutionSeconds != nil {
		out.ExecutionSeconds = *a.ExecutionSeconds
	}
	return out
}

// GetRunGraph returns a run's tasks and dependencies in the run's own release.
// A run that does not exist, or that belongs to another tenant, has no nodes.
func (s *Server) GetRunGraph(ctx context.Context, req *runv1.GetRunGraphRequest) (*runv1.GetRunGraphResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	runID, err := parseUUID("run_id", req.GetRunId())
	if err != nil {
		return nil, err
	}
	g, err := s.reads.Graphs.GetRunGraph(ctx, tenantID, runID)
	if err != nil {
		return nil, s.internalError("GetRunGraph", err, "run_id", runID)
	}
	resp := &runv1.GetRunGraphResponse{
		Nodes:              make([]*runv1.RunGraphNode, 0, len(g.Nodes)),
		Edges:              make([]*runv1.RunGraphEdge, 0, len(g.Edges)),
		RunPromotionSeq:    g.RunPromotionSeq,
		LatestPromotionSeq: g.LatestPromotionSeq,
	}
	for _, n := range g.Nodes {
		resp.Nodes = append(resp.Nodes, &runv1.RunGraphNode{
			ServiceName:    n.ServiceName,
			SchemaName:     n.SchemaName,
			TableName:      n.TableName,
			ScheduleName:   n.ScheduleName,
			NodeType:       n.NodeType,
			Status:         string(n.Status),
			TaskId:         n.TaskID.String(),
			WaitReason:     string(n.WaitReason),
			WaitingOnRunId: uuidOrEmpty(n.WaitingOnRunID),
			WaitingSince:   rfc3339OrEmpty(n.WaitingSince),
		})
	}
	for _, e := range g.Edges {
		resp.Edges = append(resp.Edges, &runv1.RunGraphEdge{FromNodeId: e.FromNodeID, ToNodeId: e.ToNodeID})
	}
	return resp, nil
}

// ListRuns returns one page of a schedule's completed runs, newest first.
func (s *Server) ListRuns(ctx context.Context, req *runv1.ListRunsRequest) (*runv1.ListRunsResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetScheduleName() == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule_name is required")
	}
	runs, total, err := s.reads.Graphs.ListRuns(ctx, tenantID, req.GetScheduleName(), int(req.GetPageSize()), int(req.GetPageOffset()))
	if err != nil {
		return nil, s.internalError("ListRuns", err, "schedule_name", req.GetScheduleName())
	}
	resp := &runv1.ListRunsResponse{
		Runs:       make([]*runv1.RunSummary, 0, len(runs)),
		TotalCount: num.ClampInt32(total),
	}
	for _, r := range runs {
		resp.Runs = append(resp.Runs, &runv1.RunSummary{
			RunId:         r.RunID.String(),
			ScheduleName:  r.ScheduleName,
			Status:        string(r.TerminalStatus),
			CreatedAt:     r.CreatedAt.UTC().Format(runSummaryTimeLayout),
			CompletedAt:   r.CompletedAt.UTC().Format(runSummaryTimeLayout),
			FailureReason: string(r.FailureReason),
		})
	}
	return resp, nil
}

// ListActiveRunDrifts returns the live promotion seq and every schedule's
// newest active run with the promotion seq of its release.
func (s *Server) ListActiveRunDrifts(ctx context.Context, _ *runv1.ListActiveRunDriftsRequest) (*runv1.ListActiveRunDriftsResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	latest, drifts, err := s.reads.Graphs.ListActiveRunDrifts(ctx, tenantID)
	if err != nil {
		return nil, s.internalError("ListActiveRunDrifts", err)
	}
	resp := &runv1.ListActiveRunDriftsResponse{
		LatestPromotionSeq: latest,
		ActiveRuns:         make([]*runv1.ActiveRunDrift, 0, len(drifts)),
	}
	for _, d := range drifts {
		resp.ActiveRuns = append(resp.ActiveRuns, &runv1.ActiveRunDrift{
			ScheduleName: d.ScheduleName, RunId: d.RunID.String(), RunPromotionSeq: d.RunPromotionSeq,
		})
	}
	return resp, nil
}
