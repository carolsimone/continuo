package handlers

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/carolsimone/continuo/pkg/num"
	"github.com/carolsimone/continuo/state/domain/projection"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
)

// runSummaryTimeLayout is the RFC3339 UTC, whole-second layout of a run
// summary's timestamps.
const runSummaryTimeLayout = "2006-01-02T15:04:05Z"

// RunReader is the read side of the run-read RPCs; each read reads the rows
// of the tenant it is given.
type RunReader interface {
	GetRunGraph(ctx context.Context, tenantID string, runID uuid.UUID) (projection.RunGraph, error)
	ListRuns(ctx context.Context, tenantID, scheduleName string, pageSize, pageOffset int) ([]projection.RunSummary, int, error)
	ListActiveRunDrifts(ctx context.Context, tenantID string) (int64, []projection.ActiveRunDrift, error)
}

// RunReadHandler serves GetRunGraph, ListRuns and ListActiveRunDrifts, each
// over the rows of the request's tenant.
type RunReadHandler struct {
	reads  RunReader
	logger *slog.Logger
}

// NewRunReadHandler builds a RunReadHandler over reads.
func NewRunReadHandler(reads RunReader, logger *slog.Logger) *RunReadHandler {
	return &RunReadHandler{reads: reads, logger: logger}
}

// GetRunGraph implements the StateService GetRunGraph RPC. Another tenant's
// run reads as a run with no row: a graph with no nodes.
func (h *RunReadHandler) GetRunGraph(ctx context.Context, req *statev1.GetRunGraphRequest) (*statev1.GetRunGraphResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	runID, err := uuid.Parse(req.GetRunId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "run_id must be a UUID: %q", req.GetRunId())
	}
	g, err := h.reads.GetRunGraph(ctx, tenantID, runID)
	if err != nil {
		h.logger.Error("GetRunGraph failed", "run_id", runID, "error", err)
		return nil, status.Errorf(codes.Internal, "GetRunGraph: %v", err)
	}
	resp := &statev1.GetRunGraphResponse{
		Nodes:              make([]*statev1.RunGraphNode, 0, len(g.Nodes)),
		Edges:              make([]*statev1.RunGraphEdge, 0, len(g.Edges)),
		RunPromotionSeq:    g.RunPromotionSeq,
		LatestPromotionSeq: g.LatestPromotionSeq,
	}
	for _, n := range g.Nodes {
		resp.Nodes = append(resp.Nodes, &statev1.RunGraphNode{
			ServiceName: n.ServiceName, SchemaName: n.SchemaName, TableName: n.TableName,
			ScheduleName: n.ScheduleName, NodeType: n.NodeType,
			Status: string(n.Status), TaskId: n.TaskID.String(),
			WaitReason: string(n.WaitReason), WaitingOnRunId: uuidPtrOrEmpty(n.WaitingOnRunID),
			WaitingSince: timePtrToRFC(n.WaitingSince),
		})
	}
	for _, e := range g.Edges {
		resp.Edges = append(resp.Edges, &statev1.RunGraphEdge{FromNodeId: e.FromNodeID, ToNodeId: e.ToNodeID})
	}
	return resp, nil
}

// ListRuns implements the StateService ListRuns RPC.
func (h *RunReadHandler) ListRuns(ctx context.Context, req *statev1.ListRunsRequest) (*statev1.ListRunsResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetScheduleName() == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule_name is required")
	}
	runs, total, err := h.reads.ListRuns(ctx, tenantID, req.GetScheduleName(), int(req.GetPageSize()), int(req.GetPageOffset()))
	if err != nil {
		h.logger.Error("ListRuns failed", "schedule_name", req.GetScheduleName(), "error", err)
		return nil, status.Errorf(codes.Internal, "ListRuns: %v", err)
	}
	resp := &statev1.ListRunsResponse{
		Runs:       make([]*statev1.RunSummary, 0, len(runs)),
		TotalCount: num.ClampInt32(total),
	}
	for _, r := range runs {
		resp.Runs = append(resp.Runs, &statev1.RunSummary{
			RunId:          r.RunID.String(),
			ScheduleName:   r.ScheduleName,
			TerminalStatus: string(r.TerminalStatus),
			CreatedAt:      r.CreatedAt.UTC().Format(runSummaryTimeLayout),
			CompletedAt:    r.CompletedAt.UTC().Format(runSummaryTimeLayout),
			FailureReason:  string(r.FailureReason),
		})
	}
	return resp, nil
}

// ListActiveRunDrifts implements the StateService ListActiveRunDrifts RPC.
func (h *RunReadHandler) ListActiveRunDrifts(ctx context.Context, _ *statev1.ListActiveRunDriftsRequest) (*statev1.ListActiveRunDriftsResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	latest, drifts, err := h.reads.ListActiveRunDrifts(ctx, tenantID)
	if err != nil {
		h.logger.Error("ListActiveRunDrifts failed", "error", err)
		return nil, status.Errorf(codes.Internal, "ListActiveRunDrifts: %v", err)
	}
	resp := &statev1.ListActiveRunDriftsResponse{
		LatestPromotionSeq: latest,
		ActiveRuns:         make([]*statev1.ActiveRunDrift, 0, len(drifts)),
	}
	for _, d := range drifts {
		resp.ActiveRuns = append(resp.ActiveRuns, &statev1.ActiveRunDrift{
			ScheduleName: d.ScheduleName, RunId: d.RunID.String(), RunPromotionSeq: d.RunPromotionSeq,
		})
	}
	return resp, nil
}
