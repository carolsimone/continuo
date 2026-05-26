package grpc

import (
	"context"
	"log/slog"

	orchestratorv1 "github.com/carolsimone/continuo/orchestrator/api/orchestrator/v1"
	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/service/queries"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ScheduleAndRunListReader returns the schedule's full topology graph and the
// per-schedule run history. Both reads go straight to Neo4j and return raw
// domain types — no drift information is composed in. Satisfied by
// adapters/neo4j.OrchestratorQueryRepository.
type ScheduleAndRunListReader interface {
	GetScheduleGraph(ctx context.Context, scheduleName string) (*domain.ScheduleGraph, error)
	ListRuns(ctx context.Context, scheduleName string) ([]*domain.RunSummary, error)
	ListScheduleTopologies(ctx context.Context) ([]*domain.ScheduleTopologySummary, error)
}

// DriftAwareRunReader returns view-shaped run data composed from Neo4j
// (the :Run node and its pinned topology_generation) AND Postgres (the
// current topology_state.topology_generation). Used by the RPCs that
// surface drift to the dashboard and rerun modal. Satisfied by
// service/queries.RunQueryService.
type DriftAwareRunReader interface {
	GetRunGraph(ctx context.Context, runID string) (*queries.RunGraphView, error)
	ListActiveRunDrifts(ctx context.Context) (*queries.ActiveRunDriftView, error)
}

// QueryHandler implements the OrchestratorQuery gRPC service.
type QueryHandler struct {
	orchestratorv1.UnimplementedOrchestratorQueryServer
	scheduleAndRunLists ScheduleAndRunListReader
	driftAwareRuns      DriftAwareRunReader
	logger              *slog.Logger
}

func NewQueryHandler(scheduleAndRunLists ScheduleAndRunListReader, driftAwareRuns DriftAwareRunReader, logger *slog.Logger) *QueryHandler {
	return &QueryHandler{scheduleAndRunLists: scheduleAndRunLists, driftAwareRuns: driftAwareRuns, logger: logger}
}

func (h *QueryHandler) GetScheduleGraph(ctx context.Context, req *orchestratorv1.GetScheduleGraphRequest) (*orchestratorv1.GetScheduleGraphResponse, error) {
	if req.ScheduleName == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule_name is required")
	}
	graph, err := h.scheduleAndRunLists.GetScheduleGraph(ctx, req.ScheduleName)
	if err != nil {
		h.logger.Error("GetScheduleGraph failed", "schedule", req.ScheduleName, "error", err)
		return nil, status.Errorf(codes.Internal, "GetScheduleGraph: %v", err)
	}
	resp := &orchestratorv1.GetScheduleGraphResponse{
		Nodes: make([]*orchestratorv1.TableNode, 0, len(graph.Nodes)),
		Edges: make([]*orchestratorv1.GraphEdge, 0, len(graph.Edges)),
	}
	resp.TopologyGeneration = graph.TopologyGeneration
	for _, n := range graph.Nodes {
		resp.Nodes = append(resp.Nodes, domainToProtoNode(n))
	}
	for _, e := range graph.Edges {
		resp.Edges = append(resp.Edges, &orchestratorv1.GraphEdge{
			FromNodeId: e.FromNodeID,
			ToNodeId:   e.ToNodeID,
		})
	}
	return resp, nil
}

func (h *QueryHandler) ListRuns(ctx context.Context, req *orchestratorv1.ListRunsRequest) (*orchestratorv1.ListRunsResponse, error) {
	if req.ScheduleName == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule_name is required")
	}
	runs, err := h.scheduleAndRunLists.ListRuns(ctx, req.ScheduleName)
	if err != nil {
		h.logger.Error("ListRuns failed", "schedule", req.ScheduleName, "error", err)
		return nil, status.Errorf(codes.Internal, "ListRuns: %v", err)
	}
	resp := &orchestratorv1.ListRunsResponse{Runs: make([]*orchestratorv1.RunSummary, 0, len(runs))}
	for _, r := range runs {
		resp.Runs = append(resp.Runs, &orchestratorv1.RunSummary{
			RunId:          r.RunID,
			ScheduleName:   r.ScheduleName,
			TerminalStatus: r.TerminalStatus,
			CreatedAt:      r.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
			CompletedAt:    r.CompletedAt.UTC().Format("2006-01-02T15:04:05Z"),
		})
	}
	return resp, nil
}

func (h *QueryHandler) GetRunGraph(ctx context.Context, req *orchestratorv1.GetRunGraphRequest) (*orchestratorv1.GetRunGraphResponse, error) {
	if req.RunId == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	view, err := h.driftAwareRuns.GetRunGraph(ctx, req.RunId)
	if err != nil {
		h.logger.Error("GetRunGraph failed", "run_id", req.RunId, "error", err)
		return nil, status.Errorf(codes.Internal, "GetRunGraph: %v", err)
	}
	resp := &orchestratorv1.GetRunGraphResponse{
		Nodes:                    make([]*orchestratorv1.TableNode, 0, len(view.Nodes)),
		Edges:                    make([]*orchestratorv1.GraphEdge, 0, len(view.Edges)),
		RunTopologyGeneration:    view.RunTopologyGeneration,
		LatestTopologyGeneration: view.LatestTopologyGeneration,
	}
	for _, n := range view.Nodes {
		resp.Nodes = append(resp.Nodes, domainToProtoNode(n))
	}
	for _, e := range view.Edges {
		resp.Edges = append(resp.Edges, &orchestratorv1.GraphEdge{
			FromNodeId: e.FromNodeID,
			ToNodeId:   e.ToNodeID,
		})
	}
	return resp, nil
}

func (h *QueryHandler) ListActiveRunDrifts(ctx context.Context, req *orchestratorv1.ListActiveRunDriftsRequest) (*orchestratorv1.ListActiveRunDriftsResponse, error) {
	view, err := h.driftAwareRuns.ListActiveRunDrifts(ctx)
	if err != nil {
		h.logger.Error("ListActiveRunDrifts failed", "error", err)
		return nil, status.Errorf(codes.Internal, "ListActiveRunDrifts: %v", err)
	}
	resp := &orchestratorv1.ListActiveRunDriftsResponse{
		LatestTopologyGeneration: view.LatestTopologyGeneration,
		ActiveRuns:               make([]*orchestratorv1.ActiveRunDrift, 0, len(view.ActiveRuns)),
	}
	for _, r := range view.ActiveRuns {
		resp.ActiveRuns = append(resp.ActiveRuns, &orchestratorv1.ActiveRunDrift{
			ScheduleName:          r.ScheduleName,
			RunId:                 r.RunID,
			RunTopologyGeneration: r.TopologyGeneration,
		})
	}
	return resp, nil
}

func (h *QueryHandler) ListScheduleTopologies(ctx context.Context, _ *orchestratorv1.ListScheduleTopologiesRequest) (*orchestratorv1.ListScheduleTopologiesResponse, error) {
	summaries, err := h.scheduleAndRunLists.ListScheduleTopologies(ctx)
	if err != nil {
		h.logger.Error("ListScheduleTopologies failed", "error", err)
		return nil, status.Errorf(codes.Internal, "ListScheduleTopologies: %v", err)
	}
	resp := &orchestratorv1.ListScheduleTopologiesResponse{
		Schedules: make([]*orchestratorv1.ScheduleTopologySummary, 0, len(summaries)),
	}
	for _, s := range summaries {
		item := &orchestratorv1.ScheduleTopologySummary{
			ScheduleName: s.ScheduleName,
			NodeCount:    int32(s.NodeCount),
		}
		if !s.LastUpdatedAt.IsZero() {
			item.LastUpdatedAt = timestamppb.New(s.LastUpdatedAt)
		}
		resp.Schedules = append(resp.Schedules, item)
	}
	return resp, nil
}

func domainToProtoNode(n *domain.TableNode) *orchestratorv1.TableNode {
	return &orchestratorv1.TableNode{
		TableName:     n.TableName,
		SchemaName:    n.SchemaName,
		ServiceName:   n.ServiceName,
		Owner:         n.Owner,
		ScheduleName:  n.ScheduleName,
		Criticality:   domainToProtoCriticality(n.Criticality),
		LastUpdatedAt: timestamppb.New(n.LastUpdatedAt),
		CreatedAt:     timestamppb.New(n.CreatedAt),
		NodeType:      n.NodeType,
		Status:        n.Status,
	}
}

func domainToProtoCriticality(c domain.Criticality) orchestratorv1.Criticality {
	switch c {
	case domain.CriticalityRegulatory:
		return orchestratorv1.Criticality_CRITICALITY_REGULATORY
	case domain.CriticalityCore:
		return orchestratorv1.Criticality_CRITICALITY_CORE
	case domain.CriticalitySecondary:
		return orchestratorv1.Criticality_CRITICALITY_SECONDARY
	default:
		return orchestratorv1.Criticality_CRITICALITY_UNSPECIFIED
	}
}
