package grpc

import (
	"context"
	"errors"
	"log/slog"

	orchestratorv1 "github.com/carolsimone/continuo/orchestrator/api/orchestrator/v1"
	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/service/queries"
	"github.com/carolsimone/continuo/pkg/num"
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
	ListRuns(ctx context.Context, scheduleName string, limit, offset int) ([]*domain.RunSummary, int, error)
	ListScheduleTopologies(ctx context.Context) ([]*domain.ScheduleTopologySummary, error)
	GetNodeAncestry(ctx context.Context, nodeUniqueID string, maxDepth int) ([]*domain.NodeAncestor, error)
	GetNode(ctx context.Context, service, schema, table string) (*domain.NodeMeta, error)
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

// ListRuns pagination bounds. defaultListRunsPageSize applies when the request
// leaves page_size unset (0); maxListRunsPageSize caps oversized requests so a
// single call can never scan an unbounded run history. Mirrors the
// StateService.ListNodes 50/200 contract.
const (
	defaultListRunsPageSize = 50
	maxListRunsPageSize     = 200
)

// maxAncestryDepth caps GetNodeAncestry's hop count so a single call cannot walk
// an unbounded path; mirrors the page-size clamp philosophy.
const maxAncestryDepth = 100

func clampPageSize(requested int32) int {
	size := int(requested)
	if size <= 0 {
		return defaultListRunsPageSize
	}
	if size > maxListRunsPageSize {
		return maxListRunsPageSize
	}
	return size
}

func clampOffset(requested int32) int {
	if requested < 0 {
		return 0
	}
	return int(requested)
}

func (h *QueryHandler) ListRuns(ctx context.Context, req *orchestratorv1.ListRunsRequest) (*orchestratorv1.ListRunsResponse, error) {
	if req.ScheduleName == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule_name is required")
	}
	limit := clampPageSize(req.PageSize)
	offset := clampOffset(req.PageOffset)
	runs, total, err := h.scheduleAndRunLists.ListRuns(ctx, req.ScheduleName, limit, offset)
	if err != nil {
		h.logger.Error("ListRuns failed", "schedule", req.ScheduleName, "error", err)
		return nil, status.Errorf(codes.Internal, "ListRuns: %v", err)
	}
	resp := &orchestratorv1.ListRunsResponse{
		Runs:       make([]*orchestratorv1.RunSummary, 0, len(runs)),
		TotalCount: num.ClampInt32(total),
	}
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
			NodeCount:    num.ClampInt32(s.NodeCount),
		}
		if !s.LastUpdatedAt.IsZero() {
			item.LastUpdatedAt = timestamppb.New(s.LastUpdatedAt)
		}
		resp.Schedules = append(resp.Schedules, item)
	}
	return resp, nil
}

func (h *QueryHandler) GetNodeAncestry(ctx context.Context, req *orchestratorv1.GetNodeAncestryRequest) (*orchestratorv1.GetNodeAncestryResponse, error) {
	if req.NodeUniqueId == "" {
		return nil, status.Error(codes.InvalidArgument, "node_unique_id is required")
	}
	if req.MaxDepth < 0 {
		return nil, status.Error(codes.InvalidArgument, "max_depth must be >= 0")
	}
	depth := int(req.MaxDepth)
	if depth > maxAncestryDepth {
		depth = maxAncestryDepth
	}
	ancestors, err := h.scheduleAndRunLists.GetNodeAncestry(ctx, req.NodeUniqueId, depth)
	if err != nil {
		if errors.Is(err, domain.ErrNodeNotFound) {
			return nil, status.Errorf(codes.NotFound, "node %q not found", req.NodeUniqueId)
		}
		h.logger.Error("GetNodeAncestry failed", "node", req.NodeUniqueId, "error", err)
		return nil, status.Errorf(codes.Internal, "GetNodeAncestry: %v", err)
	}
	resp := &orchestratorv1.GetNodeAncestryResponse{
		Ancestors: make([]*orchestratorv1.AncestorNode, 0, len(ancestors)),
	}
	for _, a := range ancestors {
		resp.Ancestors = append(resp.Ancestors, domainToProtoAncestor(a))
	}
	return resp, nil
}

func (h *QueryHandler) GetNode(ctx context.Context, req *orchestratorv1.GetNodeRequest) (*orchestratorv1.GetNodeResponse, error) {
	if req.ServiceName == "" || req.SchemaName == "" || req.TableName == "" {
		return nil, status.Error(codes.InvalidArgument, "service_name, schema_name and table_name are required")
	}
	meta, err := h.scheduleAndRunLists.GetNode(ctx, req.ServiceName, req.SchemaName, req.TableName)
	if err != nil {
		if errors.Is(err, domain.ErrNodeNotFound) {
			return nil, status.Errorf(codes.NotFound, "node %s.%s.%s not found", req.ServiceName, req.SchemaName, req.TableName)
		}
		h.logger.Error("GetNode failed", "service", req.ServiceName, "schema", req.SchemaName, "table", req.TableName, "error", err)
		return nil, status.Errorf(codes.Internal, "GetNode: %v", err)
	}
	return &orchestratorv1.GetNodeResponse{
		NodeType:       meta.NodeType,
		TestCount:      num.ClampInt32(meta.TestCount),
		TestCountKnown: meta.TestCountKnown,
	}, nil
}

func domainToProtoAncestor(a *domain.NodeAncestor) *orchestratorv1.AncestorNode {
	node := &orchestratorv1.AncestorNode{
		UniqueId:      a.UniqueID,
		SchemaName:    a.SchemaName,
		TableName:     a.TableName,
		ServiceName:   a.ServiceName,
		NodeType:      a.NodeType,
		Depth:         num.ClampInt32(a.Depth),
		LastCommitSha: a.LastCommitSHA,
		LastRepo:      a.LastRepo,
		LastReleaseId: a.LastReleaseID,
		FilePath:      a.FilePath,
	}
	if a.LastChangedAt != nil {
		node.LastChangedAt = timestamppb.New(*a.LastChangedAt)
	}
	return node
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
