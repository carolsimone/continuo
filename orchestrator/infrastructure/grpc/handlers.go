package grpc

import (
	"context"
	"log/slog"

	orchestratorv1 "github.com/carolsimone/continuo/orchestrator/api/orchestrator/v1"
	"github.com/carolsimone/continuo/orchestrator/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// QueryReader is the interface the handler depends on for read-side queries.
type QueryReader interface {
	GetScheduleGraph(ctx context.Context, scheduleName string) (*domain.ScheduleGraph, error)
	ListRuns(ctx context.Context, scheduleName string) ([]*domain.RunSummary, error)
	GetRunGraph(ctx context.Context, runID string) ([]*domain.TableNode, []*domain.GraphEdge, error)
}

// QueryHandler implements the OrchestratorQuery gRPC service methods.
type QueryHandler struct {
	reader QueryReader
	logger *slog.Logger
}

// NewQueryHandler creates a new QueryHandler.
func NewQueryHandler(reader QueryReader, logger *slog.Logger) *QueryHandler {
	return &QueryHandler{reader: reader, logger: logger}
}

// GetScheduleGraph returns nodes and edges for a schedule's topology.
func (h *QueryHandler) GetScheduleGraph(ctx context.Context, req *orchestratorv1.GetScheduleGraphRequest) (*orchestratorv1.GetScheduleGraphResponse, error) {
	if req.ScheduleName == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule_name is required")
	}

	graph, err := h.reader.GetScheduleGraph(ctx, req.ScheduleName)
	if err != nil {
		h.logger.Error("GetScheduleGraph failed", "schedule", req.ScheduleName, "error", err)
		return nil, status.Errorf(codes.Internal, "GetScheduleGraph: %v", err)
	}

	resp := &orchestratorv1.GetScheduleGraphResponse{
		Nodes: make([]*orchestratorv1.TableNode, 0, len(graph.Nodes)),
		Edges: make([]*orchestratorv1.GraphEdge, 0, len(graph.Edges)),
	}
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

// ListRuns returns completed runs for a schedule, ordered by creation date descending.
func (h *QueryHandler) ListRuns(ctx context.Context, req *orchestratorv1.ListRunsRequest) (*orchestratorv1.ListRunsResponse, error) {
	if req.ScheduleName == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule_name is required")
	}

	runs, err := h.reader.ListRuns(ctx, req.ScheduleName)
	if err != nil {
		h.logger.Error("ListRuns failed", "schedule", req.ScheduleName, "error", err)
		return nil, status.Errorf(codes.Internal, "ListRuns: %v", err)
	}

	resp := &orchestratorv1.ListRunsResponse{
		Runs: make([]*orchestratorv1.RunSummary, 0, len(runs)),
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

// GetRunGraph returns nodes with execution status and edges for a specific run.
func (h *QueryHandler) GetRunGraph(ctx context.Context, req *orchestratorv1.GetRunGraphRequest) (*orchestratorv1.GetRunGraphResponse, error) {
	if req.RunId == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}

	nodes, edges, err := h.reader.GetRunGraph(ctx, req.RunId)
	if err != nil {
		h.logger.Error("GetRunGraph failed", "run_id", req.RunId, "error", err)
		return nil, status.Errorf(codes.Internal, "GetRunGraph: %v", err)
	}

	resp := &orchestratorv1.GetRunGraphResponse{
		Nodes: make([]*orchestratorv1.TableNode, 0, len(nodes)),
		Edges: make([]*orchestratorv1.GraphEdge, 0, len(edges)),
	}
	for _, n := range nodes {
		resp.Nodes = append(resp.Nodes, domainToProtoNode(n))
	}
	for _, e := range edges {
		resp.Edges = append(resp.Edges, &orchestratorv1.GraphEdge{
			FromNodeId: e.FromNodeID,
			ToNodeId:   e.ToNodeID,
		})
	}
	return resp, nil
}

// domainToProtoNode converts a domain TableNode to its proto representation.
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

// domainToProtoCriticality converts a domain Criticality to its proto enum value.
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
