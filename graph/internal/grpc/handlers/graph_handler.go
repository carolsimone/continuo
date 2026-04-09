package handlers

import (
	"context"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/graph/adapters/neo4j"
	graphv1 "github.com/carolsimone/continuo/graph/api/graph/v1"
	"github.com/carolsimone/continuo/graph/domain/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// GraphHandler handles all graph-related gRPC requests
type GraphHandler struct {
	repo   neo4j.GraphRepository
	logger *slog.Logger
}

// NewGraphHandler creates a new GraphHandler
func NewGraphHandler(repo neo4j.GraphRepository, logger *slog.Logger) *GraphHandler {
	return &GraphHandler{
		repo:   repo,
		logger: logger,
	}
}

// CreateNode creates or updates a table node with dependencies
func (h *GraphHandler) CreateNode(ctx context.Context, req *graphv1.CreateNodeRequest) (*graphv1.NodeResponse, error) {
	h.logger.Info("Creating node",
		"schema", req.SchemaName,
		"table", req.TableName,
		"service", req.ServiceName,
	)

	// Validation
	if req.TableName == "" || req.SchemaName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "table_name and schema_name are required")
	}
	if req.ServiceName == "" || req.Owner == "" || req.ScheduleName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "service_name, owner, and schedule_name are required")
	}
	if req.Criticality == graphv1.Criticality_CRITICALITY_UNSPECIFIED {
		return nil, status.Errorf(codes.InvalidArgument, "criticality is required")
	}

	// Convert proto request to repository request
	upstreamDeps := make([]*model.UpstreamDependency, len(req.UpstreamDependencies))
	for i, dep := range req.UpstreamDependencies {
		upstreamDeps[i] = &model.UpstreamDependency{
			TableName:   dep.TableName,
			SchemaName:  dep.SchemaName,
			ServiceName: dep.ServiceName,
		}
	}

	repoReq := &neo4j.CreateNodeRequest{
		TableName:            req.TableName,
		SchemaName:           req.SchemaName,
		ServiceName:          req.ServiceName,
		Owner:                req.Owner,
		ScheduleName:         req.ScheduleName,
		Criticality:          protoToDomainCriticality(req.Criticality),
		UpstreamDependencies: upstreamDeps,
		NodeType:             req.NodeType,
		ManifestVersion:      req.ManifestVersion,
	}

	// Call repository
	node, err := h.repo.CreateNode(ctx, repoReq)
	if err != nil {
		h.logger.Error("Failed to create node", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to create node")
	}

	return &graphv1.NodeResponse{
		Node: domainToProtoNode(node),
	}, nil
}

// UpdateNodeTimestamp updates a node's last_updated_at timestamp
func (h *GraphHandler) UpdateNodeTimestamp(ctx context.Context, req *graphv1.UpdateNodeTimestampRequest) (*graphv1.NodeResponse, error) {
	h.logger.Info("Updating node timestamp",
		"schema", req.SchemaName,
		"table", req.TableName,
	)

	if req.TableName == "" || req.SchemaName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "table_name and schema_name are required")
	}

	node, err := h.repo.UpdateNodeTimestamp(ctx, req.SchemaName, req.TableName)
	if err != nil {
		if err == neo4j.ErrNotFound {
			return nil, status.Errorf(codes.NotFound, "node not found")
		}
		h.logger.Error("Failed to update node timestamp", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to update node timestamp")
	}

	return &graphv1.NodeResponse{
		Node: domainToProtoNode(node),
	}, nil
}

// GetStaleRootNodes gets root nodes not updated in last N hours
func (h *GraphHandler) GetStaleRootNodes(ctx context.Context, req *graphv1.GetStaleRootNodesRequest) (*graphv1.GetStaleRootNodesResponse, error) {
	h.logger.Debug("Getting stale root nodes", "hours_threshold", req.HoursThreshold)

	if req.HoursThreshold <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "hours_threshold must be positive")
	}

	nodes, err := h.repo.GetStaleRootNodes(ctx, int(req.HoursThreshold))
	if err != nil {
		h.logger.Error("Failed to get stale root nodes", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to get stale root nodes")
	}

	protoNodes := make([]*graphv1.TableNode, len(nodes))
	for i, n := range nodes {
		protoNodes[i] = domainToProtoNode(n)
	}

	return &graphv1.GetStaleRootNodesResponse{
		Nodes: protoNodes,
	}, nil
}

// GetDownstreamDependencies gets all downstream dependencies
func (h *GraphHandler) GetDownstreamDependencies(ctx context.Context, req *graphv1.GetDownstreamDependenciesRequest) (*graphv1.GetDownstreamDependenciesResponse, error) {
	h.logger.Debug("Getting downstream dependencies",
		"schedule", req.ScheduleName,
		"schema", req.SchemaName,
		"table", req.TableName,
	)

	if req.ScheduleName == "" || req.SchemaName == "" || req.TableName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_name, schema_name, and table_name are required")
	}

	maxDepth := int(req.MaxDepth)
	dependencies, err := h.repo.GetDownstreamDependencies(
		ctx,
		req.ScheduleName,
		req.SchemaName,
		req.TableName,
		maxDepth,
	)
	if err != nil {
		h.logger.Error("Failed to get downstream dependencies", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to get downstream dependencies")
	}

	protoDeps := make([]*graphv1.Dependency, len(dependencies))
	for i, d := range dependencies {
		protoDeps[i] = &graphv1.Dependency{
			Node:  domainToProtoNode(d.Node),
			Depth: int32(d.Depth),
		}
	}

	return &graphv1.GetDownstreamDependenciesResponse{
		Dependencies: protoDeps,
		TotalCount:   int32(len(dependencies)),
	}, nil
}

// CheckUpstreamFreshness checks if upstream dependencies are fresh
func (h *GraphHandler) CheckUpstreamFreshness(ctx context.Context, req *graphv1.CheckUpstreamFreshnessRequest) (*graphv1.CheckUpstreamFreshnessResponse, error) {
	h.logger.Debug("Checking upstream freshness",
		"schema", req.SchemaName,
		"table", req.TableName,
	)

	if req.SchemaName == "" || req.TableName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schema_name and table_name are required")
	}

	freshnessMinutes := 30 // Default
	if req.FreshnessMinutes > 0 {
		freshnessMinutes = int(req.FreshnessMinutes)
	}

	allFresh, staleUpstreams, err := h.repo.CheckUpstreamFreshness(
		ctx,
		req.SchemaName,
		req.TableName,
		freshnessMinutes,
	)
	if err != nil {
		h.logger.Error("Failed to check upstream freshness", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to check upstream freshness")
	}

	protoStale := make([]*graphv1.StaleUpstream, len(staleUpstreams))
	for i, s := range staleUpstreams {
		protoStale[i] = &graphv1.StaleUpstream{
			Node:               domainToProtoNode(s.Node),
			MinutesSinceUpdate: int32(s.MinutesSinceUpdate),
		}
	}

	// Count fresh upstreams (we need to query total count separately or calculate it)
	// For now, we'll set fresh_count and total_count based on stale count
	// This is a simplification - ideally we'd get total count from repository
	freshCount := 0
	totalCount := len(staleUpstreams)
	if allFresh {
		freshCount = totalCount
	}

	return &graphv1.CheckUpstreamFreshnessResponse{
		AllFresh:       allFresh,
		FreshCount:     int32(freshCount),
		TotalCount:     int32(totalCount),
		StaleUpstreams: protoStale,
	}, nil
}

// GetScheduleGraph returns all nodes and edges for a schedule,
// including cross-boundary upstream dependencies.
func (h *GraphHandler) GetScheduleGraph(ctx context.Context, req *graphv1.GetScheduleGraphRequest) (*graphv1.GetScheduleGraphResponse, error) {
	h.logger.Debug("Getting schedule graph", "schedule", req.ScheduleName)

	if req.ScheduleName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_name is required")
	}

	graph, err := h.repo.GetScheduleGraph(ctx, req.ScheduleName)
	if err != nil {
		h.logger.Error("Failed to get schedule graph", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to get schedule graph")
	}

	protoNodes := make([]*graphv1.TableNode, len(graph.Nodes))
	for i, n := range graph.Nodes {
		protoNodes[i] = domainToProtoNode(n)
	}

	protoEdges := make([]*graphv1.GraphEdge, len(graph.Edges))
	for i, e := range graph.Edges {
		protoEdges[i] = &graphv1.GraphEdge{
			FromNodeId: e.FromNodeID,
			ToNodeId:   e.ToNodeID,
		}
	}

	return &graphv1.GetScheduleGraphResponse{
		Nodes: protoNodes,
		Edges: protoEdges,
	}, nil
}

// GetTransitiveDownstream returns all non-SUCCEEDED nodes downstream of the target.
func (h *GraphHandler) GetTransitiveDownstream(ctx context.Context, req *graphv1.GetTransitiveDownstreamRequest) (*graphv1.GetTransitiveDownstreamResponse, error) {
	nodes, err := h.repo.GetTransitiveDownstream(ctx, req.ScheduleName, req.SchemaName, req.TableName)
	if err != nil {
		h.logger.Error("Failed to get transitive downstream", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to get transitive downstream: %v", err)
	}
	protoNodes := make([]*graphv1.TableNode, len(nodes))
	for i, n := range nodes {
		protoNodes[i] = domainToProtoNode(n)
	}
	return &graphv1.GetTransitiveDownstreamResponse{Nodes: protoNodes}, nil
}

// ============================================================================
// CONVERSION HELPERS
// ============================================================================

// domainToProtoNode converts a domain TableNode to proto TableNode
func domainToProtoNode(n *model.TableNode) *graphv1.TableNode {
	return &graphv1.TableNode{
		TableName:     n.TableName,
		SchemaName:    n.SchemaName,
		ServiceName:   n.ServiceName,
		Owner:         n.Owner,
		ScheduleName:  n.ScheduleName,
		Criticality:   domainToProtoCriticality(n.Criticality),
		LastUpdatedAt: timestamppb.New(n.LastUpdatedAt),
		CreatedAt:     timestamppb.New(n.CreatedAt),
		NodeType:      n.NodeType,
		Status:        n.Status, // populated only by GetTransitiveDownstream; empty for all other RPCs
	}
}

// protoToDomainCriticality converts proto Criticality to domain Criticality
func protoToDomainCriticality(c graphv1.Criticality) model.Criticality {
	switch c {
	case graphv1.Criticality_CRITICALITY_REGULATORY:
		return model.CriticalityRegulatory
	case graphv1.Criticality_CRITICALITY_CORE:
		return model.CriticalityCore
	case graphv1.Criticality_CRITICALITY_SECONDARY:
		return model.CriticalitySecondary
	default:
		return model.CriticalityUnspecified
	}
}

// domainToProtoCriticality converts domain Criticality to proto Criticality
func domainToProtoCriticality(c model.Criticality) graphv1.Criticality {
	switch c {
	case model.CriticalityRegulatory:
		return graphv1.Criticality_CRITICALITY_REGULATORY
	case model.CriticalityCore:
		return graphv1.Criticality_CRITICALITY_CORE
	case model.CriticalitySecondary:
		return graphv1.Criticality_CRITICALITY_SECONDARY
	default:
		return graphv1.Criticality_CRITICALITY_UNSPECIFIED
	}
}

// GetScheduleInitNodes returns all_nodes, root_nodes, and seed_nodes for a
// schedule in a single call (backed by one Neo4j read transaction).
func (h *GraphHandler) GetScheduleInitNodes(
	ctx context.Context,
	req *graphv1.GetScheduleInitNodesRequest,
) (*graphv1.GetScheduleInitNodesResponse, error) {
	if req.ScheduleName == "" || req.RunId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_name and run_id are required")
	}
	result, err := h.repo.GetScheduleInitNodes(ctx, req.ScheduleName, req.RunId)
	if err != nil {
		h.logger.Error("Failed to get schedule init nodes", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to get schedule init nodes")
	}
	return &graphv1.GetScheduleInitNodesResponse{
		AllNodes:  domainNodesToProto(result.AllNodes),
		RootNodes: domainNodesToProto(result.RootNodes),
		SeedNodes: domainNodesToProto(result.SeedNodes),
	}, nil
}

func domainNodesToProto(nodes []*model.TableNode) []*graphv1.TableNode {
	out := make([]*graphv1.TableNode, len(nodes))
	for i, n := range nodes {
		out[i] = domainToProtoNode(n)
	}
	return out
}

// UpdateNodeStatus sets the execution status on a Table node.
func (h *GraphHandler) UpdateNodeStatus(ctx context.Context, req *graphv1.UpdateNodeStatusRequest) (*graphv1.UpdateNodeStatusResponse, error) {
	if req.ScheduleName == "" || req.SchemaName == "" || req.TableName == "" || req.Status == "" || req.RunId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_name, schema_name, table_name, status, and run_id are required")
	}

	err := h.repo.UpdateNodeStatus(ctx, req.ScheduleName, req.SchemaName, req.TableName, req.Status, req.RunId)
	if err != nil {
		h.logger.Error("Failed to update node status", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to update node status")
	}

	return &graphv1.UpdateNodeStatusResponse{}, nil
}

// GetReadyDownstream finds downstream nodes ready for execution after a node succeeds.
func (h *GraphHandler) GetReadyDownstream(ctx context.Context, req *graphv1.GetReadyDownstreamRequest) (*graphv1.GetReadyDownstreamResponse, error) {
	if req.ScheduleName == "" || req.SchemaName == "" || req.TableName == "" || req.RunId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_name, schema_name, table_name, and run_id are required")
	}

	nodes, err := h.repo.GetReadyDownstream(ctx, req.ScheduleName, req.SchemaName, req.TableName, req.RunId)
	if err != nil {
		h.logger.Error("Failed to get ready downstream nodes", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to get ready downstream nodes")
	}

	protoNodes := make([]*graphv1.TableNode, len(nodes))
	for i, n := range nodes {
		protoNodes[i] = &graphv1.TableNode{
			ServiceName: n.ServiceName,
			SchemaName:  n.SchemaName,
			TableName:   n.TableName,
			NodeType:    n.NodeType,
		}
	}

	return &graphv1.GetReadyDownstreamResponse{Nodes: protoNodes}, nil
}

// CheckScheduleCompletion reports whether a schedule has finished (with or without failures).
func (h *GraphHandler) CheckScheduleCompletion(ctx context.Context, req *graphv1.CheckScheduleCompletionRequest) (*graphv1.CheckScheduleCompletionResponse, error) {
	if req.ScheduleName == "" || req.RunId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_name and run_id are required")
	}

	isComplete, hasFailed, err := h.repo.CheckScheduleCompletion(ctx, req.ScheduleName, req.RunId)
	if err != nil {
		h.logger.Error("Failed to check schedule completion", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to check schedule completion")
	}

	return &graphv1.CheckScheduleCompletionResponse{
		IsComplete: isComplete,
		HasFailed:  hasFailed,
	}, nil
}

// SnapshotGraph materializes a run-scoped execution snapshot from the live schedule graph.
func (h *GraphHandler) SnapshotGraph(ctx context.Context, req *graphv1.SnapshotGraphRequest) (*graphv1.SnapshotGraphResponse, error) {
	if req.RunId == "" || req.ScheduleName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "run_id and schedule_name are required")
	}

	if err := h.repo.SnapshotGraph(ctx, req.RunId, req.ScheduleName); err != nil {
		h.logger.Error("Failed to snapshot graph", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to snapshot graph")
	}

	return &graphv1.SnapshotGraphResponse{}, nil
}

func (h *GraphHandler) FinalizeRun(ctx context.Context, req *graphv1.FinalizeRunRequest) (*graphv1.FinalizeRunResponse, error) {
	if req.RunId == "" || req.TerminalStatus == "" {
		return nil, status.Errorf(codes.InvalidArgument, "run_id and terminal_status are required")
	}
	if err := h.repo.FinalizeRun(ctx, req.RunId, req.TerminalStatus); err != nil {
		h.logger.Error("Failed to finalize run", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to finalize run")
	}
	return &graphv1.FinalizeRunResponse{}, nil
}

func (h *GraphHandler) ListRuns(ctx context.Context, req *graphv1.ListRunsRequest) (*graphv1.ListRunsResponse, error) {
	if req.ScheduleName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_name is required")
	}
	runs, err := h.repo.ListRuns(ctx, req.ScheduleName)
	if err != nil {
		h.logger.Error("Failed to list runs", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to list runs")
	}

	protoRuns := make([]*graphv1.RunSummary, len(runs))
	for i, run := range runs {
		protoRuns[i] = &graphv1.RunSummary{
			RunId:          run.RunID,
			ScheduleName:   run.ScheduleName,
			TerminalStatus: run.TerminalStatus,
			CreatedAt:      run.CreatedAt.Format(time.RFC3339),
			CompletedAt:    run.CompletedAt.Format(time.RFC3339),
		}
	}
	return &graphv1.ListRunsResponse{Runs: protoRuns}, nil
}

func (h *GraphHandler) GetRunGraph(ctx context.Context, req *graphv1.GetRunGraphRequest) (*graphv1.GetRunGraphResponse, error) {
	if req.RunId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "run_id is required")
	}
	graph, err := h.repo.GetRunGraph(ctx, req.RunId)
	if err != nil {
		h.logger.Error("Failed to get run graph", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to get run graph")
	}

	protoNodes := make([]*graphv1.TableNode, len(graph.Nodes))
	for i, node := range graph.Nodes {
		protoNodes[i] = domainToProtoNode(node)
	}
	protoEdges := make([]*graphv1.GraphEdge, len(graph.Edges))
	for i, edge := range graph.Edges {
		protoEdges[i] = &graphv1.GraphEdge{
			FromNodeId: edge.FromNodeID,
			ToNodeId:   edge.ToNodeID,
		}
	}
	return &graphv1.GetRunGraphResponse{Nodes: protoNodes, Edges: protoEdges}, nil
}
