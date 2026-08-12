package grpc

import (
	"context"
	"errors"
	"log/slog"
	"time"

	orchestratorv1 "github.com/carolsimone/continuo/orchestrator/api/orchestrator/v1"
	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/codeversion"
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

// CodeVersionHistoryReader returns the code-version history views served by
// the code-version RPCs: node version chains, rendered diffs, upstream
// changes, shared-code unit chains, and run history. Cap policy (upstream
// ancestor count, diff byte size) and limit defaulting/clamping live here,
// not in the handler. Satisfied by service/queries.CodeVersionQueryService.
type CodeVersionHistoryReader interface {
	GetNodeVersions(ctx context.Context, uniqueID string, limit int32) ([]codeversion.VersionView, error)
	GetNodeVersionDiff(ctx context.Context, uniqueID string, fromSeq, toSeq int64) (*codeversion.VersionDiff, error)
	GetUpstreamChanges(ctx context.Context, uniqueID string, depth int32, since time.Time) ([]codeversion.UpstreamChange, error)
	GetCodeUnitVersions(ctx context.Context, unitID, uniqueID string, limit int32) ([]codeversion.UnitVersionView, error)
	GetNodeRunHistory(ctx context.Context, uniqueID string, limit int32) ([]codeversion.RunExecution, error)
}

// QueryHandler implements the OrchestratorQuery gRPC service.
type QueryHandler struct {
	orchestratorv1.UnimplementedOrchestratorQueryServer
	scheduleAndRunLists ScheduleAndRunListReader
	driftAwareRuns      DriftAwareRunReader
	codeVersions        CodeVersionHistoryReader
	logger              *slog.Logger
}

func NewQueryHandler(scheduleAndRunLists ScheduleAndRunListReader, driftAwareRuns DriftAwareRunReader, codeVersions CodeVersionHistoryReader, logger *slog.Logger) *QueryHandler {
	return &QueryHandler{scheduleAndRunLists: scheduleAndRunLists, driftAwareRuns: driftAwareRuns, codeVersions: codeVersions, logger: logger}
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

// maxUpstreamDepth caps GetUpstreamChanges' depth request; the proto
// documents this as the maximum and rejects anything above it.
const maxUpstreamDepth = 10

func (h *QueryHandler) GetNodeVersions(ctx context.Context, req *orchestratorv1.GetNodeVersionsRequest) (*orchestratorv1.GetNodeVersionsResponse, error) {
	if req.UniqueId == "" {
		return nil, status.Error(codes.InvalidArgument, "unique_id is required")
	}
	versions, err := h.codeVersions.GetNodeVersions(ctx, req.UniqueId, req.Limit)
	if err != nil {
		if errors.Is(err, domain.ErrNodeNotFound) {
			return nil, status.Errorf(codes.NotFound, "node %q not found", req.UniqueId)
		}
		h.logger.Error("GetNodeVersions failed", "unique_id", req.UniqueId, "error", err)
		return nil, status.Errorf(codes.Internal, "GetNodeVersions: %v", err)
	}
	resp := &orchestratorv1.GetNodeVersionsResponse{
		Versions: make([]*orchestratorv1.VersionView, 0, len(versions)),
	}
	for _, v := range versions {
		resp.Versions = append(resp.Versions, codeVersionViewToProto(v))
	}
	return resp, nil
}

func (h *QueryHandler) GetNodeVersionDiff(ctx context.Context, req *orchestratorv1.GetNodeVersionDiffRequest) (*orchestratorv1.GetNodeVersionDiffResponse, error) {
	if req.UniqueId == "" {
		return nil, status.Error(codes.InvalidArgument, "unique_id is required")
	}
	if req.FromSeq == req.ToSeq {
		return nil, status.Error(codes.InvalidArgument, "from_seq and to_seq must differ")
	}
	diff, err := h.codeVersions.GetNodeVersionDiff(ctx, req.UniqueId, req.FromSeq, req.ToSeq)
	if err != nil {
		if errors.Is(err, domain.ErrNodeNotFound) {
			return nil, status.Errorf(codes.NotFound, "node %q version %d or %d not found", req.UniqueId, req.FromSeq, req.ToSeq)
		}
		h.logger.Error("GetNodeVersionDiff failed", "unique_id", req.UniqueId, "error", err)
		return nil, status.Errorf(codes.Internal, "GetNodeVersionDiff: %v", err)
	}
	return &orchestratorv1.GetNodeVersionDiffResponse{Diff: codeVersionDiffToProto(*diff)}, nil
}

func (h *QueryHandler) GetUpstreamChanges(ctx context.Context, req *orchestratorv1.GetUpstreamChangesRequest) (*orchestratorv1.GetUpstreamChangesResponse, error) {
	if req.UniqueId == "" {
		return nil, status.Error(codes.InvalidArgument, "unique_id is required")
	}
	if req.Depth > maxUpstreamDepth {
		return nil, status.Errorf(codes.InvalidArgument, "depth must be <= %d", maxUpstreamDepth)
	}
	var since time.Time
	if req.Since != "" {
		parsed, err := time.Parse(time.RFC3339, req.Since)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "since must be RFC3339: %v", err)
		}
		since = parsed
	}
	changes, err := h.codeVersions.GetUpstreamChanges(ctx, req.UniqueId, req.Depth, since)
	if err != nil {
		if errors.Is(err, domain.ErrNodeNotFound) {
			return nil, status.Errorf(codes.NotFound, "node %q not found", req.UniqueId)
		}
		h.logger.Error("GetUpstreamChanges failed", "unique_id", req.UniqueId, "error", err)
		return nil, status.Errorf(codes.Internal, "GetUpstreamChanges: %v", err)
	}
	resp := &orchestratorv1.GetUpstreamChangesResponse{
		Changes: make([]*orchestratorv1.UpstreamChange, 0, len(changes)),
	}
	for _, c := range changes {
		resp.Changes = append(resp.Changes, codeUpstreamChangeToProto(c))
	}
	return resp, nil
}

func (h *QueryHandler) GetCodeUnitVersions(ctx context.Context, req *orchestratorv1.GetCodeUnitVersionsRequest) (*orchestratorv1.GetCodeUnitVersionsResponse, error) {
	if (req.UnitId == "") == (req.UniqueId == "") {
		return nil, status.Error(codes.InvalidArgument, "exactly one of unit_id or unique_id is required")
	}
	versions, err := h.codeVersions.GetCodeUnitVersions(ctx, req.UnitId, req.UniqueId, req.Limit)
	if err != nil {
		if errors.Is(err, domain.ErrNodeNotFound) {
			id := req.UnitId
			if id == "" {
				id = req.UniqueId
			}
			return nil, status.Errorf(codes.NotFound, "%q not found", id)
		}
		h.logger.Error("GetCodeUnitVersions failed", "unit_id", req.UnitId, "unique_id", req.UniqueId, "error", err)
		return nil, status.Errorf(codes.Internal, "GetCodeUnitVersions: %v", err)
	}
	resp := &orchestratorv1.GetCodeUnitVersionsResponse{
		Versions: make([]*orchestratorv1.UnitVersionView, 0, len(versions)),
	}
	for _, v := range versions {
		resp.Versions = append(resp.Versions, codeUnitVersionViewToProto(v))
	}
	return resp, nil
}

func (h *QueryHandler) GetNodeRunHistory(ctx context.Context, req *orchestratorv1.GetNodeRunHistoryRequest) (*orchestratorv1.GetNodeRunHistoryResponse, error) {
	if req.UniqueId == "" {
		return nil, status.Error(codes.InvalidArgument, "unique_id is required")
	}
	runs, err := h.codeVersions.GetNodeRunHistory(ctx, req.UniqueId, req.Limit)
	if err != nil {
		if errors.Is(err, domain.ErrNodeNotFound) {
			return nil, status.Errorf(codes.NotFound, "node %q not found", req.UniqueId)
		}
		h.logger.Error("GetNodeRunHistory failed", "unique_id", req.UniqueId, "error", err)
		return nil, status.Errorf(codes.Internal, "GetNodeRunHistory: %v", err)
	}
	resp := &orchestratorv1.GetNodeRunHistoryResponse{
		Runs: make([]*orchestratorv1.RunExecution, 0, len(runs)),
	}
	for _, r := range runs {
		resp.Runs = append(resp.Runs, codeRunExecutionToProto(r))
	}
	return resp, nil
}

// formatOptionalRFC3339 renders t as RFC3339, or "" for the zero time — the
// wire contract's convention for "not recorded" across every code-version
// timestamp field.
func formatOptionalRFC3339(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

func codeVersionViewToProto(v codeversion.VersionView) *orchestratorv1.VersionView {
	return &orchestratorv1.VersionView{
		UniqueId:          v.UniqueID,
		VersionSeq:        v.VersionSeq,
		ContentHash:       v.ContentHash,
		SourceHash:        v.SourceHash,
		SharedCodeHash:    v.SharedCodeHash,
		ConfigHash:        v.ConfigHash,
		Runtime:           v.Runtime,
		RawCode:           v.RawCode,
		CompiledCode:      v.CompiledCode,
		CompiledTruncated: v.CompiledTruncated,
		ConfigJson:        v.ConfigJSON,
		Repo:              v.Repo,
		CommitSha:         v.CommitSHA,
		ReleaseId:         v.ReleaseID,
		PromotedAt:        formatOptionalRFC3339(v.PromotedAt),
		Healed:            v.Healed,
		Backfilled:        v.Backfilled,
		IsCurrent:         v.IsCurrent,
	}
}

func codeVersionDiffToProto(d codeversion.VersionDiff) *orchestratorv1.VersionDiff {
	return &orchestratorv1.VersionDiff{
		UniqueId:          d.UniqueID,
		From:              codeVersionViewToProto(d.From),
		To:                codeVersionViewToProto(d.To),
		RawCodeDiff:       d.RawCodeDiff,
		ConfigDiff:        d.ConfigDiff,
		SourceChanged:     d.SourceChanged,
		SharedCodeChanged: d.SharedCodeChanged,
		ConfigChanged:     d.ConfigChanged,
		Truncated:         d.Truncated,
	}
}

func codeUpstreamChangeToProto(c codeversion.UpstreamChange) *orchestratorv1.UpstreamChange {
	return &orchestratorv1.UpstreamChange{
		UniqueId: c.UniqueID,
		Depth:    c.Depth,
		Diff:     codeVersionDiffToProto(c.Diff),
	}
}

func codeUnitVersionViewToProto(v codeversion.UnitVersionView) *orchestratorv1.UnitVersionView {
	return &orchestratorv1.UnitVersionView{
		UnitId:     v.UnitID,
		Checksum:   v.Checksum,
		Source:     v.Source,
		Repo:       v.Repo,
		CommitSha:  v.CommitSHA,
		ReleaseId:  v.ReleaseID,
		PromotedAt: formatOptionalRFC3339(v.PromotedAt),
		IsCurrent:  v.IsCurrent,
	}
}

func codeRunExecutionToProto(r codeversion.RunExecution) *orchestratorv1.RunExecution {
	return &orchestratorv1.RunExecution{
		RunId:        r.RunID,
		TaskId:       r.TaskID,
		Status:       r.Status,
		ScheduleName: r.ScheduleName,
		Operation:    r.Operation,
		ImageTag:     r.ImageTag,
		ContentHash:  r.ContentHash,
		CreatedAt:    formatOptionalRFC3339(r.CreatedAt),
		CompletedAt:  formatOptionalRFC3339(r.CompletedAt),
	}
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
