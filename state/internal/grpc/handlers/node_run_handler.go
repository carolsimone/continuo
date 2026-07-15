package handlers

import (
	"context"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/pkg/num"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NodeRunHandler exposes per-node run history via ListNodeRuns.
type NodeRunHandler struct {
	repo   postgres.NodeRunRepository
	logger *slog.Logger
}

// NewNodeRunHandler constructs a NodeRunHandler.
func NewNodeRunHandler(repo postgres.NodeRunRepository, logger *slog.Logger) *NodeRunHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &NodeRunHandler{repo: repo, logger: logger}
}

// ListNodeRuns returns the most recent task instances for a node, ordered by
// scheduler_tracker.created_at DESC. Identity fields are required; an empty
// triple returns InvalidArgument. limit is clamped to (0, 50] by the repository — passing 0 or >50 yields 50.
func (h *NodeRunHandler) ListNodeRuns(
	ctx context.Context,
	req *statev1.ListNodeRunsRequest,
) (*statev1.ListNodeRunsResponse, error) {
	if req.ServiceName == "" || req.SchemaName == "" || req.TableName == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"ListNodeRuns: service_name, schema_name, and table_name are required")
	}

	rows, err := h.repo.List(ctx, req.ServiceName, req.SchemaName, req.TableName, req.GetOperation(), int(req.Limit))
	if err != nil {
		h.logger.Error("ListNodeRuns repo error",
			"service", req.ServiceName, "schema", req.SchemaName, "table", req.TableName,
			"error", err)
		return nil, status.Errorf(codes.Internal, "ListNodeRuns: %v", err)
	}

	out := make([]*statev1.NodeRun, 0, len(rows))
	for _, r := range rows {
		out = append(out, &statev1.NodeRun{
			RunId:           r.ScheduleID.String(),
			ScheduleName:    r.ScheduleName,
			Kind:            r.Kind,
			TerminalStatus:  string(r.TerminalStatus),
			TaskId:          r.TaskID.String(),
			TaskStatus:      string(r.TaskStatus),
			RetryCount:      num.ClampInt32(r.RetryCount),
			ImageTag:        r.ImageTag,
			ManifestVersion: r.ManifestVersion,
			CreatedAt:       r.CreatedAt.UTC().Format(time.RFC3339),
			StartedAt:       timePtrToRFC(r.StartedAt),
			CompletedAt:     timePtrToRFC(r.CompletedAt),
			ErrorMessage:    stringPtrOrEmpty(r.ErrorMessage),
			LogS3Key:        stringPtrOrEmpty(r.LogS3Key),
			Operation:       r.Operation,
		})
	}
	return &statev1.ListNodeRunsResponse{Runs: out}, nil
}

// ListNodes returns the node catalog. limit defaults to 50 and is clamped to
// 200; offset below 0 becomes 0. search/service_name are optional filters.
func (h *NodeRunHandler) ListNodes(
	ctx context.Context,
	req *statev1.ListNodesRequest,
) (*statev1.ListNodesResponse, error) {
	limit := int(req.Limit)
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	offset := int(req.Offset)
	if offset < 0 {
		offset = 0
	}

	rows, total, err := h.repo.ListNodes(ctx, req.Search, req.ServiceName, req.GetOperation(), limit, offset)
	if err != nil {
		h.logger.Error("ListNodes repo error", "search", req.Search, "service", req.ServiceName, "error", err)
		return nil, status.Errorf(codes.Internal, "ListNodes: %v", err)
	}

	out := make([]*statev1.NodeSummary, 0, len(rows))
	for _, r := range rows {
		out = append(out, &statev1.NodeSummary{
			ServiceName:    r.ServiceName,
			SchemaName:     r.SchemaName,
			TableName:      r.TableName,
			RunCount:       num.ClampInt32(r.RunCount),
			SuccessRatePct: num.ClampInt32(r.SuccessRatePct),
			AvgDurationSec: num.ClampInt32(r.AvgDurationSec),
			P95DurationSec: num.ClampInt32(r.P95DurationSec),
			FlakyRatePct:   num.ClampInt32(r.FlakyRatePct),
			LastStatus:     r.LastStatus,
			LastRunAt:      r.LastRunAt.UTC().Format(time.RFC3339),
			Operation:      r.Operation,
		})
	}
	return &statev1.ListNodesResponse{Nodes: out, TotalCount: num.ClampInt32(total)}, nil
}

// ListNodeNames returns distinct node table names for the search autocomplete.
func (h *NodeRunHandler) ListNodeNames(ctx context.Context, req *statev1.ListNodeNamesRequest) (*statev1.ListNodeNamesResponse, error) {
	names, err := h.repo.ListNodeNames(ctx, req.ServiceName)
	if err != nil {
		h.logger.Error("ListNodeNames repo error", "service", req.ServiceName, "error", err)
		return nil, status.Errorf(codes.Internal, "ListNodeNames: %v", err)
	}
	return &statev1.ListNodeNamesResponse{TableNames: names}, nil
}

func timePtrToRFC(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func stringPtrOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
