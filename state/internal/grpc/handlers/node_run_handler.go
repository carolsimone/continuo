package handlers

import (
	"context"
	"log/slog"
	"time"

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
// triple returns InvalidArgument. The server caps limit at 50.
func (h *NodeRunHandler) ListNodeRuns(
	ctx context.Context,
	req *statev1.ListNodeRunsRequest,
) (*statev1.ListNodeRunsResponse, error) {
	if req.ServiceName == "" || req.SchemaName == "" || req.TableName == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"ListNodeRuns: service_name, schema_name, and table_name are required")
	}

	rows, err := h.repo.List(ctx, req.ServiceName, req.SchemaName, req.TableName, int(req.Limit))
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
			RetryCount:      int32(r.RetryCount),
			ImageTag:        r.ImageTag,
			ManifestVersion: r.ManifestVersion,
			CreatedAt:       r.CreatedAt.UTC().Format(time.RFC3339),
			StartedAt:       timePtrToRFC(r.StartedAt),
			CompletedAt:     timePtrToRFC(r.CompletedAt),
			ErrorMessage:    stringPtrOrEmpty(r.ErrorMessage),
			LogS3Key:        stringPtrOrEmpty(r.LogS3Key),
		})
	}
	return &statev1.ListNodeRunsResponse{Runs: out}, nil
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
