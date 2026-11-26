package runservice

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/num"
	"github.com/carolsimone/continuo/run-controller/domain/seeds"
	runv1 "github.com/carolsimone/continuo/run-controller/proto/run/v1"
)

// ListNodeTasks returns the most recent tasks of a node, newest run first. An
// operation the operation vocabulary does not declare is InvalidArgument.
func (s *Server) ListNodeTasks(ctx context.Context, req *runv1.ListNodeTasksRequest) (*runv1.ListNodeTasksResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetProjectName() == "" || req.GetSchemaName() == "" || req.GetTableName() == "" {
		return nil, status.Error(codes.InvalidArgument, "project_name, schema_name and table_name are required")
	}
	operation, err := model.ParseOperation(req.GetOperation())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid operation: %v", err)
	}
	rows, err := s.reads.Nodes.ListNodeTasks(ctx, tenantID, req.GetProjectName(), req.GetSchemaName(), req.GetTableName(), operation, int(req.GetLimit()))
	if err != nil {
		return nil, s.internalError("ListNodeTasks", err,
			"project", req.GetProjectName(), "schema", req.GetSchemaName(), "table", req.GetTableName())
	}
	outage, found, err := s.reads.Nodes.ViewOutage(ctx, tenantID, req.GetSchemaName(), req.GetTableName())
	if err != nil {
		return nil, s.internalError("ListNodeTasks", err,
			"project", req.GetProjectName(), "schema", req.GetSchemaName(), "table", req.GetTableName())
	}
	out := make([]*runv1.NodeTask, 0, len(rows))
	for _, r := range rows {
		out = append(out, &runv1.NodeTask{
			RunId:          r.ScheduleID.String(),
			ScheduleName:   r.ScheduleName,
			Kind:           r.Kind,
			RunStatus:      r.TerminalStatus,
			TaskId:         r.TaskID.String(),
			TaskStatus:     string(r.TaskStatus),
			Attempt:        num.ClampInt32(r.Attempt),
			ImageTag:       r.ImageTag,
			CreatedAt:      r.CreatedAt.UTC().Format(time.RFC3339),
			StartedAt:      rfc3339OrEmpty(r.StartedAt),
			CompletedAt:    rfc3339OrEmpty(r.CompletedAt),
			ErrorMessage:   stringOrEmpty(r.ErrorMessage),
			LogS3Key:       stringOrEmpty(r.LogS3Key),
			Operation:      r.Operation,
			RunResultsUri:  stringOrEmpty(r.RunResultsURI),
			ContentHash:    r.ContentHash,
			FailureClass:   stringOrEmpty(r.FailureClass),
			FailureReason:  stringOrEmpty(r.FailureReason),
			WaitReason:     string(r.WaitReason),
			WaitingOnRunId: uuidOrEmpty(r.WaitingOnRunID),
			WaitingSince:   rfc3339OrEmpty(r.WaitingSince),

			BlockingObjects: r.BlockingObjects,
		})
	}
	resp := &runv1.ListNodeTasksResponse{Tasks: out}
	if found {
		resp.Outage = toProtoOutage(outage)
	}
	return resp, nil
}

func toProtoOutage(o seeds.ViewOutage) *runv1.ViewOutage {
	return &runv1.ViewOutage{
		State:      string(o.State),
		Since:      o.Since.UTC().Format(time.RFC3339),
		SeedNodeId: string(o.SeedKey),
		RunId:      o.RunID.String(),
	}
}

// ListNodes returns one page of the node list: the nodes of the live
// release, each with its type, whether it is dormant and its run stats; a
// node that never ran has no last run. An operation the operation vocabulary
// does not declare is InvalidArgument.
func (s *Server) ListNodes(ctx context.Context, req *runv1.ListNodesRequest) (*runv1.ListNodesResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	operation, err := model.ParseOperation(req.GetOperation())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid operation: %v", err)
	}
	rows, total, err := s.reads.Nodes.ListNodes(ctx, tenantID, req.GetSearch(), req.GetProjectName(), operation, req.GetDormantOnly(),
		int(req.GetLimit()), int(req.GetOffset()))
	if err != nil {
		return nil, s.internalError("ListNodes", err, "search", req.GetSearch(), "project", req.GetProjectName())
	}
	out := make([]*runv1.NodeSummary, 0, len(rows))
	for _, r := range rows {
		out = append(out, &runv1.NodeSummary{
			ProjectName:     r.ProjectName,
			SchemaName:      r.SchemaName,
			TableName:       r.TableName,
			TaskCount:       num.ClampInt32(r.RunCount),
			SuccessRatePct:  num.ClampInt32(r.SuccessRatePct),
			AvgDurationSec:  num.ClampInt32(r.AvgDurationSec),
			P95DurationSec:  num.ClampInt32(r.P95DurationSec),
			FlakyRatePct:    num.ClampInt32(r.FlakyRatePct),
			LastStatus:      r.LastStatus,
			LastRunAt:       rfc3339OrEmpty(r.LastRunAt),
			Operation:       r.Operation,
			TimeoutSeconds:  num.ClampInt32(int64(r.Timeout / time.Second)),
			TimeoutDeclared: r.TimeoutDeclared,
			NodeType:        r.NodeType,
			Dormant:         r.Dormant,
		})
	}
	return &runv1.ListNodesResponse{Nodes: out, TotalCount: num.ClampInt32(total)}, nil
}

// ListNodeNames returns the distinct table names of the nodes ListNodes lists.
func (s *Server) ListNodeNames(ctx context.Context, req *runv1.ListNodeNamesRequest) (*runv1.ListNodeNamesResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	names, err := s.reads.Nodes.ListNodeNames(ctx, tenantID, req.GetProjectName())
	if err != nil {
		return nil, s.internalError("ListNodeNames", err, "project", req.GetProjectName())
	}
	return &runv1.ListNodeNamesResponse{TableNames: names}, nil
}

// ListNodeProjects returns the distinct project names of the nodes ListNodes
// lists.
func (s *Server) ListNodeProjects(ctx context.Context, _ *runv1.ListNodeProjectsRequest) (*runv1.ListNodeProjectsResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	projects, err := s.reads.Nodes.ListNodeProjects(ctx, tenantID)
	if err != nil {
		return nil, s.internalError("ListNodeProjects", err)
	}
	return &runv1.ListNodeProjectsResponse{ProjectNames: projects}, nil
}
