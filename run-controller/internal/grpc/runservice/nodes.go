package runservice

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/num"
	runv1 "github.com/carolsimone/continuo/run-controller/proto/run/v1"
)

// ListNodeTasks returns the most recent tasks of a node, newest run first. An
// operation the operation vocabulary does not declare is InvalidArgument.
func (s *Server) ListNodeTasks(ctx context.Context, req *runv1.ListNodeTasksRequest) (*runv1.ListNodeTasksResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetServiceName() == "" || req.GetSchemaName() == "" || req.GetTableName() == "" {
		return nil, status.Error(codes.InvalidArgument, "service_name, schema_name and table_name are required")
	}
	operation, err := model.ParseOperation(req.GetOperation())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid operation: %v", err)
	}
	rows, err := s.reads.Nodes.ListNodeTasks(ctx, tenantID, req.GetServiceName(), req.GetSchemaName(), req.GetTableName(), operation, int(req.GetLimit()))
	if err != nil {
		return nil, s.internalError("ListNodeTasks", err,
			"service", req.GetServiceName(), "schema", req.GetSchemaName(), "table", req.GetTableName())
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
		})
	}
	return &runv1.ListNodeTasksResponse{Tasks: out}, nil
}

// ListNodes returns one page of the node list. An operation the operation
// vocabulary does not declare is InvalidArgument.
func (s *Server) ListNodes(ctx context.Context, req *runv1.ListNodesRequest) (*runv1.ListNodesResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	operation, err := model.ParseOperation(req.GetOperation())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid operation: %v", err)
	}
	rows, total, err := s.reads.Nodes.ListNodes(ctx, tenantID, req.GetSearch(), req.GetServiceName(), operation, int(req.GetLimit()), int(req.GetOffset()))
	if err != nil {
		return nil, s.internalError("ListNodes", err, "search", req.GetSearch(), "service", req.GetServiceName())
	}
	out := make([]*runv1.NodeSummary, 0, len(rows))
	for _, r := range rows {
		out = append(out, &runv1.NodeSummary{
			ServiceName:     r.ServiceName,
			SchemaName:      r.SchemaName,
			TableName:       r.TableName,
			TaskCount:       num.ClampInt32(r.RunCount),
			SuccessRatePct:  num.ClampInt32(r.SuccessRatePct),
			AvgDurationSec:  num.ClampInt32(r.AvgDurationSec),
			P95DurationSec:  num.ClampInt32(r.P95DurationSec),
			FlakyRatePct:    num.ClampInt32(r.FlakyRatePct),
			LastStatus:      r.LastStatus,
			LastRunAt:       r.LastRunAt.UTC().Format(time.RFC3339),
			Operation:       r.Operation,
			TimeoutSeconds:  num.ClampInt32(int64(r.Timeout / time.Second)),
			TimeoutDeclared: r.TimeoutDeclared,
		})
	}
	return &runv1.ListNodesResponse{Nodes: out, TotalCount: num.ClampInt32(total)}, nil
}

// ListNodeNames returns the distinct table names of nodes that have run.
func (s *Server) ListNodeNames(ctx context.Context, req *runv1.ListNodeNamesRequest) (*runv1.ListNodeNamesResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	names, err := s.reads.Nodes.ListNodeNames(ctx, tenantID, req.GetServiceName())
	if err != nil {
		return nil, s.internalError("ListNodeNames", err, "service", req.GetServiceName())
	}
	return &runv1.ListNodeNamesResponse{TableNames: names}, nil
}

// ListNodeServices returns the distinct service names of nodes that have run.
func (s *Server) ListNodeServices(ctx context.Context, _ *runv1.ListNodeServicesRequest) (*runv1.ListNodeServicesResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	services, err := s.reads.Nodes.ListNodeServices(ctx, tenantID)
	if err != nil {
		return nil, s.internalError("ListNodeServices", err)
	}
	return &runv1.ListNodeServicesResponse{ServiceNames: services}, nil
}
