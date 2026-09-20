// Package client wraps the generated gRPC clients so commands do not import
// google.golang.org/grpc directly.
package client

import (
	"context"

	statev1 "github.com/carolsimone/continuo/cli/proto/state/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// StateClient is the narrow interface the CLI depends on. Fakes in tests
// implement this; the real implementation lives in stateGRPCClient.
type StateClient interface {
	TriggerSchedule(ctx context.Context, scheduleName, actor string) (*statev1.TriggerScheduleResponse, error)
	TriggerScheduleTest(ctx context.Context, scheduleName, actor string) (*statev1.TriggerScheduleResponse, error)
	ListAllSchedules(ctx context.Context) (*statev1.ListAllSchedulesResponse, error)
	ListTasks(ctx context.Context, scheduleID string, status statev1.TaskStatus, pageSize, pageOffset int32) (*statev1.ListTasksResponse, error)
	CancelSchedule(ctx context.Context, scheduleName, reason, by string) (*statev1.CancelScheduleResponse, error)
	ListNodeRuns(ctx context.Context, service, schema, table, operation string, limit int32) (*statev1.ListNodeRunsResponse, error)
	ListNodes(ctx context.Context, search, service, operation string, limit, offset int32) (*statev1.ListNodesResponse, error)
	TriggerNodeRun(ctx context.Context, service, schema, table, sourceRunID, actor string) (*statev1.TriggerSingleNodeRunResponse, error)
	TriggerNodeTest(ctx context.Context, service, schema, table, sourceRunID, actor string) (*statev1.TriggerSingleNodeRunResponse, error)
	TriggerNodeBuild(ctx context.Context, service, schema, table, sourceRunID, actor string) (*statev1.TriggerSingleNodeRunResponse, error)
	TriggerScheduleBuild(ctx context.Context, scheduleName, actor string) (*statev1.TriggerScheduleResponse, error)
	Close() error
}

// NewStateClient dials the given endpoint and returns a production StateClient.
// The returned client must be Closed by the caller.
func NewStateClient(ctx context.Context, endpoint string) (StateClient, error) {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &stateGRPCClient{conn: conn, api: statev1.NewStateServiceClient(conn)}, nil
}

type stateGRPCClient struct {
	conn *grpc.ClientConn
	api  statev1.StateServiceClient
}

func (c *stateGRPCClient) TriggerSchedule(ctx context.Context, scheduleName, actor string) (*statev1.TriggerScheduleResponse, error) {
	if actor != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, userIDMetadataKey, actor)
	}
	return c.api.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{ScheduleName: scheduleName})
}

func (c *stateGRPCClient) TriggerScheduleTest(ctx context.Context, scheduleName, actor string) (*statev1.TriggerScheduleResponse, error) {
	if actor != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, userIDMetadataKey, actor)
	}
	return c.api.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{ScheduleName: scheduleName, Operation: "test"})
}

func (c *stateGRPCClient) TriggerScheduleBuild(ctx context.Context, scheduleName, actor string) (*statev1.TriggerScheduleResponse, error) {
	if actor != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, userIDMetadataKey, actor)
	}
	return c.api.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{ScheduleName: scheduleName, Operation: "build"})
}

func (c *stateGRPCClient) ListAllSchedules(ctx context.Context) (*statev1.ListAllSchedulesResponse, error) {
	return c.api.ListAllSchedules(ctx, &statev1.ListAllSchedulesRequest{})
}

func (c *stateGRPCClient) ListTasks(ctx context.Context, scheduleID string, taskStatus statev1.TaskStatus, pageSize, pageOffset int32) (*statev1.ListTasksResponse, error) {
	return c.api.ListTasks(ctx, &statev1.ListTasksRequest{
		ScheduleId: scheduleID,
		Status:     taskStatus,
		PageSize:   pageSize,
		PageOffset: pageOffset,
	})
}

func (c *stateGRPCClient) CancelSchedule(ctx context.Context, scheduleName, reason, by string) (*statev1.CancelScheduleResponse, error) {
	return c.api.CancelSchedule(ctx, &statev1.CancelScheduleRequest{
		ScheduleName:       scheduleName,
		CancellationReason: reason,
		CancelledBy:        by,
	})
}

// userIDMetadataKey is the gRPC metadata header the state service reads to
// attribute an action to an initiating identity. It mirrors
// pkg/identity.MetadataKey, which the CLI cannot import (public-gRPC-only rule),
// so the literal is duplicated here deliberately.
const userIDMetadataKey = "x-continuo-user-id"

func (c *stateGRPCClient) ListNodes(ctx context.Context, search, service, operation string, limit, offset int32) (*statev1.ListNodesResponse, error) {
	return c.api.ListNodes(ctx, &statev1.ListNodesRequest{
		Search:      search,
		ServiceName: service,
		Operation:   operation,
		Limit:       limit,
		Offset:      offset,
	})
}

func (c *stateGRPCClient) ListNodeRuns(ctx context.Context, service, schema, table, operation string, limit int32) (*statev1.ListNodeRunsResponse, error) {
	return c.api.ListNodeRuns(ctx, &statev1.ListNodeRunsRequest{
		ServiceName: service,
		SchemaName:  schema,
		TableName:   table,
		Operation:   operation,
		Limit:       limit,
	})
}

// singleNodeRequest builds a TriggerSingleNodeRun request. An empty
// sourceRunID selects the "latest" metadata mode; a non-empty one selects
// "snapshot_of_run", pinning the run to the metadata the given past run used.
// The state service rejects any other combination, so the CLI never sends one.
func singleNodeRequest(service, schema, table, sourceRunID, operation string) *statev1.TriggerSingleNodeRunRequest {
	req := &statev1.TriggerSingleNodeRunRequest{
		ServiceName:    service,
		SchemaName:     schema,
		TableName:      table,
		MetadataSource: metadataSourceLatest,
		Operation:      operation,
	}
	if sourceRunID != "" {
		req.MetadataSource = metadataSourceSnapshotOfRun
		req.SourceRunId = sourceRunID
	}
	return req
}

// metadata_source values accepted by TriggerSingleNodeRun. They mirror the
// state service's run.MetadataSource* constants, which the CLI cannot import
// (public-gRPC-only rule), so the literals are duplicated here deliberately.
const (
	metadataSourceLatest        = "latest"
	metadataSourceSnapshotOfRun = "snapshot_of_run"
)

func (c *stateGRPCClient) triggerSingleNode(ctx context.Context, req *statev1.TriggerSingleNodeRunRequest, actor string) (*statev1.TriggerSingleNodeRunResponse, error) {
	if actor != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, userIDMetadataKey, actor)
	}
	return c.api.TriggerSingleNodeRun(ctx, req)
}

func (c *stateGRPCClient) TriggerNodeRun(ctx context.Context, service, schema, table, sourceRunID, actor string) (*statev1.TriggerSingleNodeRunResponse, error) {
	return c.triggerSingleNode(ctx, singleNodeRequest(service, schema, table, sourceRunID, ""), actor)
}

func (c *stateGRPCClient) TriggerNodeTest(ctx context.Context, service, schema, table, sourceRunID, actor string) (*statev1.TriggerSingleNodeRunResponse, error) {
	return c.triggerSingleNode(ctx, singleNodeRequest(service, schema, table, sourceRunID, "test"), actor)
}

func (c *stateGRPCClient) TriggerNodeBuild(ctx context.Context, service, schema, table, sourceRunID, actor string) (*statev1.TriggerSingleNodeRunResponse, error) {
	return c.triggerSingleNode(ctx, singleNodeRequest(service, schema, table, sourceRunID, "build"), actor)
}

func (c *stateGRPCClient) Close() error { return c.conn.Close() }
