// Package client wraps the generated gRPC clients so commands do not import
// google.golang.org/grpc directly.
package client

import (
	"context"
	"errors"
	"time"

	"github.com/carolsimone/continuo/cli/internal/output"
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// RunClient is the narrow interface the CLI depends on for runs, tasks and
// schedules. Fakes in tests implement it; the real implementation lives in
// runGRPCClient. A method that starts or cancels a run takes the request id
// the server makes the command idempotent on.
type RunClient interface {
	TriggerSchedule(ctx context.Context, requestID, scheduleName, actor string) (*runv1.StartRunResponse, error)
	TriggerScheduleTest(ctx context.Context, requestID, scheduleName, actor string) (*runv1.StartRunResponse, error)
	TriggerScheduleBuild(ctx context.Context, requestID, scheduleName, actor string) (*runv1.StartRunResponse, error)
	CancelSchedule(ctx context.Context, requestID, scheduleName, reason, by string) (*runv1.CancelActiveRunResponse, error)
	TriggerNodeRun(ctx context.Context, requestID, service, schema, table, sourceRunID, actor string) (*runv1.TriggerSingleNodeRunResponse, error)
	TriggerNodeTest(ctx context.Context, requestID, service, schema, table, sourceRunID, actor string) (*runv1.TriggerSingleNodeRunResponse, error)
	TriggerNodeBuild(ctx context.Context, requestID, service, schema, table, sourceRunID, actor string) (*runv1.TriggerSingleNodeRunResponse, error)
	TriggerNodeFullRefresh(ctx context.Context, requestID, service, schema, table, sourceRunID, actor string) (*runv1.TriggerSingleNodeRunResponse, error)

	ListAllSchedules(ctx context.Context) (*runv1.ListSchedulesResponse, error)
	GetScheduleGraph(ctx context.Context, scheduleName string) (*runv1.GetScheduleGraphResponse, error)
	ListTasks(ctx context.Context, runID, status string, pageSize, pageOffset int32) (*runv1.ListTasksResponse, error)
	ListNodeRuns(ctx context.Context, service, schema, table, operation string, limit int32) (*runv1.ListNodeTasksResponse, error)
	ListNodes(ctx context.Context, search, service, operation string, limit, offset int32) (*runv1.ListNodesResponse, error)
	Close() error
}

// retryDelays are the waits before the second and the third try of a command
// the server refused as UNAVAILABLE.
var retryDelays = []time.Duration{100 * time.Millisecond, 400 * time.Millisecond}

// commandMethods are the RPCs that start or cancel a run by request id. The
// dialled client retries these on UNAVAILABLE, and only these: the server
// answers a repeated id with the run its first call made, so a retry cannot
// duplicate work, while a read needs no retry.
var commandMethods = map[string]bool{
	runv1.RunService_StartRun_FullMethodName:             true,
	runv1.RunService_TriggerRerun_FullMethodName:         true,
	runv1.RunService_TriggerRebase_FullMethodName:        true,
	runv1.RunService_TriggerSingleNodeRun_FullMethodName: true,
	runv1.RunService_CancelActiveRun_FullMethodName:      true,
}

// retryCommandInterceptor repeats a command call that failed UNAVAILABLE, up
// to len(retryDelays)+1 tries, sending the same request, and so the same
// request id, each time. A refusal for maintenance is UNAVAILABLE too but lasts
// until an operator ends it, so it is returned at once.
func retryCommandInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	if !commandMethods[method] {
		return err
	}
	for _, delay := range retryDelays {
		if !retryable(err) {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
		err = invoker(ctx, method, req, reply, cc, opts...)
	}
	return err
}

// retryable reports whether err is a plain UNAVAILABLE, not a maintenance refusal.
func retryable(err error) bool {
	return err != nil && !errors.Is(err, output.ErrMaintenance) && status.Code(err) == codes.Unavailable
}

// runDialOptions are the dial options of the production client: plaintext
// transport, the maintenance interceptor and, outside it, the command retry.
func runDialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(retryCommandInterceptor, maintenanceInterceptor),
	}
}

// NewRunClient dials the given endpoint and returns a production RunClient.
// The returned client must be Closed by the caller.
func NewRunClient(_ context.Context, endpoint string) (RunClient, error) {
	opts := append(runDialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.NewClient(endpoint, opts...)
	if err != nil {
		return nil, err
	}
	return &runGRPCClient{conn: conn, api: runv1.NewRunServiceClient(conn)}, nil
}

type runGRPCClient struct {
	conn *grpc.ClientConn
	api  runv1.RunServiceClient
}

// userIDMetadataKey is the gRPC metadata header the state service reads to
// attribute an action to an initiating identity. It mirrors
// pkg/identity.MetadataKey, which the CLI cannot import (public-gRPC-only rule),
// so the literal is duplicated here deliberately.
const userIDMetadataKey = "x-continuo-user-id"

// withActor attaches the initiating identity to ctx; an empty actor adds nothing.
func withActor(ctx context.Context, actor string) context.Context {
	if actor == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, userIDMetadataKey, actor)
}

func (c *runGRPCClient) startRun(ctx context.Context, requestID, scheduleName, operation, actor string) (*runv1.StartRunResponse, error) {
	return c.api.StartRun(withActor(ctx, actor), &runv1.StartRunRequest{
		RequestId:    requestID,
		ScheduleName: scheduleName,
		Operation:    operation,
	})
}

func (c *runGRPCClient) TriggerSchedule(ctx context.Context, requestID, scheduleName, actor string) (*runv1.StartRunResponse, error) {
	return c.startRun(ctx, requestID, scheduleName, "", actor)
}

func (c *runGRPCClient) TriggerScheduleTest(ctx context.Context, requestID, scheduleName, actor string) (*runv1.StartRunResponse, error) {
	return c.startRun(ctx, requestID, scheduleName, "test", actor)
}

func (c *runGRPCClient) TriggerScheduleBuild(ctx context.Context, requestID, scheduleName, actor string) (*runv1.StartRunResponse, error) {
	return c.startRun(ctx, requestID, scheduleName, "build", actor)
}

func (c *runGRPCClient) CancelSchedule(ctx context.Context, requestID, scheduleName, reason, by string) (*runv1.CancelActiveRunResponse, error) {
	return c.api.CancelActiveRun(ctx, &runv1.CancelActiveRunRequest{
		RequestId:          requestID,
		ScheduleName:       scheduleName,
		CancelledBy:        by,
		CancellationReason: reason,
	})
}

func (c *runGRPCClient) ListAllSchedules(ctx context.Context) (*runv1.ListSchedulesResponse, error) {
	return c.api.ListSchedules(ctx, &runv1.ListSchedulesRequest{})
}

func (c *runGRPCClient) GetScheduleGraph(ctx context.Context, scheduleName string) (*runv1.GetScheduleGraphResponse, error) {
	return c.api.GetScheduleGraph(ctx, &runv1.GetScheduleGraphRequest{ScheduleName: scheduleName})
}

func (c *runGRPCClient) ListTasks(ctx context.Context, runID, taskStatus string, pageSize, pageOffset int32) (*runv1.ListTasksResponse, error) {
	return c.api.ListTasks(ctx, &runv1.ListTasksRequest{
		RunId:      runID,
		Status:     taskStatus,
		PageSize:   pageSize,
		PageOffset: pageOffset,
	})
}

func (c *runGRPCClient) ListNodes(ctx context.Context, search, service, operation string, limit, offset int32) (*runv1.ListNodesResponse, error) {
	return c.api.ListNodes(ctx, &runv1.ListNodesRequest{
		Search:      search,
		ServiceName: service,
		Operation:   operation,
		Limit:       limit,
		Offset:      offset,
	})
}

func (c *runGRPCClient) ListNodeRuns(ctx context.Context, service, schema, table, operation string, limit int32) (*runv1.ListNodeTasksResponse, error) {
	return c.api.ListNodeTasks(ctx, &runv1.ListNodeTasksRequest{
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
func singleNodeRequest(requestID, service, schema, table, sourceRunID, operation string) *runv1.TriggerSingleNodeRunRequest {
	req := &runv1.TriggerSingleNodeRunRequest{
		RequestId:      requestID,
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

func (c *runGRPCClient) triggerSingleNode(ctx context.Context, req *runv1.TriggerSingleNodeRunRequest, actor string) (*runv1.TriggerSingleNodeRunResponse, error) {
	return c.api.TriggerSingleNodeRun(withActor(ctx, actor), req)
}

func (c *runGRPCClient) TriggerNodeRun(ctx context.Context, requestID, service, schema, table, sourceRunID, actor string) (*runv1.TriggerSingleNodeRunResponse, error) {
	return c.triggerSingleNode(ctx, singleNodeRequest(requestID, service, schema, table, sourceRunID, ""), actor)
}

func (c *runGRPCClient) TriggerNodeTest(ctx context.Context, requestID, service, schema, table, sourceRunID, actor string) (*runv1.TriggerSingleNodeRunResponse, error) {
	return c.triggerSingleNode(ctx, singleNodeRequest(requestID, service, schema, table, sourceRunID, "test"), actor)
}

func (c *runGRPCClient) TriggerNodeBuild(ctx context.Context, requestID, service, schema, table, sourceRunID, actor string) (*runv1.TriggerSingleNodeRunResponse, error) {
	return c.triggerSingleNode(ctx, singleNodeRequest(requestID, service, schema, table, sourceRunID, "build"), actor)
}

func (c *runGRPCClient) TriggerNodeFullRefresh(ctx context.Context, requestID, service, schema, table, sourceRunID, actor string) (*runv1.TriggerSingleNodeRunResponse, error) {
	return c.triggerSingleNode(ctx, singleNodeRequest(requestID, service, schema, table, sourceRunID, "full_refresh"), actor)
}

func (c *runGRPCClient) Close() error { return c.conn.Close() }
