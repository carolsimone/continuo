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
// runGRPCClient. A method that starts a run, or cancels a schedule's active
// run, takes the request id the server makes the command idempotent on.
type RunClient interface {
	StartScheduleRun(ctx context.Context, requestID, scheduleName, actor string) (*runv1.StartRunResponse, error)
	StartScheduleTest(ctx context.Context, requestID, scheduleName, actor string) (*runv1.StartRunResponse, error)
	StartScheduleBuild(ctx context.Context, requestID, scheduleName, actor string) (*runv1.StartRunResponse, error)
	CancelSchedule(ctx context.Context, requestID, scheduleName, reason, by string) (*runv1.CancelActiveRunResponse, error)
	// StartRerun re-executes what did not succeed in a failed or cancelled run.
	StartRerun(ctx context.Context, requestID, sourceRunID, actor string) (*runv1.StartRerunResponse, error)
	// PreviewRebase reports what StartRebase would run, creating nothing.
	PreviewRebase(ctx context.Context, sourceRunID string, includeChangedUpstream bool) (*runv1.PreviewRebaseResponse, error)
	// StartRebase restarts a failed or cancelled run on the live release;
	// expectedReleaseID is the release the caller previewed.
	StartRebase(ctx context.Context, requestID, sourceRunID, expectedReleaseID string, includeChangedUpstream bool, actor string) (*runv1.StartRebaseResponse, error)
	// CancelRun cancels a run by id. It takes no request id: cancelling a run
	// that is already cancelling or cancelled changes nothing.
	CancelRun(ctx context.Context, runID, reason, by string) (*runv1.RunResponse, error)
	StartNodeRun(ctx context.Context, requestID, project, schema, table, sourceRunID, actor string) (*runv1.StartSingleNodeRunResponse, error)
	StartNodeTest(ctx context.Context, requestID, project, schema, table, sourceRunID, actor string) (*runv1.StartSingleNodeRunResponse, error)
	StartNodeBuild(ctx context.Context, requestID, project, schema, table, sourceRunID, actor string) (*runv1.StartSingleNodeRunResponse, error)
	StartNodeFullRefresh(ctx context.Context, requestID, project, schema, table, sourceRunID, actor string, acknowledged []string) (*runv1.StartSingleNodeRunResponse, error)

	ListAllSchedules(ctx context.Context) (*runv1.ListSchedulesResponse, error)
	GetScheduleGraph(ctx context.Context, scheduleName string) (*runv1.GetScheduleGraphResponse, error)
	GetRun(ctx context.Context, runID string) (*runv1.RunResponse, error)
	GetRunGraph(ctx context.Context, runID string) (*runv1.GetRunGraphResponse, error)
	ListRuns(ctx context.Context, scheduleName string, pageSize, pageOffset int32) (*runv1.ListRunsResponse, error)
	ListTasks(ctx context.Context, runID, status string, pageSize, pageOffset int32) (*runv1.ListTasksResponse, error)
	ListRunAttempts(ctx context.Context, runID, taskID string, pageSize, pageOffset int32) (*runv1.ListRunAttemptsResponse, error)
	ListNodeRuns(ctx context.Context, project, schema, table, operation string, limit int32) (*runv1.ListNodeTasksResponse, error)
	// ListNodes lists the nodes of the live release. With dormantOnly it keeps
	// only the dormant ones, the nodes continuo builds that belong to no
	// schedule.
	ListNodes(ctx context.Context, search, project, operation string, dormantOnly bool, limit, offset int32) (*runv1.ListNodesResponse, error)

	// Schedule crons. An edit takes no request id, so the server cannot answer
	// a repeat with the result of the first call: each save raises the
	// schedule's version, and a repeat that sends the expected_version of a
	// call that has saved (its reply lost) is ABORTED. The client repeats an
	// edit on UNAVAILABLE only, never on ABORTED; a caller that gets an
	// ABORTED reads the schedule again before it saves. actor is the identity
	// the server records as the author of the edit.
	UpsertSchedule(ctx context.Context, actor string, req *runv1.UpsertScheduleRequest) (*runv1.UpsertScheduleResponse, error)
	DeleteSchedule(ctx context.Context, actor, scheduleName string) (*runv1.DeleteScheduleResponse, error)
	PreviewSchedule(ctx context.Context, cron, timezone string) (*runv1.PreviewScheduleResponse, error)

	Close() error
}

// retryDelays are the waits before the second and the third try of a command
// the server refused as UNAVAILABLE or ABORTED.
var retryDelays = []time.Duration{100 * time.Millisecond, 400 * time.Millisecond}

// commandMethods are the RPCs that start or cancel a run by request id; so not
// CancelRun, which names the run and takes none. The dialled client retries
// these on UNAVAILABLE and ABORTED: the server answers a repeated id with the
// run its first call made, so a retry cannot duplicate work. They are the only
// methods retried on ABORTED. The schedule edits are retried on UNAVAILABLE
// only (scheduleEditMethods), and a read and CancelRun are not retried.
var commandMethods = map[string]bool{
	runv1.RunService_StartRun_FullMethodName:           true,
	runv1.RunService_StartRerun_FullMethodName:         true,
	runv1.RunService_StartRebase_FullMethodName:        true,
	runv1.RunService_StartSingleNodeRun_FullMethodName: true,
	runv1.RunService_CancelActiveRun_FullMethodName:    true,
}

// scheduleEditMethods are the RPCs that change the cron of a schedule. They
// carry no request id and are retried on UNAVAILABLE only; their ABORTED is
// returned at once for the caller to read the schedule again. With an
// expected_version above 0 that ABORTED is a stale version, which sending the
// same request again cannot settle. With expected_version 0, as a first cron
// is sent, it means other edits saved first on each of the server's three
// tries, which sending the request again can settle.
var scheduleEditMethods = map[string]bool{
	runv1.RunService_UpsertSchedule_FullMethodName: true,
	runv1.RunService_DeleteSchedule_FullMethodName: true,
}

// retryCommandInterceptor repeats a command call that failed UNAVAILABLE or
// ABORTED (concurrent calls of its request id kept rolling back), up to
// len(retryDelays)+1 tries, sending the same request, and so the same request
// id, each time. A schedule edit is repeated on UNAVAILABLE only. A refusal
// for maintenance is UNAVAILABLE too but lasts until an operator ends it, so
// it is returned at once.
func retryCommandInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	shouldRetry := retryable
	switch {
	case commandMethods[method]:
	case scheduleEditMethods[method]:
		shouldRetry = retryableOutage
	default:
		return err
	}
	for _, delay := range retryDelays {
		if !shouldRetry(err) {
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

// retryable reports whether err is an ABORTED or a plain UNAVAILABLE, not a
// maintenance refusal.
func retryable(err error) bool {
	if err == nil || errors.Is(err, output.ErrMaintenance) {
		return false
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.Aborted:
		return true
	default:
		return false
	}
}

// retryableOutage reports whether err is a plain UNAVAILABLE, not a
// maintenance refusal and not an ABORTED.
func retryableOutage(err error) bool {
	return retryable(err) && status.Code(err) == codes.Unavailable
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

// userIDMetadataKey is the gRPC metadata header run-controller reads to
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

func (c *runGRPCClient) StartScheduleRun(ctx context.Context, requestID, scheduleName, actor string) (*runv1.StartRunResponse, error) {
	return c.startRun(ctx, requestID, scheduleName, "", actor)
}

func (c *runGRPCClient) StartScheduleTest(ctx context.Context, requestID, scheduleName, actor string) (*runv1.StartRunResponse, error) {
	return c.startRun(ctx, requestID, scheduleName, "test", actor)
}

func (c *runGRPCClient) StartScheduleBuild(ctx context.Context, requestID, scheduleName, actor string) (*runv1.StartRunResponse, error) {
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

func (c *runGRPCClient) StartRerun(ctx context.Context, requestID, sourceRunID, actor string) (*runv1.StartRerunResponse, error) {
	return c.api.StartRerun(withActor(ctx, actor), &runv1.StartRerunRequest{
		RequestId:   requestID,
		SourceRunId: sourceRunID,
	})
}

func (c *runGRPCClient) PreviewRebase(ctx context.Context, sourceRunID string, includeChangedUpstream bool) (*runv1.PreviewRebaseResponse, error) {
	return c.api.PreviewRebase(ctx, &runv1.PreviewRebaseRequest{
		SourceRunId:            sourceRunID,
		IncludeChangedUpstream: includeChangedUpstream,
	})
}

func (c *runGRPCClient) StartRebase(ctx context.Context, requestID, sourceRunID, expectedReleaseID string, includeChangedUpstream bool, actor string) (*runv1.StartRebaseResponse, error) {
	return c.api.StartRebase(withActor(ctx, actor), &runv1.StartRebaseRequest{
		RequestId:              requestID,
		SourceRunId:            sourceRunID,
		IncludeChangedUpstream: includeChangedUpstream,
		ExpectedReleaseId:      expectedReleaseID,
	})
}

func (c *runGRPCClient) CancelRun(ctx context.Context, runID, reason, by string) (*runv1.RunResponse, error) {
	return c.api.CancelRun(ctx, &runv1.CancelRunRequest{
		RunId:              runID,
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

func (c *runGRPCClient) GetRun(ctx context.Context, runID string) (*runv1.RunResponse, error) {
	return c.api.GetRun(ctx, &runv1.GetRunRequest{RunId: runID})
}

func (c *runGRPCClient) GetRunGraph(ctx context.Context, runID string) (*runv1.GetRunGraphResponse, error) {
	return c.api.GetRunGraph(ctx, &runv1.GetRunGraphRequest{RunId: runID})
}

func (c *runGRPCClient) ListRuns(ctx context.Context, scheduleName string, pageSize, pageOffset int32) (*runv1.ListRunsResponse, error) {
	return c.api.ListRuns(ctx, &runv1.ListRunsRequest{
		ScheduleName: scheduleName,
		PageSize:     pageSize,
		PageOffset:   pageOffset,
	})
}

func (c *runGRPCClient) ListRunAttempts(ctx context.Context, runID, taskID string, pageSize, pageOffset int32) (*runv1.ListRunAttemptsResponse, error) {
	return c.api.ListRunAttempts(ctx, &runv1.ListRunAttemptsRequest{
		RunId:      runID,
		TaskId:     taskID,
		PageSize:   pageSize,
		PageOffset: pageOffset,
	})
}

func (c *runGRPCClient) ListNodes(ctx context.Context, search, project, operation string, dormantOnly bool, limit, offset int32) (*runv1.ListNodesResponse, error) {
	return c.api.ListNodes(ctx, &runv1.ListNodesRequest{
		Search:      search,
		ProjectName: project,
		Operation:   operation,
		DormantOnly: dormantOnly,
		Limit:       limit,
		Offset:      offset,
	})
}

func (c *runGRPCClient) ListNodeRuns(ctx context.Context, project, schema, table, operation string, limit int32) (*runv1.ListNodeTasksResponse, error) {
	return c.api.ListNodeTasks(ctx, &runv1.ListNodeTasksRequest{
		ProjectName: project,
		SchemaName:  schema,
		TableName:   table,
		Operation:   operation,
		Limit:       limit,
	})
}

// singleNodeRequest builds a StartSingleNodeRun request. An empty
// sourceRunID selects the "latest" metadata mode; a non-empty one selects
// "snapshot_of_run", pinning the run to the metadata the given past run used.
// run-controller rejects any other combination, so the CLI never sends one.
func singleNodeRequest(requestID, project, schema, table, sourceRunID, operation string) *runv1.StartSingleNodeRunRequest {
	req := &runv1.StartSingleNodeRunRequest{
		RequestId:      requestID,
		ProjectName:    project,
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

// metadata_source values accepted by StartSingleNodeRun. They mirror the
// run-controller component's run.MetadataSource* constants, which the CLI cannot import
// (public-gRPC-only rule), so the literals are duplicated here deliberately.
const (
	metadataSourceLatest        = "latest"
	metadataSourceSnapshotOfRun = "snapshot_of_run"
)

func (c *runGRPCClient) startSingleNode(ctx context.Context, req *runv1.StartSingleNodeRunRequest, actor string) (*runv1.StartSingleNodeRunResponse, error) {
	return c.api.StartSingleNodeRun(withActor(ctx, actor), req)
}

func (c *runGRPCClient) StartNodeRun(ctx context.Context, requestID, project, schema, table, sourceRunID, actor string) (*runv1.StartSingleNodeRunResponse, error) {
	return c.startSingleNode(ctx, singleNodeRequest(requestID, project, schema, table, sourceRunID, ""), actor)
}

func (c *runGRPCClient) StartNodeTest(ctx context.Context, requestID, project, schema, table, sourceRunID, actor string) (*runv1.StartSingleNodeRunResponse, error) {
	return c.startSingleNode(ctx, singleNodeRequest(requestID, project, schema, table, sourceRunID, "test"), actor)
}

func (c *runGRPCClient) StartNodeBuild(ctx context.Context, requestID, project, schema, table, sourceRunID, actor string) (*runv1.StartSingleNodeRunResponse, error) {
	return c.startSingleNode(ctx, singleNodeRequest(requestID, project, schema, table, sourceRunID, "build"), actor)
}

func (c *runGRPCClient) StartNodeFullRefresh(ctx context.Context, requestID, project, schema, table, sourceRunID, actor string, acknowledged []string) (*runv1.StartSingleNodeRunResponse, error) {
	req := singleNodeRequest(requestID, project, schema, table, sourceRunID, "full_refresh")
	req.AcknowledgedDependents = acknowledged
	return c.startSingleNode(ctx, req, actor)
}

func (c *runGRPCClient) UpsertSchedule(ctx context.Context, actor string, req *runv1.UpsertScheduleRequest) (*runv1.UpsertScheduleResponse, error) {
	return c.api.UpsertSchedule(withActor(ctx, actor), req)
}

func (c *runGRPCClient) DeleteSchedule(ctx context.Context, actor, scheduleName string) (*runv1.DeleteScheduleResponse, error) {
	return c.api.DeleteSchedule(withActor(ctx, actor), &runv1.DeleteScheduleRequest{ScheduleName: scheduleName})
}

func (c *runGRPCClient) PreviewSchedule(ctx context.Context, cron, timezone string) (*runv1.PreviewScheduleResponse, error) {
	return c.api.PreviewSchedule(ctx, &runv1.PreviewScheduleRequest{Cron: cron, Timezone: timezone})
}

func (c *runGRPCClient) Close() error { return c.conn.Close() }
