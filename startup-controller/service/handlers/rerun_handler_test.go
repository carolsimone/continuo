package handlers_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	graphv1 "github.com/carolsimone/continuo/graph/api/graph/v1"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	grpcadapter "github.com/carolsimone/continuo/startup-controller/adapters/grpc"
	"github.com/carolsimone/continuo/startup-controller/domain/command"
	"github.com/carolsimone/continuo/startup-controller/domain/event"
	"github.com/carolsimone/continuo/startup-controller/domain/model"
	"github.com/carolsimone/continuo/startup-controller/service/handlers"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── fakes ─────────────────────────────────────────────────────────────────────

type fakeRerunStateClient struct {
	updateInitStatusFn func(ctx context.Context, scheduleID uuid.UUID, initStatus string) error
	getTaskFn          func(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*statev1.Task, error)
	resetTaskFn        func(ctx context.Context, taskID uuid.UUID) error

	updateInitStatusCalls []string
	resetTaskCalls        []uuid.UUID
}

func (f *fakeRerunStateClient) UpdateSchedulerInitStatus(ctx context.Context, scheduleID uuid.UUID, initStatus string) error {
	f.updateInitStatusCalls = append(f.updateInitStatusCalls, initStatus)
	if f.updateInitStatusFn != nil {
		return f.updateInitStatusFn(ctx, scheduleID, initStatus)
	}
	return nil
}

func (f *fakeRerunStateClient) GetTask(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*statev1.Task, error) {
	if f.getTaskFn != nil {
		return f.getTaskFn(ctx, scheduleID, serviceName, schemaName, tableName)
	}
	return &statev1.Task{TaskId: uuid.New().String(), Status: statev1.TaskStatus_TASK_STATUS_FAILED}, nil
}

func (f *fakeRerunStateClient) ResetTask(ctx context.Context, taskID uuid.UUID) error {
	f.resetTaskCalls = append(f.resetTaskCalls, taskID)
	if f.resetTaskFn != nil {
		return f.resetTaskFn(ctx, taskID)
	}
	return nil
}

type fakeRerunGraphClient struct {
	downstream             []*graphv1.TableNode
	updateStatusCalls      []string // "schema.table:status"
	getScheduleInitNodesFn func(ctx context.Context, scheduleName, runID string) ([]model.NodeInfo, []model.NodeInfo, []model.NodeInfo, error)
}

func (f *fakeRerunGraphClient) UpdateNodeStatus(_ context.Context, _, schemaName, tableName, status, runID string) error {
	f.updateStatusCalls = append(f.updateStatusCalls, schemaName+"."+tableName+":"+status)
	return nil
}

func (f *fakeRerunGraphClient) GetTransitiveDownstream(_ context.Context, _, _, _ string) ([]*graphv1.TableNode, error) {
	return f.downstream, nil
}

func (f *fakeRerunGraphClient) GetScheduleInitNodes(ctx context.Context, scheduleName, runID string) ([]model.NodeInfo, []model.NodeInfo, []model.NodeInfo, error) {
	if f.getScheduleInitNodesFn != nil {
		return f.getScheduleInitNodesFn(ctx, scheduleName, runID)
	}
	// Default: return node with service_name matching cmd ("svc") so existing tests pass
	return []model.NodeInfo{
		{Schema: "s", TableName: "A", ServiceName: "svc", NodeType: pkg_model.NodeTypeDbtModel},
	}, nil, nil, nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newRerunCmd() command.RerunNode {
	return command.RerunNode{
		ScheduleID:   uuid.New(),
		ScheduleName: "test-schedule",
		Schema:       "s",
		TableName:    "A",
		ServiceName:  "svc",
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestRerunHandler_SkipsWhenAlreadyCompleted(t *testing.T) {
	stateClient := &fakeRerunStateClient{
		updateInitStatusFn: func(_ context.Context, _ uuid.UUID, _ string) error {
			return grpcadapter.ErrAlreadyCompleted
		},
	}
	graphClient := &fakeRerunGraphClient{}
	h := handlers.NewRerunHandler(newMockStartupUoW(), stateClient, graphClient, newStartupLogger())

	err := h.Handle(context.Background(), newRerunCmd())
	require.NoError(t, err)
	assert.Empty(t, graphClient.updateStatusCalls)
}

func TestRerunHandler_SkipsWhenErrAlreadyCompletedWrapped(t *testing.T) {
	stateClient := &fakeRerunStateClient{
		updateInitStatusFn: func(_ context.Context, _ uuid.UUID, _ string) error {
			return fmt.Errorf("wrapper: %w", grpcadapter.ErrAlreadyCompleted)
		},
	}
	graphClient := &fakeRerunGraphClient{}
	h := handlers.NewRerunHandler(newMockStartupUoW(), stateClient, graphClient, newStartupLogger())

	err := h.Handle(context.Background(), newRerunCmd())
	require.NoError(t, err)
	assert.Empty(t, graphClient.updateStatusCalls)
}

func TestRerunHandler_ResetsOnlyFAILEDGraphNodes(t *testing.T) {
	stateClient := &fakeRerunStateClient{
		getTaskFn: func(_ context.Context, _ uuid.UUID, _, _, _ string) (*statev1.Task, error) {
			return &statev1.Task{TaskId: uuid.New().String()}, nil
		},
	}
	graphClient := &fakeRerunGraphClient{
		downstream: []*graphv1.TableNode{
			{SchemaName: "s", TableName: "B", ServiceName: "svc", Status: "FAILED"},
			{SchemaName: "s", TableName: "C", ServiceName: "svc", Status: ""},
		},
	}
	h := handlers.NewRerunHandler(newMockStartupUoW(), stateClient, graphClient, newStartupLogger())

	err := h.Handle(context.Background(), newRerunCmd())
	require.NoError(t, err)

	// target A always reset
	assert.Contains(t, graphClient.updateStatusCalls, "s.A:PENDING")
	// FAILED node B reset
	assert.Contains(t, graphClient.updateStatusCalls, "s.B:PENDING")
	// blank-status node C NOT reset
	assert.NotContains(t, graphClient.updateStatusCalls, "s.C:PENDING")
}

func TestRerunHandler_ResetsTargetAndFAILEDDownstreamTaskTrackers(t *testing.T) {
	stateClient := &fakeRerunStateClient{
		getTaskFn: func(_ context.Context, _ uuid.UUID, _, _, _ string) (*statev1.Task, error) {
			return &statev1.Task{TaskId: uuid.New().String(), Status: statev1.TaskStatus_TASK_STATUS_FAILED}, nil
		},
	}
	graphClient := &fakeRerunGraphClient{
		downstream: []*graphv1.TableNode{
			{SchemaName: "s", TableName: "B", ServiceName: "svc", Status: "FAILED"},
			{SchemaName: "s", TableName: "C", ServiceName: "svc", Status: ""},
		},
	}
	h := handlers.NewRerunHandler(newMockStartupUoW(), stateClient, graphClient, newStartupLogger())

	err := h.Handle(context.Background(), newRerunCmd())
	require.NoError(t, err)

	// ResetTask called twice: target A + FAILED downstream B (blank-status C is skipped)
	assert.Len(t, stateClient.resetTaskCalls, 2)
}

func TestRerunHandler_DispatchesOnlyTargetNode(t *testing.T) {
	stateClient := &fakeRerunStateClient{}
	graphClient := &fakeRerunGraphClient{
		downstream: []*graphv1.TableNode{
			{SchemaName: "s", TableName: "B", ServiceName: "svc", Status: "FAILED"},
		},
	}
	uow := newMockStartupUoW()
	h := handlers.NewRerunHandler(uow, stateClient, graphClient, newStartupLogger())

	cmd := newRerunCmd()
	err := h.Handle(context.Background(), cmd)
	require.NoError(t, err)

	require.Len(t, uow.outboxRepo.CreatedEntries, 1, "exactly one outbox entry for target")
	var evt event.NodeReadyForExecution
	require.NoError(t, json.Unmarshal(uow.outboxRepo.CreatedEntries[0].Payload, &evt))
	assert.Equal(t, cmd.TableName, evt.TableName)
	assert.Equal(t, cmd.ScheduleName, evt.ScheduleName)
}

func TestRerunHandler_SetsInitStatusCompletedAfterGraphReset(t *testing.T) {
	stateClient := &fakeRerunStateClient{}
	graphClient := &fakeRerunGraphClient{}
	h := handlers.NewRerunHandler(newMockStartupUoW(), stateClient, graphClient, newStartupLogger())

	err := h.Handle(context.Background(), newRerunCmd())
	require.NoError(t, err)

	require.Len(t, stateClient.updateInitStatusCalls, 2)
	assert.Equal(t, "in_progress", stateClient.updateInitStatusCalls[0])
	assert.Equal(t, "completed", stateClient.updateInitStatusCalls[1])

	// "completed" must come after graph resets (at least target node was reset)
	assert.NotEmpty(t, graphClient.updateStatusCalls)
}

func TestRerunHandler_EmptyDownstream_DispatchesTarget(t *testing.T) {
	stateClient := &fakeRerunStateClient{}
	graphClient := &fakeRerunGraphClient{
		downstream: []*graphv1.TableNode{},
	}
	uow := newMockStartupUoW()
	h := handlers.NewRerunHandler(uow, stateClient, graphClient, newStartupLogger())

	cmd := newRerunCmd()
	err := h.Handle(context.Background(), cmd)
	require.NoError(t, err)

	require.Len(t, uow.outboxRepo.CreatedEntries, 1)
	var evt event.NodeReadyForExecution
	require.NoError(t, json.Unmarshal(uow.outboxRepo.CreatedEntries[0].Payload, &evt))
	assert.Equal(t, cmd.TableName, evt.TableName)
	assert.Len(t, stateClient.resetTaskCalls, 1, "ResetTask called for target even with no downstream")
}

func TestRerunHandler_ResetsTargetTask(t *testing.T) {
	// Target has exhausted its retries (retry_count == max_retries).
	// ResetTask must be called so the re-run starts with retry_count=0.
	targetID := uuid.New()
	stateClient := &fakeRerunStateClient{
		getTaskFn: func(_ context.Context, _ uuid.UUID, _, _, _ string) (*statev1.Task, error) {
			return &statev1.Task{
				TaskId:     targetID.String(),
				Status:     statev1.TaskStatus_TASK_STATUS_FAILED,
				RetryCount: 3,
				MaxRetries: 3,
			}, nil
		},
	}
	graphClient := &fakeRerunGraphClient{
		downstream: []*graphv1.TableNode{},
	}
	h := handlers.NewRerunHandler(newMockStartupUoW(), stateClient, graphClient, newStartupLogger())

	err := h.Handle(context.Background(), newRerunCmd())
	require.NoError(t, err)

	require.Len(t, stateClient.resetTaskCalls, 1)
	assert.Equal(t, targetID, stateClient.resetTaskCalls[0])
}

func TestRerunHandler_DispatchUsesGraphServiceNameNotCmdServiceName(t *testing.T) {
	// Graph node for "A" has service_name="new-svc" (updated by fix step),
	// but cmd.ServiceName is still "old-svc" (original task_tracker value).
	// The dispatch event must use "new-svc" (from graph), not "old-svc" (from cmd).
	stateClient := &fakeRerunStateClient{
		getTaskFn: func(_ context.Context, _ uuid.UUID, serviceName, _, _ string) (*statev1.Task, error) {
			// GetTask is called with cmd.ServiceName = "old-svc"
			if serviceName != "old-svc" {
				return nil, fmt.Errorf("unexpected serviceName in GetTask: %s", serviceName)
			}
			return &statev1.Task{TaskId: uuid.New().String()}, nil
		},
	}
	graphClient := &fakeRerunGraphClient{
		// GetScheduleInitNodes returns the graph's current service_name for node "A"
		getScheduleInitNodesFn: func(_ context.Context, _, _ string) ([]model.NodeInfo, []model.NodeInfo, []model.NodeInfo, error) {
			return []model.NodeInfo{
				{Schema: "s", TableName: "A", ServiceName: "new-svc", NodeType: pkg_model.NodeTypeDbtModel},
			}, nil, nil, nil
		},
	}
	uow := newMockStartupUoW()
	h := handlers.NewRerunHandler(uow, stateClient, graphClient, newStartupLogger())

	cmd := command.RerunNode{
		ScheduleID:   uuid.New(),
		ScheduleName: "test-schedule",
		Schema:       "s",
		TableName:    "A",
		ServiceName:  "old-svc", // stale value, from task_tracker
	}
	err := h.Handle(context.Background(), cmd)
	require.NoError(t, err)

	require.Len(t, uow.outboxRepo.CreatedEntries, 1)
	var evt event.NodeReadyForExecution
	require.NoError(t, json.Unmarshal(uow.outboxRepo.CreatedEntries[0].Payload, &evt))
	assert.Equal(t, "new-svc", evt.ServiceName,
		"dispatch must use graph's current service_name, not stale cmd.ServiceName")
}
