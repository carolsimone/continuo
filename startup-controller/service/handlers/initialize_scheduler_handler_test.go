package handlers_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	grpcadapter "github.com/carolsimone/continuo/startup-controller/adapters/grpc"
	"github.com/carolsimone/continuo/startup-controller/adapters/postgres"
	"github.com/carolsimone/continuo/startup-controller/domain/command"
	"github.com/carolsimone/continuo/startup-controller/domain/event"
	"github.com/carolsimone/continuo/startup-controller/domain/model"
	"github.com/carolsimone/continuo/startup-controller/service/handlers"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── mocks ────────────────────────────────────────────────────────────────────

type mockInitStateClient struct {
	updateSchedulerInitStatusFn func(ctx context.Context, scheduleID uuid.UUID, initStatus string) error
	getTaskFn                   func(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*statev1.Task, error)
	createTaskFn                func(ctx context.Context, taskID, scheduleID uuid.UUID, serviceName, schemaName, tableName string, maxRetries int) error
	updateTaskStatusFn          func(ctx context.Context, taskID uuid.UUID, taskStatus statev1.TaskStatus) error

	// call-order tracking
	callLog         []string
	createTaskCalls []string

	// in-memory task store for stateful GetTask/CreateTask behaviour
	tasks map[string]*statev1.Task
}

func (m *mockInitStateClient) UpdateSchedulerInitStatus(ctx context.Context, scheduleID uuid.UUID, initStatus string) error {
	m.callLog = append(m.callLog, "init:"+initStatus)
	if m.updateSchedulerInitStatusFn != nil {
		return m.updateSchedulerInitStatusFn(ctx, scheduleID, initStatus)
	}
	return nil
}

func (m *mockInitStateClient) GetTask(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*statev1.Task, error) {
	if m.getTaskFn != nil {
		return m.getTaskFn(ctx, scheduleID, serviceName, schemaName, tableName)
	}
	// Stateful default: return task if previously created, otherwise not found.
	key := scheduleID.String() + "/" + serviceName + "/" + schemaName + "/" + tableName
	if m.tasks != nil {
		if t, ok := m.tasks[key]; ok {
			return t, nil
		}
	}
	return nil, grpcadapter.ErrTaskNotFound
}

func (m *mockInitStateClient) CreateTask(ctx context.Context, taskID, scheduleID uuid.UUID, serviceName, schemaName, tableName string, maxRetries int) error {
	m.createTaskCalls = append(m.createTaskCalls, tableName)
	if m.createTaskFn != nil {
		return m.createTaskFn(ctx, taskID, scheduleID, serviceName, schemaName, tableName, maxRetries)
	}
	// Store the created task so subsequent GetTask calls find it.
	if m.tasks == nil {
		m.tasks = make(map[string]*statev1.Task)
	}
	key := scheduleID.String() + "/" + serviceName + "/" + schemaName + "/" + tableName
	m.tasks[key] = &statev1.Task{
		TaskId: taskID.String(),
		Status: statev1.TaskStatus_TASK_STATUS_PENDING,
	}
	return nil
}

func (m *mockInitStateClient) UpdateTaskStatus(ctx context.Context, taskID uuid.UUID, taskStatus statev1.TaskStatus) error {
	if m.updateTaskStatusFn != nil {
		return m.updateTaskStatusFn(ctx, taskID, taskStatus)
	}
	return nil
}

type mockInitGraphClient struct {
	allNodes      []model.NodeInfo
	rootNodes     []model.NodeInfo
	seedNodes     []model.NodeInfo
	snapshotCalls []string
}

func (m *mockInitGraphClient) SnapshotGraph(ctx context.Context, runID, scheduleName string) error {
	m.snapshotCalls = append(m.snapshotCalls, runID+":"+scheduleName)
	return nil
}

func (m *mockInitGraphClient) GetScheduleInitNodes(ctx context.Context, scheduleName, runID string) ([]model.NodeInfo, []model.NodeInfo, []model.NodeInfo, error) {
	return m.allNodes, m.rootNodes, m.seedNodes, nil
}

type mockStartupOutboxRepo struct {
	CreatedEntries []*model.OutboxEntry
}

func (m *mockStartupOutboxRepo) Create(ctx context.Context, entry *model.OutboxEntry) error {
	m.CreatedEntries = append(m.CreatedEntries, entry)
	return nil
}
func (m *mockStartupOutboxRepo) GetPendingBatch(ctx context.Context, limit int) ([]*model.OutboxEntry, error) {
	return nil, nil
}
func (m *mockStartupOutboxRepo) MarkProcessed(ctx context.Context, id uuid.UUID) error { return nil }
func (m *mockStartupOutboxRepo) MarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error {
	return nil
}
func (m *mockStartupOutboxRepo) IncrementRetry(ctx context.Context, id uuid.UUID) error { return nil }

type mockStartupUoW struct {
	outboxRepo *mockStartupOutboxRepo
}

func newMockStartupUoW() *mockStartupUoW {
	return &mockStartupUoW{outboxRepo: &mockStartupOutboxRepo{}}
}

func (m *mockStartupUoW) OutboxRepo() postgres.OutboxRepository { return m.outboxRepo }
func (m *mockStartupUoW) Begin(ctx context.Context) error       { return nil }
func (m *mockStartupUoW) Commit() error                         { return nil }
func (m *mockStartupUoW) Rollback() error                       { return nil }

func newStartupLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// ── tests ─────────────────────────────────────────────────────────────────────

// Handler must call UpdateSchedulerInitStatus("completed") as the last state-client call,
// and must NOT set scheduler status at all (state service infers running from init completion).
func TestHandle_InitCompletedIsLastStateCall(t *testing.T) {
	stateClient := &mockInitStateClient{}
	uow := newMockStartupUoW()
	rootNode := model.NodeInfo{ServiceName: "warehouse", Schema: "public", TableName: "orders", NodeType: pkg_model.NodeTypeDbtModel}
	graphClient := &mockInitGraphClient{
		rootNodes: []model.NodeInfo{rootNode},
		allNodes:  []model.NodeInfo{rootNode},
	}

	h := handlers.NewInitializeSchedulerHandler(uow, graphClient, stateClient, newStartupLogger())

	cmd := command.InitializeScheduler{RunnerID: uuid.New(), ScheduleName: "daily"}
	err := h.Handle(context.Background(), cmd)

	require.NoError(t, err)
	require.Equal(t, []string{cmd.RunnerID.String() + ":daily"}, graphClient.snapshotCalls)

	// "init:completed" must be the last entry in the call log
	require.NotEmpty(t, stateClient.callLog)
	assert.Equal(t, "init:completed", stateClient.callLog[len(stateClient.callLog)-1],
		"UpdateSchedulerInitStatus('completed') must be the last state-client call")

	// No scheduler status writes must appear in the log
	for _, entry := range stateClient.callLog {
		assert.NotContains(t, entry, "status:", "scheduler status must never be written by startup-controller")
	}

	// Verify NodeType in event payload
	require.Len(t, uow.outboxRepo.CreatedEntries, 1)
	var evt event.NodeReadyForExecution
	require.NoError(t, json.Unmarshal(uow.outboxRepo.CreatedEntries[0].Payload, &evt))
	assert.Equal(t, "dbt-model", evt.NodeType)
}

// Even with no root nodes, init:completed must be called and no status writes must occur.
func TestHandle_InitCompleted_EvenWithNoRootNodes(t *testing.T) {
	stateClient := &mockInitStateClient{}
	graphClient := &mockInitGraphClient{rootNodes: []model.NodeInfo{}, allNodes: []model.NodeInfo{}}

	h := handlers.NewInitializeSchedulerHandler(newMockStartupUoW(), graphClient, stateClient, newStartupLogger())

	cmd := command.InitializeScheduler{RunnerID: uuid.New(), ScheduleName: "hourly"}
	err := h.Handle(context.Background(), cmd)

	require.NoError(t, err)
	require.Equal(t, []string{cmd.RunnerID.String() + ":hourly"}, graphClient.snapshotCalls)
	assert.Contains(t, stateClient.callLog, "init:completed")
	for _, entry := range stateClient.callLog {
		assert.NotContains(t, entry, "status:", "scheduler status must never be written by startup-controller")
	}
}

// When upstream seeds exist, only seed nodes are dispatched (not model root nodes).
// Model root nodes will be dispatched by dependency-controller after seeds complete.
func TestHandle_WithUpstreamSeeds_DispatchesSeedsOnly(t *testing.T) {
	stateClient := &mockInitStateClient{}
	uow := newMockStartupUoW()

	seedNode := model.NodeInfo{
		ServiceName: "service-1",
		Schema:      "e2e_schema",
		TableName:   "seed_table_1",
		NodeType:    pkg_model.NodeTypeDbtSeed,
	}
	modelRootNode := model.NodeInfo{
		ServiceName: "service-1",
		Schema:      "e2e_schema",
		TableName:   "table_a",
		NodeType:    pkg_model.NodeTypeDbtModel,
	}

	graphClient := &mockInitGraphClient{
		rootNodes: []model.NodeInfo{modelRootNode},
		seedNodes: []model.NodeInfo{seedNode},
		allNodes:  []model.NodeInfo{seedNode, modelRootNode},
	}

	h := handlers.NewInitializeSchedulerHandler(uow, graphClient, stateClient, newStartupLogger())

	cmd := command.InitializeScheduler{RunnerID: uuid.New(), ScheduleName: "e2e-schedule"}
	err := h.Handle(context.Background(), cmd)

	require.NoError(t, err)

	// Only the seed node is in the outbox — model root node is NOT dispatched yet
	require.Len(t, uow.outboxRepo.CreatedEntries, 1, "Expected exactly 1 outbox entry (seed only)")

	var evt event.NodeReadyForExecution
	require.NoError(t, json.Unmarshal(uow.outboxRepo.CreatedEntries[0].Payload, &evt))
	assert.Equal(t, "seed_table_1", evt.TableName)
	assert.Equal(t, "dbt-seed", evt.NodeType)
	assert.Equal(t, "e2e-schedule", evt.ScheduleName, "Seed event must carry triggering schedule name")
}

// When no upstream seeds exist, model root nodes are dispatched (backward-compatible path).
func TestHandle_NoUpstreamSeeds_DispatchesModelRootNodes(t *testing.T) {
	stateClient := &mockInitStateClient{}
	uow := newMockStartupUoW()

	modelRootNode := model.NodeInfo{
		ServiceName: "warehouse",
		Schema:      "public",
		TableName:   "orders",
		NodeType:    pkg_model.NodeTypeDbtModel,
	}

	graphClient := &mockInitGraphClient{
		rootNodes: []model.NodeInfo{modelRootNode},
		seedNodes: []model.NodeInfo{}, // no seeds
		allNodes:  []model.NodeInfo{modelRootNode},
	}

	h := handlers.NewInitializeSchedulerHandler(uow, graphClient, stateClient, newStartupLogger())

	cmd := command.InitializeScheduler{RunnerID: uuid.New(), ScheduleName: "daily"}
	err := h.Handle(context.Background(), cmd)

	require.NoError(t, err)

	require.Len(t, uow.outboxRepo.CreatedEntries, 1, "Expected 1 outbox entry (model root node)")
	var evt event.NodeReadyForExecution
	require.NoError(t, json.Unmarshal(uow.outboxRepo.CreatedEntries[0].Payload, &evt))
	assert.Equal(t, "orders", evt.TableName)
	assert.Equal(t, "dbt-model", evt.NodeType)
}

// Pre-registration loop must call CreateTask for every node (model + seed)
// before any NodeReadyForExecution events are written to the outbox.
func TestHandle_PreRegistersAllNodes(t *testing.T) {
	stateClient := &mockInitStateClient{}
	uow := newMockStartupUoW()

	modelNode1 := model.NodeInfo{
		ServiceName: "service-1", Schema: "e2e_schema", TableName: "table_a",
		NodeType: pkg_model.NodeTypeDbtModel,
	}
	modelNode2 := model.NodeInfo{
		ServiceName: "service-1", Schema: "e2e_schema", TableName: "table_b",
		NodeType: pkg_model.NodeTypeDbtModel,
	}
	seedNode1 := model.NodeInfo{
		ServiceName: "service-1", Schema: "e2e_schema", TableName: "seed_table_1",
		NodeType: pkg_model.NodeTypeDbtSeed,
	}
	seedNode2 := model.NodeInfo{
		ServiceName: "service-1", Schema: "e2e_schema", TableName: "seed_table_2",
		NodeType: pkg_model.NodeTypeDbtSeed,
	}

	graphClient := &mockInitGraphClient{
		allNodes:  []model.NodeInfo{modelNode1, modelNode2, seedNode1, seedNode2},
		rootNodes: []model.NodeInfo{modelNode1, modelNode2},
		seedNodes: []model.NodeInfo{seedNode1, seedNode2},
	}

	h := handlers.NewInitializeSchedulerHandler(uow, graphClient, stateClient, newStartupLogger())

	cmd := command.InitializeScheduler{RunnerID: uuid.New(), ScheduleName: "e2e-schedule"}
	err := h.Handle(context.Background(), cmd)

	require.NoError(t, err)

	// All 4 nodes must be pre-registered (2 model + 2 seed)
	assert.Len(t, stateClient.createTaskCalls, 4,
		"expected CreateTask called once per node during pre-registration")
	assert.ElementsMatch(t,
		[]string{"table_a", "table_b", "seed_table_1", "seed_table_2"},
		stateClient.createTaskCalls,
	)

	// Only seeds are dispatched (2 outbox entries)
	assert.Len(t, uow.outboxRepo.CreatedEntries, 2,
		"expected outbox entries only for dispatched seeds")
}
