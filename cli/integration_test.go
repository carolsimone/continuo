//go:build integration

package cli_test

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	orchestratorv1 "github.com/carolsimone/continuo/cli/proto/orchestrator/v1"
	statev1 "github.com/carolsimone/continuo/cli/proto/state/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// ---------------------------------------------------------------------------
// Stub servers
// ---------------------------------------------------------------------------

type stubStateServer struct {
	statev1.UnimplementedStateServiceServer
	triggerResp *statev1.TriggerScheduleResponse
	listResp    *statev1.ListAllSchedulesResponse
}

func (s *stubStateServer) TriggerSchedule(_ context.Context, _ *statev1.TriggerScheduleRequest) (*statev1.TriggerScheduleResponse, error) {
	return s.triggerResp, nil
}

func (s *stubStateServer) ListAllSchedules(_ context.Context, _ *statev1.ListAllSchedulesRequest) (*statev1.ListAllSchedulesResponse, error) {
	return s.listResp, nil
}

type stubOrchestratorServer struct {
	orchestratorv1.UnimplementedOrchestratorQueryServer
	resp *orchestratorv1.GetScheduleGraphResponse
}

func (s *stubOrchestratorServer) GetScheduleGraph(_ context.Context, _ *orchestratorv1.GetScheduleGraphRequest) (*orchestratorv1.GetScheduleGraphResponse, error) {
	return s.resp, nil
}

// ---------------------------------------------------------------------------
// Helper: build binary once per test (reuses TempDir)
// ---------------------------------------------------------------------------

func buildBinary(t *testing.T) string {
	t.Helper()
	binPath := filepath.Join(t.TempDir(), "continuo")
	cmd := exec.Command("go", "build", "-o", binPath, "./cmd/continuo")
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Run())
	return binPath
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestCLI_TriggerScheduleAgainstInProcessServer(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	endpoint := lis.Addr().String()

	grpcSrv := grpc.NewServer()
	statev1.RegisterStateServiceServer(grpcSrv, &stubStateServer{
		triggerResp: &statev1.TriggerScheduleResponse{ScheduleId: "sched_integ_1"},
		listResp:    nil,
	})
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.GracefulStop()

	binPath := buildBinary(t)

	out, err := exec.Command(binPath, "--endpoint", endpoint, "schedule", "trigger", "daily_ingest").Output()
	require.NoError(t, err)

	var payload map[string]string
	require.NoError(t, json.Unmarshal(out, &payload))
	assert.Equal(t, "sched_integ_1", payload["schedule_id"])
	assert.Equal(t, "daily_ingest", payload["schedule_name"])
	assert.NotEmpty(t, payload["triggered_at"])
}

func TestCLI_ScheduleListAgainstInProcessServer(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	endpoint := lis.Addr().String()

	grpcSrv := grpc.NewServer()
	statev1.RegisterStateServiceServer(grpcSrv, &stubStateServer{
		triggerResp: nil,
		listResp: &statev1.ListAllSchedulesResponse{
			Schedules: []*statev1.ScheduleSummary{
				{
					ScheduleName:   "daily_ingest",
					CronExpression: "0 2 * * *",
					Timezone:       "UTC",
					IsRunning:      false,
					LastRunStatus:  "success",
					LastRunId:      "run_001",
				},
			},
		},
	})
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.GracefulStop()

	binPath := buildBinary(t)

	out, err := exec.Command(binPath, "--endpoint", endpoint, "schedule", "list").Output()
	require.NoError(t, err)

	var payload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out, &payload))
	_, ok := payload["schedules"]
	assert.True(t, ok, "response must contain 'schedules' key")

	var schedules []map[string]interface{}
	require.NoError(t, json.Unmarshal(payload["schedules"], &schedules))
	assert.Len(t, schedules, 1, "expected one schedule entry")
	assert.Equal(t, "daily_ingest", schedules[0]["schedule_name"])
}

func TestCLI_ScheduleGraphAgainstInProcessServer(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	endpoint := lis.Addr().String()

	grpcSrv := grpc.NewServer()
	orchestratorv1.RegisterOrchestratorQueryServer(grpcSrv, &stubOrchestratorServer{
		resp: &orchestratorv1.GetScheduleGraphResponse{
			Nodes: []*orchestratorv1.TableNode{
				{
					TableName:   "orders",
					SchemaName:  "public",
					ServiceName: "analytics",
					NodeType:    "model",
					Status:      "healthy",
				},
			},
			Edges: []*orchestratorv1.GraphEdge{
				{
					FromNodeId: "node_a",
					ToNodeId:   "node_b",
				},
			},
		},
	})
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.GracefulStop()

	binPath := buildBinary(t)

	out, err := exec.Command(binPath, "--orchestrator-endpoint", endpoint, "schedule", "graph", "test_schedule").Output()
	require.NoError(t, err)

	var payload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out, &payload))

	_, hasScheduleName := payload["schedule_name"]
	assert.True(t, hasScheduleName, "response must contain 'schedule_name' key")

	_, hasNodes := payload["nodes"]
	assert.True(t, hasNodes, "response must contain 'nodes' key")

	_, hasEdges := payload["edges"]
	assert.True(t, hasEdges, "response must contain 'edges' key")

	var nodes []json.RawMessage
	require.NoError(t, json.Unmarshal(payload["nodes"], &nodes))
	assert.Len(t, nodes, 1, "expected one node entry")

	var edges []json.RawMessage
	require.NoError(t, json.Unmarshal(payload["edges"], &edges))
	assert.Len(t, edges, 1, "expected one edge entry")
}
