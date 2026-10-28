//go:build integration

package cli_test

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// cliBinary is set by TestMain once at the start of all integration tests
var cliBinary string

// ---------------------------------------------------------------------------
// TestMain: build the binary once for all integration tests
// ---------------------------------------------------------------------------

func TestMain(m *testing.M) {
	// Create a temporary directory for the binary
	tmpDir, err := os.MkdirTemp("", "continuo-integration-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	// Build the binary once
	binPath := filepath.Join(tmpDir, "continuo")
	cmd := exec.Command("go", "build", "-o", binPath, "./cmd/continuo")
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		panic(err)
	}

	cliBinary = binPath

	// Run all tests
	os.Exit(m.Run())
}

// ---------------------------------------------------------------------------
// Stub servers
// ---------------------------------------------------------------------------

type stubRunServer struct {
	runv1.UnimplementedRunServiceServer
	startResp *runv1.StartRunResponse
	listResp  *runv1.ListSchedulesResponse
	graphResp *runv1.GetScheduleGraphResponse

	mu sync.Mutex
	// startFailures is how many StartRun calls fail UNAVAILABLE before one
	// succeeds; startIDs is the request id of every StartRun received.
	startFailures int
	startIDs      []string

	// The schedule edits: the requests received, the status codes the first
	// calls fail with, and the answer of a successful upsert.
	upserts    []*runv1.UpsertScheduleRequest
	deletes    []*runv1.DeleteScheduleRequest
	upsertErrs []error
	upsertResp *runv1.UpsertScheduleResponse
	previewMsg string

	// rebaseExpected is the release the stub treats as live; a TriggerRebase
	// naming another release is refused FAILED_PRECONDITION. rebaseReqs holds
	// every TriggerRebase received.
	rebaseExpected string
	rebaseReqs     []*runv1.TriggerRebaseRequest
}

func (s *stubRunServer) TriggerRebase(_ context.Context, req *runv1.TriggerRebaseRequest) (*runv1.TriggerRebaseResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rebaseReqs = append(s.rebaseReqs, req)
	if req.GetExpectedReleaseId() != s.rebaseExpected {
		return nil, status.Error(codes.FailedPrecondition, "the live release changed; preview again")
	}
	return &runv1.TriggerRebaseResponse{RunId: "run_rebase_1", ScheduleName: "daily_ingest"}, nil
}

func (s *stubRunServer) StartRun(_ context.Context, req *runv1.StartRunRequest) (*runv1.StartRunResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startIDs = append(s.startIDs, req.GetRequestId())
	if s.startFailures > 0 {
		s.startFailures--
		return nil, status.Error(codes.Unavailable, "connection reset")
	}
	return s.startResp, nil
}

func (s *stubRunServer) ListSchedules(_ context.Context, _ *runv1.ListSchedulesRequest) (*runv1.ListSchedulesResponse, error) {
	return s.listResp, nil
}

func (s *stubRunServer) GetScheduleGraph(_ context.Context, _ *runv1.GetScheduleGraphRequest) (*runv1.GetScheduleGraphResponse, error) {
	return s.graphResp, nil
}

func (s *stubRunServer) UpsertSchedule(_ context.Context, req *runv1.UpsertScheduleRequest) (*runv1.UpsertScheduleResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upserts = append(s.upserts, req)
	if n := len(s.upserts); len(s.upsertErrs) >= n && s.upsertErrs[n-1] != nil {
		return nil, s.upsertErrs[n-1]
	}
	return s.upsertResp, nil
}

func (s *stubRunServer) DeleteSchedule(_ context.Context, req *runv1.DeleteScheduleRequest) (*runv1.DeleteScheduleResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletes = append(s.deletes, req)
	return &runv1.DeleteScheduleResponse{}, nil
}

func (s *stubRunServer) PreviewSchedule(_ context.Context, _ *runv1.PreviewScheduleRequest) (*runv1.PreviewScheduleResponse, error) {
	return &runv1.PreviewScheduleResponse{CronWords: s.previewMsg}, nil
}

const integRunID = "11111111-1111-4111-8111-111111111111"

func (s *stubRunServer) GetRun(_ context.Context, req *runv1.GetRunRequest) (*runv1.RunResponse, error) {
	if req.GetRunId() != integRunID {
		return nil, status.Error(codes.NotFound, "run not found")
	}
	return &runv1.RunResponse{Run: &runv1.Run{RunId: integRunID, ScheduleName: "daily_ingest", Status: "failed"}}, nil
}

func (s *stubRunServer) ListTasks(context.Context, *runv1.ListTasksRequest) (*runv1.ListTasksResponse, error) {
	return &runv1.ListTasksResponse{TotalCount: 1, Tasks: []*runv1.Task{
		{TaskId: "t1", ServiceName: "svc", SchemaName: "raw", TableName: "orders", Status: "failed", Attempt: 1},
	}}, nil
}

func (s *stubRunServer) ListRunAttempts(context.Context, *runv1.ListRunAttemptsRequest) (*runv1.ListRunAttemptsResponse, error) {
	return &runv1.ListRunAttemptsResponse{TotalCount: 1, Attempts: []*runv1.TaskAttempt{
		{TaskId: "t1", Attempt: 1, Status: "failed", FailureClass: "task", FailureReason: "dbt_error"},
	}}, nil
}

// serveRun serves srv on a loopback port and returns its address.
func serveRun(t *testing.T, srv runv1.RunServiceServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcSrv := grpc.NewServer()
	runv1.RegisterRunServiceServer(grpcSrv, srv)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.GracefulStop)
	return lis.Addr().String()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestCLI_TriggerScheduleAgainstInProcessServer(t *testing.T) {
	endpoint := serveRun(t, &stubRunServer{startResp: &runv1.StartRunResponse{RunId: "run_integ_1"}})

	out, err := exec.Command(cliBinary, "--endpoint", endpoint, "schedule", "trigger", "daily_ingest").Output()
	require.NoError(t, err)

	var payload map[string]string
	require.NoError(t, json.Unmarshal(out, &payload))
	assert.Equal(t, "run_integ_1", payload["run_id"])
	assert.Equal(t, "daily_ingest", payload["schedule_name"])
	assert.NotEmpty(t, payload["triggered_at"])
}

// A trigger refused UNAVAILABLE is tried again, and every try carries the one
// request id of the invocation, so the server can recognise the repeat.
func TestCLI_TriggerRetriesOnUnavailableWithOneRequestID(t *testing.T) {
	srv := &stubRunServer{startResp: &runv1.StartRunResponse{RunId: "run_integ_2"}, startFailures: 2}
	endpoint := serveRun(t, srv)

	cmd := exec.Command(cliBinary, "--endpoint", endpoint, "schedule", "trigger", "daily_ingest")
	cmd.Env = append(os.Environ(), "CONTINUO_REQUEST_ID=")
	out, err := cmd.Output()
	require.NoError(t, err)

	var payload map[string]string
	require.NoError(t, json.Unmarshal(out, &payload))
	assert.Equal(t, "run_integ_2", payload["run_id"])
	require.Len(t, srv.startIDs, 3)
	assert.NotEmpty(t, srv.startIDs[0], "a request id is sent without any configuration")
	assert.Equal(t, srv.startIDs[0], srv.startIDs[1])
	assert.Equal(t, srv.startIDs[0], srv.startIDs[2])
}

func TestCLI_RequestIDComesFromTheEnvironmentWhenSet(t *testing.T) {
	srv := &stubRunServer{startResp: &runv1.StartRunResponse{RunId: "run_integ_3"}}
	endpoint := serveRun(t, srv)

	cmd := exec.Command(cliBinary, "--endpoint", endpoint, "schedule", "trigger", "daily_ingest")
	cmd.Env = append(os.Environ(), "CONTINUO_REQUEST_ID=5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b")
	_, err := cmd.Output()
	require.NoError(t, err)
	assert.Equal(t, []string{"5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b"}, srv.startIDs)
}

func TestCLI_ScheduleListAgainstInProcessServer(t *testing.T) {
	endpoint := serveRun(t, &stubRunServer{
		listResp: &runv1.ListSchedulesResponse{
			Schedules: []*runv1.ScheduleSummary{
				{
					ScheduleName:  "daily_ingest",
					Cron:          "0 2 * * *",
					Timezone:      "UTC",
					HasActiveRun:  false,
					LastRunStatus: "succeeded",
					LastRunId:     "run_001",
				},
			},
		},
	})

	out, err := exec.Command(cliBinary, "--endpoint", endpoint, "schedule", "list").Output()
	require.NoError(t, err)

	var payload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out, &payload))
	_, ok := payload["schedules"]
	assert.True(t, ok, "response must contain 'schedules' key")

	var schedules []map[string]interface{}
	require.NoError(t, json.Unmarshal(payload["schedules"], &schedules))
	assert.Len(t, schedules, 1, "expected one schedule entry")
	assert.Equal(t, "daily_ingest", schedules[0]["schedule_name"])
	assert.Equal(t, false, schedules[0]["has_active_run"])
}

func TestCLI_ScheduleGraphAgainstInProcessServer(t *testing.T) {
	endpoint := serveRun(t, &stubRunServer{
		graphResp: &runv1.GetScheduleGraphResponse{
			Nodes: []*runv1.ScheduleGraphNode{
				{
					TableName:    "orders",
					SchemaName:   "public",
					ServiceName:  "analytics",
					NodeType:     "model",
					ScheduleName: "test_schedule",
				},
			},
			Edges: []*runv1.ScheduleGraphEdge{
				{
					FromNodeId: "node_a",
					ToNodeId:   "node_b",
				},
			},
			PromotionSeq: 3,
		},
	})

	out, err := exec.Command(cliBinary, "--endpoint", endpoint, "schedule", "graph", "test_schedule").Output()
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

// exitCodeOf returns the exit code of a finished CLI command.
func exitCodeOf(t *testing.T, err error) int {
	t.Helper()
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	return exitErr.ExitCode()
}

func TestCLI_ScheduleSetCronAgainstInProcessServer(t *testing.T) {
	srv := &stubRunServer{
		listResp: &runv1.ListSchedulesResponse{Schedules: []*runv1.ScheduleSummary{
			{ScheduleName: "daily", Cron: "0 1 * * *", Timezone: "UTC", Description: "Runs daily", Version: 3, ModelCount: 4},
		}},
		upsertResp: &runv1.UpsertScheduleResponse{
			Schedule:      &runv1.Schedule{ScheduleName: "daily", Cron: "0 2 * * *", Timezone: "Europe/Paris", Description: "Runs daily", Version: 4},
			NextFireTimes: []*timestamppb.Timestamp{timestamppb.New(time.Date(2026, 10, 28, 1, 0, 0, 0, time.UTC))},
		},
		previewMsg: "At 02:00",
	}
	endpoint := serveRun(t, srv)

	out, err := exec.Command(cliBinary, "--endpoint", endpoint, "schedule", "set-cron", "daily", "0 2 * * *", "Europe/Paris").Output()
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(out, &payload))
	assert.Equal(t, "daily", payload["schedule_name"])
	assert.Equal(t, "At 02:00", payload["cron_words"])
	assert.Equal(t, []any{"2026-10-28T01:00:00Z"}, payload["next_fire_times"])
	require.Len(t, srv.upserts, 1)
	assert.Equal(t, "0 2 * * *", srv.upserts[0].GetCron(), "a cron of several fields reaches the server as one value")
	assert.Equal(t, "Runs daily", srv.upserts[0].GetDescription(), "the description is kept")
	assert.EqualValues(t, 3, srv.upserts[0].GetExpectedVersion(), "the version of the schedule as listed")
}

// An ABORTED upsert is a stale version. set-cron reads the schedule again and
// saves once more; the connection never repeats an ABORTED on its own, so the
// server sees exactly two saves.
func TestCLI_ScheduleSetCronSavesOnceMoreAfterAStaleVersion(t *testing.T) {
	srv := &stubRunServer{
		listResp: &runv1.ListSchedulesResponse{Schedules: []*runv1.ScheduleSummary{
			{ScheduleName: "daily", Cron: "0 1 * * *", Timezone: "UTC", Description: "Runs daily", Version: 3, ModelCount: 4},
		}},
		upsertErrs: []error{status.Error(codes.Aborted, "schedule \"daily\" is at version 4, not 3")},
		upsertResp: &runv1.UpsertScheduleResponse{
			Schedule: &runv1.Schedule{ScheduleName: "daily", Cron: "0 2 * * *", Timezone: "UTC", Description: "Runs daily", Version: 5},
		},
	}
	endpoint := serveRun(t, srv)

	out, err := exec.Command(cliBinary, "--endpoint", endpoint, "schedule", "set-cron", "daily", "0 2 * * *", "UTC").Output()
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(out, &payload))
	assert.EqualValues(t, 5, payload["version"])
	require.Len(t, srv.upserts, 2, "the first save and its one retry")
}

func TestCLI_ScheduleSetCronExitsFourOnASecondStaleVersion(t *testing.T) {
	srv := &stubRunServer{
		listResp: &runv1.ListSchedulesResponse{Schedules: []*runv1.ScheduleSummary{
			{ScheduleName: "daily", Cron: "0 1 * * *", Timezone: "UTC", Version: 3, UpdatedBy: "bob@example.com"},
		}},
		upsertErrs: []error{
			status.Error(codes.Aborted, "schedule \"daily\" is at version 4, not 3"),
			status.Error(codes.Aborted, "schedule \"daily\" is at version 5, not 3"),
		},
	}
	endpoint := serveRun(t, srv)

	out, err := exec.Command(cliBinary, "--endpoint", endpoint, "schedule", "set-cron", "daily", "0 2 * * *", "UTC").Output()

	assert.Equal(t, 4, exitCodeOf(t, err))
	var env struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal(out, &env))
	assert.Equal(t, "conflict", env.Error.Code)
	assert.Equal(t, "changed by bob@example.com while this ran; run it again", env.Error.Message)
	require.Len(t, srv.upserts, 2, "one save and one retry, no more")
}

// An ABORTED upsert is a stale version. The CLI sends it once and exits with
// the conflict code.
func TestCLI_SchedulePauseExitsFourOnAStaleVersionWithoutRetrying(t *testing.T) {
	srv := &stubRunServer{
		listResp: &runv1.ListSchedulesResponse{Schedules: []*runv1.ScheduleSummary{
			{ScheduleName: "daily", Cron: "0 1 * * *", Timezone: "UTC", Version: 3, UpdatedBy: "bob@example.com"},
		}},
		upsertErrs: []error{status.Error(codes.Aborted, "schedule \"daily\" is at version 4, not 3")},
	}
	endpoint := serveRun(t, srv)

	out, err := exec.Command(cliBinary, "--endpoint", endpoint, "schedule", "pause", "daily").Output()

	assert.Equal(t, 4, exitCodeOf(t, err))
	var env struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal(out, &env))
	assert.Equal(t, "conflict", env.Error.Code)
	assert.Equal(t, "changed by bob@example.com while this ran; run it again", env.Error.Message)
	require.Len(t, srv.upserts, 1, "the stale version is sent once")
	assert.EqualValues(t, 3, srv.upserts[0].GetExpectedVersion())
}

func TestCLI_ScheduleRemoveCronAgainstInProcessServer(t *testing.T) {
	srv := &stubRunServer{}
	endpoint := serveRun(t, srv)

	out, err := exec.Command(cliBinary, "--endpoint", endpoint, "schedule", "remove-cron", "e2e-schedule").Output()
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(out, &payload))
	assert.Equal(t, false, payload["has_cron"])
	require.Len(t, srv.deletes, 1)
	assert.Equal(t, "e2e-schedule", srv.deletes[0].GetScheduleName())
}

func TestCLI_ScheduleSetCronWithAnUnquotedCronExitsTwo(t *testing.T) {
	endpoint := serveRun(t, &stubRunServer{})

	out, err := exec.Command(cliBinary, "--endpoint", endpoint, "schedule", "set-cron", "daily", "0", "1", "*", "*", "*", "UTC").Output()

	assert.Equal(t, 2, exitCodeOf(t, err))
	assert.Contains(t, string(out), "quote")
}

func TestCLI_RunShowAgainstInProcessServer(t *testing.T) {
	endpoint := serveRun(t, &stubRunServer{})

	out, err := exec.Command(cliBinary, "--endpoint", endpoint, "run", "show", integRunID).Output()
	require.NoError(t, err)

	var payload struct {
		Run   map[string]any `json:"run"`
		Tasks []struct {
			Node          string `json:"node"`
			FailureReason string `json:"failure_reason"`
		} `json:"tasks"`
	}
	require.NoError(t, json.Unmarshal(out, &payload))
	assert.Equal(t, "failed", payload.Run["status"])
	require.Len(t, payload.Tasks, 1)
	assert.Equal(t, "svc.raw.orders", payload.Tasks[0].Node)
	assert.Equal(t, "dbt_error", payload.Tasks[0].FailureReason)
}

func TestCLI_RunShowOfAnUnknownRunExitsNotFound(t *testing.T) {
	endpoint := serveRun(t, &stubRunServer{})

	out, err := exec.Command(cliBinary, "--endpoint", endpoint, "run", "show", "22222222-2222-4222-8222-222222222222").Output()

	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	assert.Equal(t, 3, exitErr.ExitCode())
	assert.Contains(t, string(out), `"not_found"`)
}

func TestCLI_RunRebaseAgainstInProcessServer(t *testing.T) {
	srv := &stubRunServer{rebaseExpected: "rel-9"}
	endpoint := serveRun(t, srv)
	const requestID = "5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b"

	cmd := exec.Command(cliBinary, "--endpoint", endpoint, "run", "rebase", integRunID, "rel-9", "true")
	cmd.Env = append(os.Environ(), "CONTINUO_REQUEST_ID="+requestID)
	out, err := cmd.Output()
	require.NoError(t, err)

	assert.JSONEq(t, `{"run_id":"run_rebase_1","schedule_name":"daily_ingest"}`, string(out))
	require.Len(t, srv.rebaseReqs, 1)
	assert.Equal(t, requestID, srv.rebaseReqs[0].GetRequestId())
	assert.Equal(t, integRunID, srv.rebaseReqs[0].GetSourceRunId())
	assert.Equal(t, "rel-9", srv.rebaseReqs[0].GetExpectedReleaseId())
	assert.True(t, srv.rebaseReqs[0].GetIncludeChangedUpstream())
}

func TestCLI_RunRebaseOfAMovedReleaseExitsConflictAndSaysPreviewAgain(t *testing.T) {
	endpoint := serveRun(t, &stubRunServer{rebaseExpected: "rel-10"})

	out, err := exec.Command(cliBinary, "--endpoint", endpoint, "run", "rebase", integRunID, "rel-9").Output()

	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	assert.Equal(t, 4, exitErr.ExitCode())
	assert.Contains(t, string(out), "continuo run rebase-preview "+integRunID)
}

func TestCLI_RunRebaseWithoutAReleaseIDExitsUsage(t *testing.T) {
	endpoint := serveRun(t, &stubRunServer{})

	_, err := exec.Command(cliBinary, "--endpoint", endpoint, "run", "rebase", integRunID).Output()

	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	assert.Equal(t, 2, exitErr.ExitCode())
}
