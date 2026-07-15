package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	statev1 "github.com/carolsimone/continuo/cli/proto/state/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeNodeState struct {
	runsResp     *statev1.ListNodeRunsResponse
	runsErr      error
	gotSvc       string
	gotSchema    string
	gotTable     string
	gotOperation string
	gotLimit     int32

	trigResp   *statev1.TriggerSingleNodeRunResponse
	trigErr    error
	gotActor   string
	gotTrigSvc string

	testResp      *statev1.TriggerSingleNodeRunResponse
	testErr       error
	gotTestActor  string
	gotTestSvc    string
	gotTestSchema string
	gotTestTable  string

	buildResp      *statev1.TriggerSingleNodeRunResponse
	buildErr       error
	gotBuildActor  string
	gotBuildSvc    string
	gotBuildSchema string
	gotBuildTable  string
}

func (f *fakeNodeState) ListNodeRuns(_ context.Context, service, schema, table, operation string, limit int32) (*statev1.ListNodeRunsResponse, error) {
	f.gotSvc, f.gotSchema, f.gotTable, f.gotOperation, f.gotLimit = service, schema, table, operation, limit
	return f.runsResp, f.runsErr
}

func (f *fakeNodeState) TriggerNodeRun(_ context.Context, service, _, _, actor string) (*statev1.TriggerSingleNodeRunResponse, error) {
	f.gotTrigSvc, f.gotActor = service, actor
	return f.trigResp, f.trigErr
}

func (f *fakeNodeState) TriggerNodeTest(_ context.Context, service, schema, table, actor string) (*statev1.TriggerSingleNodeRunResponse, error) {
	f.gotTestSvc, f.gotTestSchema, f.gotTestTable, f.gotTestActor = service, schema, table, actor
	return f.testResp, f.testErr
}

func (f *fakeNodeState) TriggerNodeBuild(_ context.Context, service, schema, table, actor string) (*statev1.TriggerSingleNodeRunResponse, error) {
	f.gotBuildSvc, f.gotBuildSchema, f.gotBuildTable, f.gotBuildActor = service, schema, table, actor
	return f.buildResp, f.buildErr
}

func (f *fakeNodeState) TriggerSchedule(context.Context, string) (*statev1.TriggerScheduleResponse, error) {
	panic("TriggerSchedule should not be called in node tests")
}
func (f *fakeNodeState) TriggerScheduleTest(context.Context, string) (*statev1.TriggerScheduleResponse, error) {
	panic("TriggerScheduleTest should not be called in node tests")
}
func (f *fakeNodeState) TriggerScheduleBuild(context.Context, string) (*statev1.TriggerScheduleResponse, error) {
	return nil, nil
}
func (f *fakeNodeState) ListAllSchedules(context.Context) (*statev1.ListAllSchedulesResponse, error) {
	panic("ListAllSchedules should not be called in node tests")
}
func (f *fakeNodeState) ListTasks(context.Context, string, statev1.TaskStatus, int32, int32) (*statev1.ListTasksResponse, error) {
	panic("ListTasks should not be called in node tests")
}
func (f *fakeNodeState) CancelSchedule(context.Context, string, string, string) (*statev1.CancelScheduleResponse, error) {
	panic("CancelSchedule should not be called in node tests")
}
func (f *fakeNodeState) Close() error { return nil }

func runHistory(t *testing.T, fake client.StateClient, args []string, human bool) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human}
	cmd := NewHistoryCommand(func(context.Context, string) (client.StateClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
	cmd.SetArgs(args)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	err := cmd.Execute()
	exit = 0
	if err != nil {
		var cliErr output.CLIError
		if errors.As(err, &cliErr) {
			exit = cliErr.ExitCode()
		} else {
			exit = 1
		}
	}
	return outBuf.String(), errBuf.String(), exit
}

func TestHistory_SuccessMapsAllFields(t *testing.T) {
	fake := &fakeNodeState{runsResp: &statev1.ListNodeRunsResponse{Runs: []*statev1.NodeRun{{
		RunId: "run_1", ScheduleName: "single-node-run-abcd1234", Kind: "single_node_run",
		TerminalStatus: "succeeded", TaskId: "task_1", TaskStatus: "succeeded",
		RetryCount: 0, ImageTag: "img:v9", ManifestVersion: "m42",
		CreatedAt: "2026-07-10T10:00:00Z", CompletedAt: "2026-07-10T10:05:00Z",
	}}}}

	stdout, stderr, exit := runHistory(t, fake, []string{"finance", "analytics", "orders"}, false)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stderr)
	assert.Equal(t, int32(50), fake.gotLimit)
	assert.Equal(t, "finance", fake.gotSvc)
	assert.Equal(t, "analytics", fake.gotSchema)
	assert.Equal(t, "orders", fake.gotTable)
	assert.Equal(t, "run", fake.gotOperation)

	var payload struct {
		Runs []map[string]any `json:"runs"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	require.Len(t, payload.Runs, 1)
	r := payload.Runs[0]
	assert.Equal(t, "run_1", r["run_id"])
	assert.Equal(t, "single_node_run", r["kind"])
	assert.Equal(t, "succeeded", r["terminal_status"])
	assert.Equal(t, "img:v9", r["image_tag"])
	assert.Equal(t, "m42", r["manifest_version"])
}

func TestHistory_OperationFlagIsForwarded(t *testing.T) {
	fake := &fakeNodeState{runsResp: &statev1.ListNodeRunsResponse{Runs: []*statev1.NodeRun{{
		RunId: "run_1", TaskStatus: "succeeded", Operation: "test",
	}}}}

	stdout, _, exit := runHistory(t, fake, []string{"finance", "analytics", "orders", "--operation", "test"}, false)

	assert.Equal(t, 0, exit)
	assert.Equal(t, "test", fake.gotOperation)

	var payload struct {
		Runs []map[string]any `json:"runs"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	require.Len(t, payload.Runs, 1)
	assert.Equal(t, "test", payload.Runs[0]["operation"])
}

func TestHistory_InvalidOperationFlagExits2(t *testing.T) {
	fake := &fakeNodeState{}
	stdout, _, exit := runHistory(t, fake, []string{"finance", "analytics", "orders", "--operation", "bogus"}, false)
	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestHistory_EmptyResultIsEmptyArrayNotNull(t *testing.T) {
	fake := &fakeNodeState{runsResp: &statev1.ListNodeRunsResponse{Runs: nil}}
	stdout, _, exit := runHistory(t, fake, []string{"finance", "analytics", "orders"}, false)
	assert.Equal(t, 0, exit)
	assert.Contains(t, stdout, `"runs":[]`)
}

func TestHistory_OmitsEmptyOptionalFields(t *testing.T) {
	fake := &fakeNodeState{runsResp: &statev1.ListNodeRunsResponse{Runs: []*statev1.NodeRun{{
		RunId: "run_1", TaskStatus: "running", // no terminal_status/completed_at/error/log key
	}}}}
	stdout, _, _ := runHistory(t, fake, []string{"finance", "analytics", "orders"}, false)
	assert.NotContains(t, stdout, "terminal_status")
	assert.NotContains(t, stdout, "completed_at")
	assert.NotContains(t, stdout, "error_message")
	assert.NotContains(t, stdout, "log_s3_key")
}

func TestHistory_WrongArgCountExits2(t *testing.T) {
	fake := &fakeNodeState{}
	stdout, _, exit := runHistory(t, fake, []string{"finance", "analytics"}, false)
	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestHistory_UnavailableExits5(t *testing.T) {
	fake := &fakeNodeState{runsErr: status.Error(codes.Unavailable, "server down")}
	_, _, exit := runHistory(t, fake, []string{"finance", "analytics", "orders"}, false)
	assert.Equal(t, 5, exit)
}

func TestHistory_HumanModeUsesStderr(t *testing.T) {
	fake := &fakeNodeState{runsResp: &statev1.ListNodeRunsResponse{Runs: []*statev1.NodeRun{{
		RunId: "run_1", TaskStatus: "succeeded", Kind: "single_node_run", CompletedAt: "2026-07-10T10:05:00Z",
	}}}}
	stdout, stderr, exit := runHistory(t, fake, []string{"finance", "analytics", "orders"}, true)
	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "run_1")
	assert.Contains(t, stderr, "succeeded")
}
