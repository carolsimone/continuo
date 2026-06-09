package schedule

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

// fakeStateStatus implements client.StateClient for status tests.
type fakeStateStatus struct {
	schedules     *statev1.ListAllSchedulesResponse
	schedulesErr  error
	pages         []*statev1.ListTasksResponse // returned in order, one per ListTasks call
	listErr       error
	listTasksCall int
}

func (f *fakeStateStatus) TriggerSchedule(_ context.Context, _ string) (*statev1.TriggerScheduleResponse, error) {
	panic("TriggerSchedule should not be called in status tests")
}
func (f *fakeStateStatus) ListAllSchedules(_ context.Context) (*statev1.ListAllSchedulesResponse, error) {
	return f.schedules, f.schedulesErr
}
func (f *fakeStateStatus) ListTasks(_ context.Context, _ string, _ statev1.TaskStatus, _, _ int32) (*statev1.ListTasksResponse, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	resp := f.pages[f.listTasksCall]
	f.listTasksCall++
	return resp, nil
}
func (f *fakeStateStatus) Close() error { return nil }

func runStatus(t *testing.T, fake client.StateClient, args []string, human bool) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human}
	cmd := NewStatusCommand(
		func(_ context.Context, _ string) (client.StateClient, error) { return fake, nil },
		cfg, &outBuf, &errBuf,
	)
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

type statusNodeJSON struct {
	ServiceName string `json:"service_name"`
	SchemaName  string `json:"schema_name"`
	TableName   string `json:"table_name"`
	Status      string `json:"status"`
}
type statusPayloadJSON struct {
	ScheduleName  string           `json:"schedule_name"`
	RunId         string           `json:"run_id"`
	IsRunning     bool             `json:"is_running"`
	LastRunStatus string           `json:"last_run_status"`
	Nodes         []statusNodeJSON `json:"nodes"`
}

func TestStatus_SuccessEmitsNodesJSON(t *testing.T) {
	fake := &fakeStateStatus{
		schedules: &statev1.ListAllSchedulesResponse{Schedules: []*statev1.ScheduleSummary{
			{ScheduleName: "daily", LastRunId: "run-1", IsRunning: true, LastRunStatus: ""},
		}},
		pages: []*statev1.ListTasksResponse{{
			TotalCount: 2,
			Tasks: []*statev1.Task{
				{ServiceName: "svc", SchemaName: "analytics", TableName: "orders", Status: statev1.TaskStatus_TASK_STATUS_SUCCEEDED},
				{ServiceName: "svc", SchemaName: "analytics", TableName: "revenue", Status: statev1.TaskStatus_TASK_STATUS_RUNNING},
			},
		}},
	}

	stdout, stderr, exit := runStatus(t, fake, []string{"daily"}, false)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stderr)
	var p statusPayloadJSON
	require.NoError(t, json.Unmarshal([]byte(stdout), &p))
	assert.Equal(t, "daily", p.ScheduleName)
	assert.Equal(t, "run-1", p.RunId)
	assert.True(t, p.IsRunning)
	require.Len(t, p.Nodes, 2)
	assert.Equal(t, "succeeded", p.Nodes[0].Status)
	assert.Equal(t, "running", p.Nodes[1].Status)
	assert.Equal(t, "orders", p.Nodes[0].TableName)
}

func TestStatus_PagesThroughAllNodes(t *testing.T) {
	page1 := &statev1.ListTasksResponse{TotalCount: 3, Tasks: []*statev1.Task{
		{ServiceName: "s", SchemaName: "x", TableName: "a", Status: statev1.TaskStatus_TASK_STATUS_SUCCEEDED},
		{ServiceName: "s", SchemaName: "x", TableName: "b", Status: statev1.TaskStatus_TASK_STATUS_FAILED},
	}}
	page2 := &statev1.ListTasksResponse{TotalCount: 3, Tasks: []*statev1.Task{
		{ServiceName: "s", SchemaName: "x", TableName: "c", Status: statev1.TaskStatus_TASK_STATUS_SKIPPED},
	}}
	fake := &fakeStateStatus{
		schedules: &statev1.ListAllSchedulesResponse{Schedules: []*statev1.ScheduleSummary{
			{ScheduleName: "daily", LastRunId: "run-1"},
		}},
		pages: []*statev1.ListTasksResponse{page1, page2},
	}

	stdout, _, exit := runStatus(t, fake, []string{"daily"}, false)

	assert.Equal(t, 0, exit)
	var p statusPayloadJSON
	require.NoError(t, json.Unmarshal([]byte(stdout), &p))
	require.Len(t, p.Nodes, 3)
	assert.Equal(t, "skipped", p.Nodes[2].Status)
	assert.Equal(t, 2, fake.listTasksCall)
}

func TestStatus_UnknownNameExits3(t *testing.T) {
	fake := &fakeStateStatus{schedules: &statev1.ListAllSchedulesResponse{Schedules: []*statev1.ScheduleSummary{
		{ScheduleName: "other", LastRunId: "run-9"},
	}}}

	stdout, _, exit := runStatus(t, fake, []string{"daily"}, false)

	assert.Equal(t, 3, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeNotFound, env["error"].Code)
}

func TestStatus_NeverRunReturnsEmptyNodesNoListCall(t *testing.T) {
	fake := &fakeStateStatus{schedules: &statev1.ListAllSchedulesResponse{Schedules: []*statev1.ScheduleSummary{
		{ScheduleName: "daily", LastRunId: "", IsRunning: false, LastRunStatus: ""},
	}}}

	stdout, _, exit := runStatus(t, fake, []string{"daily"}, false)

	assert.Equal(t, 0, exit)
	var p statusPayloadJSON
	require.NoError(t, json.Unmarshal([]byte(stdout), &p))
	assert.Equal(t, "", p.RunId)
	assert.Empty(t, p.Nodes)
	assert.NotNil(t, p.Nodes) // JSON [] not null
	assert.Equal(t, 0, fake.listTasksCall)
}

func TestStatus_ListTasksRPCErrorExits5(t *testing.T) {
	fake := &fakeStateStatus{
		schedules: &statev1.ListAllSchedulesResponse{Schedules: []*statev1.ScheduleSummary{
			{ScheduleName: "daily", LastRunId: "run-1"},
		}},
		listErr: status.Error(codes.Unavailable, "down"),
	}

	_, _, exit := runStatus(t, fake, []string{"daily"}, false)

	assert.Equal(t, 5, exit)
}

func TestStatus_WrongArgCountExits2(t *testing.T) {
	fake := &fakeStateStatus{}
	_, _, exit := runStatus(t, fake, []string{}, false)
	assert.Equal(t, 2, exit)
}

// TestStatus_OutputSchemaMatchesEmittedKeys is the drift control: the declared
// output_schema annotation must list exactly the top-level keys the command emits.
func TestStatus_OutputSchemaMatchesEmittedKeys(t *testing.T) {
	fake := &fakeStateStatus{schedules: &statev1.ListAllSchedulesResponse{Schedules: []*statev1.ScheduleSummary{
		{ScheduleName: "daily", LastRunId: ""},
	}}}
	stdout, _, _ := runStatus(t, fake, []string{"daily"}, false)

	var emitted map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(stdout), &emitted))

	cmd := NewStatusCommand(nil, &config.Config{}, &bytes.Buffer{}, &bytes.Buffer{})
	var schema map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(cmd.Annotations["output_schema"]), &schema))

	assert.ElementsMatch(t, keys(emitted), keys(schema))
}

func keys(m map[string]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
