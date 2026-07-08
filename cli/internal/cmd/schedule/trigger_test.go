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

type fakeState struct {
	resp            *statev1.TriggerScheduleResponse
	err             error
	gotScheduleName string

	cancelResp      *statev1.CancelScheduleResponse
	cancelErr       error
	gotCancelReason string
	gotCancelBy     string
}

func (f *fakeState) TriggerSchedule(_ context.Context, name string) (*statev1.TriggerScheduleResponse, error) {
	f.gotScheduleName = name
	return f.resp, f.err
}

func (f *fakeState) CancelSchedule(_ context.Context, name, reason, by string) (*statev1.CancelScheduleResponse, error) {
	f.gotScheduleName = name
	f.gotCancelReason = reason
	f.gotCancelBy = by
	return f.cancelResp, f.cancelErr
}

func (f *fakeState) Close() error { return nil }

func (f *fakeState) ListAllSchedules(_ context.Context) (*statev1.ListAllSchedulesResponse, error) {
	return nil, nil
}

func (f *fakeState) ListTasks(_ context.Context, _ string, _ statev1.TaskStatus, _, _ int32) (*statev1.ListTasksResponse, error) {
	panic("ListTasks should not be called in trigger tests")
}

// run invokes the trigger command end-to-end with the provided fake client and args.
// It captures stdout/stderr and returns the exit code.
func run(t *testing.T, fake client.StateClient, args []string, human bool) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human}
	cmd := NewTriggerCommand(func(_ context.Context, _ string) (client.StateClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
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

func TestTrigger_SuccessEmitsJSON(t *testing.T) {
	fake := &fakeState{resp: &statev1.TriggerScheduleResponse{ScheduleId: "sched_123"}}

	stdout, stderr, exit := run(t, fake, []string{"daily_ingest"}, false)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stderr)
	var payload map[string]string
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	assert.Equal(t, "sched_123", payload["schedule_id"])
	assert.Equal(t, "daily_ingest", payload["schedule_name"])
	_, err := time.Parse(time.RFC3339, payload["triggered_at"])
	assert.NoError(t, err, "triggered_at must be RFC3339")
	assert.Equal(t, "daily_ingest", fake.gotScheduleName)
}

func TestTrigger_NotFoundExits3(t *testing.T) {
	fake := &fakeState{err: status.Error(codes.NotFound, "schedule 'missing' not found")}

	stdout, _, exit := run(t, fake, []string{"missing"}, false)

	assert.Equal(t, 3, exit)
	var envelope struct {
		Error output.CLIError `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &envelope))
	assert.Equal(t, output.CodeNotFound, envelope.Error.Code)
	assert.False(t, envelope.Error.Retryable)
}

func TestTrigger_FailedPreconditionExits4(t *testing.T) {
	fake := &fakeState{err: status.Error(codes.FailedPrecondition, "already running")}

	stdout, _, exit := run(t, fake, []string{"daily_ingest"}, false)

	assert.Equal(t, 4, exit)
	var envelope struct {
		Error output.CLIError `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &envelope))
	assert.Equal(t, output.CodeConflict, envelope.Error.Code)
	assert.True(t, envelope.Error.Retryable)
}

func TestTrigger_UnavailableExits5(t *testing.T) {
	fake := &fakeState{err: status.Error(codes.Unavailable, "server down")}

	_, _, exit := run(t, fake, []string{"daily_ingest"}, false)

	assert.Equal(t, 5, exit)
}

func TestTrigger_MissingArgumentExits2(t *testing.T) {
	fake := &fakeState{}

	stdout, _, exit := run(t, fake, []string{}, false)

	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestTrigger_HumanModeUsesStderrAndEmptyStdout(t *testing.T) {
	fake := &fakeState{resp: &statev1.TriggerScheduleResponse{ScheduleId: "sched_123"}}

	stdout, stderr, exit := run(t, fake, []string{"daily_ingest"}, true)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "Triggered run sched_123 for schedule 'daily_ingest'")
}
