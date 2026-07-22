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

// runTest invokes the test command end-to-end with the provided fake client and args.
func runTest(t *testing.T, fake client.StateClient, args []string, human bool) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human}
	cmd := NewTestCommand(func(_ context.Context, _ string) (client.StateClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
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

func TestScheduleTest_ForwardsActor(t *testing.T) {
	fake := &fakeState{testResp: &statev1.TriggerScheduleResponse{ScheduleId: "s1"}}
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Actor: "okta.example.com|alice"}
	cmd := NewTestCommand(func(_ context.Context, _ string) (client.StateClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
	cmd.SetArgs([]string{"daily"})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	require.NoError(t, cmd.Execute())
	assert.Equal(t, "okta.example.com|alice", fake.gotTestActor)
}

func TestScheduleTest_SuccessEmitsJSON(t *testing.T) {
	fake := &fakeState{testResp: &statev1.TriggerScheduleResponse{ScheduleId: "sched_456"}}

	stdout, stderr, exit := runTest(t, fake, []string{"daily_ingest"}, false)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stderr)
	var payload map[string]string
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	assert.Equal(t, "sched_456", payload["schedule_id"])
	assert.Equal(t, "daily_ingest", payload["schedule_name"])
	_, err := time.Parse(time.RFC3339, payload["triggered_at"])
	assert.NoError(t, err, "triggered_at must be RFC3339")
	assert.Equal(t, "daily_ingest", fake.gotTestScheduleName)
}

func TestScheduleTest_NotFoundExits3(t *testing.T) {
	fake := &fakeState{testErr: status.Error(codes.NotFound, "schedule 'missing' not found")}

	stdout, _, exit := runTest(t, fake, []string{"missing"}, false)

	assert.Equal(t, 3, exit)
	var envelope struct {
		Error output.CLIError `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &envelope))
	assert.Equal(t, output.CodeNotFound, envelope.Error.Code)
}

func TestScheduleTest_MissingArgumentExits2(t *testing.T) {
	fake := &fakeState{}

	stdout, _, exit := runTest(t, fake, []string{}, false)

	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestScheduleTest_TooManyArgumentsExits2(t *testing.T) {
	fake := &fakeState{}

	_, _, exit := runTest(t, fake, []string{"daily_ingest", "extra"}, false)

	assert.Equal(t, 2, exit)
}

func TestScheduleTest_HumanModeUsesStderrAndEmptyStdout(t *testing.T) {
	fake := &fakeState{testResp: &statev1.TriggerScheduleResponse{ScheduleId: "sched_456"}}

	stdout, stderr, exit := runTest(t, fake, []string{"daily_ingest"}, true)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "sched_456")
}
