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

// runCancel invokes the cancel command end-to-end with the provided fake client
// and args, capturing stdout/stderr and returning the exit code. actor is the
// resolved CONTINUO_ACTOR value (config.Config.Actor) forwarded as cancelled_by.
func runCancel(t *testing.T, fake client.StateClient, args []string, human bool, actor string) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human, Actor: actor}
	cmd := NewCancelCommand(func(_ context.Context, _ string) (client.StateClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
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

func TestCancel_SuccessEmitsJSON(t *testing.T) {
	fake := &fakeState{cancelResp: &statev1.CancelScheduleResponse{ScheduleId: "sched_123"}}

	stdout, stderr, exit := runCancel(t, fake, []string{"daily_ingest", "bad upstream data"}, false, "")

	assert.Equal(t, 0, exit)
	assert.Empty(t, stderr)
	var payload map[string]string
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	assert.Equal(t, "sched_123", payload["schedule_id"])
	assert.Equal(t, "daily_ingest", payload["schedule_name"])
	_, err := time.Parse(time.RFC3339, payload["cancelled_at"])
	assert.NoError(t, err, "cancelled_at must be RFC3339")
	assert.Equal(t, "daily_ingest", fake.gotScheduleName)
	assert.Equal(t, "bad upstream data", fake.gotCancelReason)
}

func TestCancel_ForwardsActorAsCancelledBy(t *testing.T) {
	fake := &fakeState{cancelResp: &statev1.CancelScheduleResponse{ScheduleId: "sched_123"}}

	_, _, exit := runCancel(t, fake, []string{"daily_ingest", "cleanup"}, false, "agent-chat-llm")

	assert.Equal(t, 0, exit)
	assert.Equal(t, "agent-chat-llm", fake.gotCancelBy)
}

func TestCancel_EmptyActorSendsEmptyCancelledBy(t *testing.T) {
	fake := &fakeState{cancelResp: &statev1.CancelScheduleResponse{ScheduleId: "sched_123"}}

	_, _, exit := runCancel(t, fake, []string{"daily_ingest", "cleanup"}, false, "")

	assert.Equal(t, 0, exit)
	assert.Equal(t, "", fake.gotCancelBy)
}

func TestCancel_MissingReasonArgumentExits2(t *testing.T) {
	fake := &fakeState{}

	stdout, _, exit := runCancel(t, fake, []string{"daily_ingest"}, false, "")

	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
	assert.Empty(t, fake.gotScheduleName, "RPC must not be issued when reason is missing")
}

func TestCancel_BlankReasonExits2(t *testing.T) {
	fake := &fakeState{}

	stdout, _, exit := runCancel(t, fake, []string{"daily_ingest", ""}, false, "")

	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
	assert.Empty(t, fake.gotScheduleName, "RPC must not be issued when reason is blank")
}

func TestCancel_TooManyArgumentsExits2(t *testing.T) {
	fake := &fakeState{}

	stdout, _, exit := runCancel(t, fake, []string{"daily_ingest", "reason", "extra"}, false, "")

	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestCancel_FailedPreconditionExits4(t *testing.T) {
	fake := &fakeState{cancelErr: status.Error(codes.FailedPrecondition, "no active run for schedule \"daily_ingest\"")}

	stdout, _, exit := runCancel(t, fake, []string{"daily_ingest", "x"}, false, "")

	assert.Equal(t, 4, exit)
	var envelope struct {
		Error output.CLIError `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &envelope))
	assert.Equal(t, output.CodeConflict, envelope.Error.Code)
	assert.True(t, envelope.Error.Retryable)
}

func TestCancel_UnavailableExits5(t *testing.T) {
	fake := &fakeState{cancelErr: status.Error(codes.Unavailable, "server down")}

	_, _, exit := runCancel(t, fake, []string{"daily_ingest", "x"}, false, "")

	assert.Equal(t, 5, exit)
}

func TestCancel_HumanModeUsesStderrAndEmptyStdout(t *testing.T) {
	fake := &fakeState{cancelResp: &statev1.CancelScheduleResponse{ScheduleId: "sched_123"}}

	stdout, stderr, exit := runCancel(t, fake, []string{"daily_ingest", "x"}, true, "")

	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "Cancelled run sched_123 for schedule 'daily_ingest'")
}
