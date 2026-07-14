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

// runBuild invokes the build command end-to-end with the provided fake client and args.
func runBuild(t *testing.T, fake client.StateClient, args []string, human bool) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human}
	cmd := NewBuildCommand(func(_ context.Context, _ string) (client.StateClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
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

func TestScheduleBuild_SuccessEmitsJSON(t *testing.T) {
	fake := &fakeState{buildResp: &statev1.TriggerScheduleResponse{ScheduleId: "sched_456"}}

	stdout, stderr, exit := runBuild(t, fake, []string{"daily_ingest"}, false)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stderr)
	var payload map[string]string
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	assert.Equal(t, "sched_456", payload["schedule_id"])
	assert.Equal(t, "daily_ingest", payload["schedule_name"])
	_, err := time.Parse(time.RFC3339, payload["triggered_at"])
	assert.NoError(t, err, "triggered_at must be RFC3339")
	assert.Equal(t, "daily_ingest", fake.gotBuildScheduleName)
}

func TestScheduleBuild_NotFoundExits3(t *testing.T) {
	fake := &fakeState{buildErr: status.Error(codes.NotFound, "schedule 'missing' not found")}

	stdout, _, exit := runBuild(t, fake, []string{"missing"}, false)

	assert.Equal(t, 3, exit)
	var envelope struct {
		Error output.CLIError `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &envelope))
	assert.Equal(t, output.CodeNotFound, envelope.Error.Code)
}

func TestScheduleBuild_ConflictExits4(t *testing.T) {
	fake := &fakeState{buildErr: status.Error(codes.FailedPrecondition, "a run is already active")}

	_, _, exit := runBuild(t, fake, []string{"daily_ingest"}, false)

	assert.Equal(t, 4, exit)
}

func TestScheduleBuild_UnavailableExits5(t *testing.T) {
	fake := &fakeState{buildErr: status.Error(codes.Unavailable, "server down")}

	_, _, exit := runBuild(t, fake, []string{"daily_ingest"}, false)

	assert.Equal(t, 5, exit)
}

func TestScheduleBuild_MissingArgumentExits2(t *testing.T) {
	fake := &fakeState{}

	stdout, _, exit := runBuild(t, fake, []string{}, false)

	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestScheduleBuild_TooManyArgumentsExits2(t *testing.T) {
	fake := &fakeState{}

	_, _, exit := runBuild(t, fake, []string{"daily_ingest", "extra"}, false)

	assert.Equal(t, 2, exit)
}

func TestScheduleBuild_HumanModeUsesStderrAndEmptyStdout(t *testing.T) {
	fake := &fakeState{buildResp: &statev1.TriggerScheduleResponse{ScheduleId: "sched_456"}}

	stdout, stderr, exit := runBuild(t, fake, []string{"daily_ingest"}, true)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "sched_456")
}
