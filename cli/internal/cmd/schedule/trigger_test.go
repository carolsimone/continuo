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
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeState struct {
	client.RunClient

	resp            *runv1.StartRunResponse
	err             error
	gotScheduleName string
	gotTriggerActor string
	// gotRequestIDs is the request id of every command the fake received.
	gotRequestIDs []string

	testResp            *runv1.StartRunResponse
	testErr             error
	gotTestScheduleName string
	gotTestActor        string

	cancelResp      *runv1.CancelActiveRunResponse
	cancelErr       error
	gotCancelReason string
	gotCancelBy     string

	buildResp            *runv1.StartRunResponse
	buildErr             error
	gotBuildScheduleName string
	gotBuildActor        string
}

func (f *fakeState) TriggerSchedule(_ context.Context, requestID, name, actor string) (*runv1.StartRunResponse, error) {
	f.gotRequestIDs = append(f.gotRequestIDs, requestID)
	f.gotScheduleName = name
	f.gotTriggerActor = actor
	return f.resp, f.err
}

func (f *fakeState) TriggerScheduleTest(_ context.Context, requestID, name, actor string) (*runv1.StartRunResponse, error) {
	f.gotRequestIDs = append(f.gotRequestIDs, requestID)
	f.gotTestScheduleName = name
	f.gotTestActor = actor
	return f.testResp, f.testErr
}

func (f *fakeState) TriggerScheduleBuild(_ context.Context, requestID, name, actor string) (*runv1.StartRunResponse, error) {
	f.gotRequestIDs = append(f.gotRequestIDs, requestID)
	f.gotBuildScheduleName = name
	f.gotBuildActor = actor
	return f.buildResp, f.buildErr
}

func (f *fakeState) CancelSchedule(_ context.Context, requestID, name, reason, by string) (*runv1.CancelActiveRunResponse, error) {
	f.gotRequestIDs = append(f.gotRequestIDs, requestID)
	f.gotScheduleName = name
	f.gotCancelReason = reason
	f.gotCancelBy = by
	return f.cancelResp, f.cancelErr
}

func (f *fakeState) Close() error { return nil }

// run invokes the trigger command end-to-end with the provided fake client and args.
// It captures stdout/stderr and returns the exit code.
func run(t *testing.T, fake client.RunClient, args []string, human bool) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human}
	cmd := NewTriggerCommand(func(_ context.Context, _ string) (client.RunClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
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

// runTriggerWithActor invokes the trigger command with the resolved actor
// (config.Config.Actor, sourced from CONTINUO_ACTOR) and returns the exit code.
func runTriggerWithActor(t *testing.T, fake client.RunClient, args []string, actor string) int {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Actor: actor}
	cmd := NewTriggerCommand(func(_ context.Context, _ string) (client.RunClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
	cmd.SetArgs(args)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	if err := cmd.Execute(); err != nil {
		var cliErr output.CLIError
		if errors.As(err, &cliErr) {
			return cliErr.ExitCode()
		}
		return 1
	}
	return 0
}

func TestTrigger_ForwardsActor(t *testing.T) {
	fake := &fakeState{resp: &runv1.StartRunResponse{RunId: "s1"}}
	exit := runTriggerWithActor(t, fake, []string{"daily"}, "okta.example.com|alice")
	assert.Equal(t, 0, exit)
	assert.Equal(t, "okta.example.com|alice", fake.gotTriggerActor)
}

func TestTrigger_SuccessEmitsJSON(t *testing.T) {
	fake := &fakeState{resp: &runv1.StartRunResponse{RunId: "sched_123"}}

	stdout, stderr, exit := run(t, fake, []string{"daily_ingest"}, false)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stderr)
	var payload map[string]string
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	assert.Equal(t, "sched_123", payload["run_id"])
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
	fake := &fakeState{resp: &runv1.StartRunResponse{RunId: "sched_123"}}

	stdout, stderr, exit := run(t, fake, []string{"daily_ingest"}, true)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "Triggered run sched_123 for schedule 'daily_ingest'")
}

const fixedRequestID = "5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b"

// A command that starts a run, or cancels a schedule's active run, hands the
// client the request id of the invocation, which CONTINUO_REQUEST_ID fixes, so
// the client can send it on every try and the server can recognise a repeat.
func TestScheduleCommands_SendTheRequestIDOfTheInvocation(t *testing.T) {
	t.Setenv("CONTINUO_REQUEST_ID", fixedRequestID)
	ok := &runv1.StartRunResponse{RunId: "r1"}
	cases := map[string]func(fake *fakeState) (int, []string){
		"trigger": func(fake *fakeState) (int, []string) {
			fake.resp = ok
			_, _, exit := run(t, fake, []string{"daily"}, false)
			return exit, fake.gotRequestIDs
		},
		"test": func(fake *fakeState) (int, []string) {
			fake.testResp = ok
			_, _, exit := runTest(t, fake, []string{"daily"}, false)
			return exit, fake.gotRequestIDs
		},
		"build": func(fake *fakeState) (int, []string) {
			fake.buildResp = ok
			_, _, exit := runBuild(t, fake, []string{"daily"}, false)
			return exit, fake.gotRequestIDs
		},
		"cancel": func(fake *fakeState) (int, []string) {
			fake.cancelResp = &runv1.CancelActiveRunResponse{RunId: "r1"}
			_, _, exit := runCancel(t, fake, []string{"daily", "why"}, false, "")
			return exit, fake.gotRequestIDs
		},
	}
	for name, invoke := range cases {
		t.Run(name, func(t *testing.T) {
			exit, ids := invoke(&fakeState{})
			assert.Equal(t, 0, exit)
			assert.Equal(t, []string{fixedRequestID}, ids)
		})
	}
}
