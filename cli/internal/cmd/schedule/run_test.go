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

type fakeRunController struct {
	client.RunClient

	resp            *runv1.StartRunResponse
	err             error
	gotScheduleName string
	gotStartActor   string
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

	// delay holds every start command for that long after it is received,
	// and calledAt is when the last one was received.
	delay    time.Duration
	calledAt time.Time
}

// received records when a start command arrived and holds it for f.delay.
func (f *fakeRunController) received() {
	f.calledAt = time.Now()
	time.Sleep(f.delay)
}

func (f *fakeRunController) StartScheduleRun(_ context.Context, requestID, name, actor string) (*runv1.StartRunResponse, error) {
	f.received()
	f.gotRequestIDs = append(f.gotRequestIDs, requestID)
	f.gotScheduleName = name
	f.gotStartActor = actor
	return f.resp, f.err
}

func (f *fakeRunController) StartScheduleTest(_ context.Context, requestID, name, actor string) (*runv1.StartRunResponse, error) {
	f.received()
	f.gotRequestIDs = append(f.gotRequestIDs, requestID)
	f.gotTestScheduleName = name
	f.gotTestActor = actor
	return f.testResp, f.testErr
}

func (f *fakeRunController) StartScheduleBuild(_ context.Context, requestID, name, actor string) (*runv1.StartRunResponse, error) {
	f.received()
	f.gotRequestIDs = append(f.gotRequestIDs, requestID)
	f.gotBuildScheduleName = name
	f.gotBuildActor = actor
	return f.buildResp, f.buildErr
}

func (f *fakeRunController) CancelSchedule(_ context.Context, requestID, name, reason, by string) (*runv1.CancelActiveRunResponse, error) {
	f.gotRequestIDs = append(f.gotRequestIDs, requestID)
	f.gotScheduleName = name
	f.gotCancelReason = reason
	f.gotCancelBy = by
	return f.cancelResp, f.cancelErr
}

func (f *fakeRunController) Close() error { return nil }

// run invokes the schedule run command end-to-end with the provided fake client and args.
// It captures stdout/stderr and returns the exit code.
func run(t *testing.T, fake client.RunClient, args []string, human bool) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human}
	cmd := NewRunCommand(func(_ context.Context, _ string) (client.RunClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
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

// runScheduleRunWithActor invokes the schedule run command with the resolved actor
// (config.Config.Actor, sourced from CONTINUO_ACTOR) and returns the exit code.
func runScheduleRunWithActor(t *testing.T, fake client.RunClient, args []string, actor string) int {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Actor: actor}
	cmd := NewRunCommand(func(_ context.Context, _ string) (client.RunClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
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

func TestScheduleRun_ForwardsActor(t *testing.T) {
	fake := &fakeRunController{resp: &runv1.StartRunResponse{RunId: "s1"}}
	exit := runScheduleRunWithActor(t, fake, []string{"daily"}, "okta.example.com|alice")
	assert.Equal(t, 0, exit)
	assert.Equal(t, "okta.example.com|alice", fake.gotStartActor)
}

func TestScheduleRun_SuccessEmitsJSON(t *testing.T) {
	fake := &fakeRunController{resp: &runv1.StartRunResponse{RunId: "sched_123"}}

	stdout, stderr, exit := run(t, fake, []string{"daily_ingest"}, false)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stderr)
	var payload map[string]string
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	assert.Equal(t, "sched_123", payload["run_id"])
	assert.Equal(t, "daily_ingest", payload["schedule_name"])
	_, err := time.Parse(time.RFC3339, payload["requested_at"])
	assert.NoError(t, err, "requested_at must be RFC3339")
	assert.Equal(t, "daily_ingest", fake.gotScheduleName)
}

// requested_at is when the command asked for the run: a run-controller that
// takes longer than a second to answer does not move it past the moment the
// command was sent.
func TestScheduleStartCommands_RequestedAtIsWhenTheCommandAsked(t *testing.T) {
	ok := &runv1.StartRunResponse{RunId: "r1"}
	for name, start := range map[string]func(t *testing.T, fake client.RunClient, args []string, human bool) (string, string, int){
		"run":   run,
		"test":  runTest,
		"build": runBuild,
	} {
		t.Run(name, func(t *testing.T) {
			fake := &fakeRunController{resp: ok, testResp: ok, buildResp: ok, delay: 1100 * time.Millisecond}
			stdout, _, exit := start(t, fake, []string{"daily_ingest"}, false)
			require.Equal(t, 0, exit)
			var payload map[string]string
			require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
			requestedAt, err := time.Parse(time.RFC3339, payload["requested_at"])
			require.NoError(t, err)
			assert.False(t, requestedAt.After(fake.calledAt.Truncate(time.Second)),
				"requested_at %s is after the command reached run-controller at %s", requestedAt, fake.calledAt)
		})
	}
}

func TestScheduleRun_NotFoundExits3(t *testing.T) {
	fake := &fakeRunController{err: status.Error(codes.NotFound, "schedule 'missing' not found")}

	stdout, _, exit := run(t, fake, []string{"missing"}, false)

	assert.Equal(t, 3, exit)
	var envelope struct {
		Error output.CLIError `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &envelope))
	assert.Equal(t, output.CodeNotFound, envelope.Error.Code)
	assert.False(t, envelope.Error.Retryable)
}

func TestScheduleRun_FailedPreconditionExits4(t *testing.T) {
	fake := &fakeRunController{err: status.Error(codes.FailedPrecondition, "already running")}

	stdout, _, exit := run(t, fake, []string{"daily_ingest"}, false)

	assert.Equal(t, 4, exit)
	var envelope struct {
		Error output.CLIError `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &envelope))
	assert.Equal(t, output.CodeConflict, envelope.Error.Code)
	assert.True(t, envelope.Error.Retryable)
}

func TestScheduleRun_UnavailableExits5(t *testing.T) {
	fake := &fakeRunController{err: status.Error(codes.Unavailable, "server down")}

	_, _, exit := run(t, fake, []string{"daily_ingest"}, false)

	assert.Equal(t, 5, exit)
}

func TestScheduleRun_MissingArgumentExits2(t *testing.T) {
	fake := &fakeRunController{}

	stdout, _, exit := run(t, fake, []string{}, false)

	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestScheduleRun_HumanModeUsesStderrAndEmptyStdout(t *testing.T) {
	fake := &fakeRunController{resp: &runv1.StartRunResponse{RunId: "sched_123"}}

	stdout, stderr, exit := run(t, fake, []string{"daily_ingest"}, true)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "Started manual run sched_123 of schedule 'daily_ingest'")
}

const fixedRequestID = "5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b"

// A command that starts a run, or cancels a schedule's active run, hands the
// client the request id of the invocation, which CONTINUO_REQUEST_ID fixes, so
// the client can send it on every try and the server can recognise a repeat.
func TestScheduleCommands_SendTheRequestIDOfTheInvocation(t *testing.T) {
	t.Setenv("CONTINUO_REQUEST_ID", fixedRequestID)
	ok := &runv1.StartRunResponse{RunId: "r1"}
	cases := map[string]func(fake *fakeRunController) (int, []string){
		"run": func(fake *fakeRunController) (int, []string) {
			fake.resp = ok
			_, _, exit := run(t, fake, []string{"daily"}, false)
			return exit, fake.gotRequestIDs
		},
		"test": func(fake *fakeRunController) (int, []string) {
			fake.testResp = ok
			_, _, exit := runTest(t, fake, []string{"daily"}, false)
			return exit, fake.gotRequestIDs
		},
		"build": func(fake *fakeRunController) (int, []string) {
			fake.buildResp = ok
			_, _, exit := runBuild(t, fake, []string{"daily"}, false)
			return exit, fake.gotRequestIDs
		},
		"cancel": func(fake *fakeRunController) (int, []string) {
			fake.cancelResp = &runv1.CancelActiveRunResponse{RunId: "r1"}
			_, _, exit := runCancel(t, fake, []string{"daily", "why"}, false, "")
			return exit, fake.gotRequestIDs
		},
	}
	for name, invoke := range cases {
		t.Run(name, func(t *testing.T) {
			exit, ids := invoke(&fakeRunController{})
			assert.Equal(t, 0, exit)
			assert.Equal(t, []string{fixedRequestID}, ids)
		})
	}
}
