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
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func runNodeRun(t *testing.T, fake client.RunClient, cfg *config.Config, args []string) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cmd := NewRunCommand(func(context.Context, string) (client.RunClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
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

func TestNodeRun_SuccessEmitsJSON(t *testing.T) {
	fake := &fakeNodeRunController{runResp: &runv1.StartSingleNodeRunResponse{RunId: "run_9", ScheduleName: "single-node-run-abcd1234"}}
	cfg := &config.Config{Timeout: 2 * time.Second}

	stdout, stderr, exit := runNodeRun(t, fake, cfg, []string{"finance", "analytics", "orders"})

	assert.Equal(t, 0, exit)
	assert.Empty(t, stderr)
	var payload map[string]string
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	assert.Equal(t, "run_9", payload["run_id"])
	assert.Equal(t, "single-node-run-abcd1234", payload["schedule_name"])
	assert.Equal(t, "finance", fake.gotRunProject)
}

func TestNodeRun_ForwardsActorFromConfig(t *testing.T) {
	fake := &fakeNodeRunController{runResp: &runv1.StartSingleNodeRunResponse{RunId: "run_9"}}
	cfg := &config.Config{Timeout: 2 * time.Second, Actor: "agent-chat-llm"}

	_, _, exit := runNodeRun(t, fake, cfg, []string{"finance", "analytics", "orders"})

	assert.Equal(t, 0, exit)
	assert.Equal(t, "agent-chat-llm", fake.gotActor)
}

func TestNodeRun_EmptyActorWhenUnset(t *testing.T) {
	fake := &fakeNodeRunController{runResp: &runv1.StartSingleNodeRunResponse{RunId: "run_9"}}
	cfg := &config.Config{Timeout: 2 * time.Second}

	_, _, _ = runNodeRun(t, fake, cfg, []string{"finance", "analytics", "orders"})

	assert.Equal(t, "", fake.gotActor)
}

func TestNodeRun_WrongArgCountExits2(t *testing.T) {
	fake := &fakeNodeRunController{}
	cfg := &config.Config{Timeout: 2 * time.Second}

	stdout, _, exit := runNodeRun(t, fake, cfg, []string{"finance", "analytics"})

	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestNodeRun_InvalidArgumentExits2(t *testing.T) {
	fake := &fakeNodeRunController{runErr: status.Error(codes.InvalidArgument, "project_name required")}
	cfg := &config.Config{Timeout: 2 * time.Second}

	_, _, exit := runNodeRun(t, fake, cfg, []string{"finance", "analytics", "orders"})

	assert.Equal(t, 2, exit)
}

func TestNodeRun_UnavailableExits5(t *testing.T) {
	fake := &fakeNodeRunController{runErr: status.Error(codes.Unavailable, "server down")}
	cfg := &config.Config{Timeout: 2 * time.Second}

	_, _, exit := runNodeRun(t, fake, cfg, []string{"finance", "analytics", "orders"})

	assert.Equal(t, 5, exit)
}

func TestNodeRun_HumanModeUsesStderr(t *testing.T) {
	fake := &fakeNodeRunController{runResp: &runv1.StartSingleNodeRunResponse{RunId: "run_9"}}
	cfg := &config.Config{Timeout: 2 * time.Second, Human: true}

	stdout, stderr, exit := runNodeRun(t, fake, cfg, []string{"finance", "analytics", "orders"})

	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "run_9")
	assert.Contains(t, stderr, "finance.analytics.orders")
}

const snapshotSourceRun = "3f9e1c2a-7b4d-4e8f-9a1b-2c3d4e5f6a7b"

func TestNodeRun_ThreeArgsSelectLatestMetadata(t *testing.T) {
	fake := &fakeNodeRunController{runResp: &runv1.StartSingleNodeRunResponse{RunId: "run_9"}}
	cfg := &config.Config{Timeout: 2 * time.Second}

	_, _, exit := runNodeRun(t, fake, cfg, []string{"finance", "analytics", "orders"})

	assert.Equal(t, 0, exit)
	assert.Equal(t, "", fake.gotRunSourceRun)
}

func TestNodeRun_FourthArgIsTheSnapshotSourceRun(t *testing.T) {
	fake := &fakeNodeRunController{runResp: &runv1.StartSingleNodeRunResponse{RunId: "run_9"}}
	cfg := &config.Config{Timeout: 2 * time.Second}

	_, _, exit := runNodeRun(t, fake, cfg, []string{"finance", "analytics", "orders", snapshotSourceRun})

	assert.Equal(t, 0, exit)
	assert.Equal(t, snapshotSourceRun, fake.gotRunSourceRun)
}

func TestNodeRun_FiveArgsExits2(t *testing.T) {
	fake := &fakeNodeRunController{}
	cfg := &config.Config{Timeout: 2 * time.Second}

	stdout, _, exit := runNodeRun(t, fake, cfg, []string{"finance", "analytics", "orders", snapshotSourceRun, "extra"})

	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
	assert.Equal(t, "", fake.gotRunSourceRun)
}

func TestNodeRun_SourceRunNotFoundExits3(t *testing.T) {
	fake := &fakeNodeRunController{runErr: status.Error(codes.NotFound, "source run not found")}
	cfg := &config.Config{Timeout: 2 * time.Second}

	_, _, exit := runNodeRun(t, fake, cfg, []string{"finance", "analytics", "orders", snapshotSourceRun})

	assert.Equal(t, 3, exit)
}

func TestNodeRun_SourceRunNotTerminalExits4(t *testing.T) {
	fake := &fakeNodeRunController{runErr: status.Error(codes.FailedPrecondition, "source run is not terminal")}
	cfg := &config.Config{Timeout: 2 * time.Second}

	_, _, exit := runNodeRun(t, fake, cfg, []string{"finance", "analytics", "orders", snapshotSourceRun})

	assert.Equal(t, 4, exit)
}

// An explicitly empty fourth argument (a shell variable that was never set)
// is a usage error, not a silent fall-back to latest metadata: the caller
// asked for a snapshot and must not get a run against current code instead.
func TestNodeRun_EmptySourceRunArgExits2WithoutCalling(t *testing.T) {
	for _, empty := range []string{"", "   "} {
		fake := &fakeNodeRunController{}
		cfg := &config.Config{Timeout: 2 * time.Second}

		stdout, _, exit := runNodeRun(t, fake, cfg, []string{"finance", "analytics", "orders", empty})

		assert.Equal(t, 2, exit, "arg %q", empty)
		var env map[string]output.CLIError
		require.NoError(t, json.Unmarshal([]byte(stdout), &env))
		assert.Equal(t, output.CodeUsage, env["error"].Code)
		assert.Equal(t, "", fake.gotRunProject, "run-controller must not be called")
	}
}

// Every command that starts a single-node run hands the client the request id
// of the invocation, which CONTINUO_REQUEST_ID fixes, so the client can send it
// on every try and the server can recognise a repeat.
func TestNodeRunCommands_SendTheRequestIDOfTheInvocation(t *testing.T) {
	const id = "5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b"
	t.Setenv("CONTINUO_REQUEST_ID", id)
	resp := &runv1.StartSingleNodeRunResponse{RunId: "run_9"}
	cfg := &config.Config{Timeout: 2 * time.Second}
	args := []string{"finance", "analytics", "orders"}

	cases := map[string]func(*fakeNodeRunController) int{
		"run": func(f *fakeNodeRunController) int {
			f.runResp = resp
			_, _, e := runNodeRun(t, f, cfg, args)
			return e
		},
		"test":  func(f *fakeNodeRunController) int { f.testResp = resp; _, _, e := runTest(t, f, cfg, args); return e },
		"build": func(f *fakeNodeRunController) int { f.buildResp = resp; _, _, e := runBuild(t, f, cfg, args); return e },
		"full-refresh": func(f *fakeNodeRunController) int {
			f.fullRefreshResp = resp
			_, _, e := runFullRefresh(t, f, cfg, args)
			return e
		},
	}
	for name, invoke := range cases {
		t.Run(name, func(t *testing.T) {
			fake := &fakeNodeRunController{}
			assert.Equal(t, 0, invoke(fake))
			assert.Equal(t, []string{id}, fake.gotRequestIDs)
		})
	}
}
