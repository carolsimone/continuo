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

func runTest(t *testing.T, fake client.StateClient, cfg *config.Config, args []string) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cmd := NewTestCommand(func(context.Context, string) (client.StateClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
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

func TestTestNode_SuccessEmitsJSON(t *testing.T) {
	fake := &fakeNodeState{testResp: &statev1.TriggerSingleNodeRunResponse{RunId: "run_9", ScheduleName: "single-node-run-abcd1234"}}
	cfg := &config.Config{Timeout: 2 * time.Second}

	stdout, stderr, exit := runTest(t, fake, cfg, []string{"finance", "analytics", "orders"})

	assert.Equal(t, 0, exit)
	assert.Empty(t, stderr)
	var payload map[string]string
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	assert.Equal(t, "run_9", payload["run_id"])
	assert.Equal(t, "single-node-run-abcd1234", payload["schedule_name"])
	assert.Equal(t, "finance", fake.gotTestSvc)
	assert.Equal(t, "analytics", fake.gotTestSchema)
	assert.Equal(t, "orders", fake.gotTestTable)
}

func TestTestNode_ForwardsActorFromConfig(t *testing.T) {
	fake := &fakeNodeState{testResp: &statev1.TriggerSingleNodeRunResponse{RunId: "run_9"}}
	cfg := &config.Config{Timeout: 2 * time.Second, Actor: "agent-chat-llm"}

	_, _, exit := runTest(t, fake, cfg, []string{"finance", "analytics", "orders"})

	assert.Equal(t, 0, exit)
	assert.Equal(t, "agent-chat-llm", fake.gotTestActor)
}

func TestTestNode_EmptyActorWhenUnset(t *testing.T) {
	fake := &fakeNodeState{testResp: &statev1.TriggerSingleNodeRunResponse{RunId: "run_9"}}
	cfg := &config.Config{Timeout: 2 * time.Second}

	_, _, _ = runTest(t, fake, cfg, []string{"finance", "analytics", "orders"})

	assert.Equal(t, "", fake.gotTestActor)
}

func TestTestNode_WrongArgCountExits2(t *testing.T) {
	fake := &fakeNodeState{}
	cfg := &config.Config{Timeout: 2 * time.Second}

	stdout, _, exit := runTest(t, fake, cfg, []string{"finance", "analytics"})

	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestTestNode_InvalidArgumentExits2(t *testing.T) {
	fake := &fakeNodeState{testErr: status.Error(codes.InvalidArgument, "no tests defined for node")}
	cfg := &config.Config{Timeout: 2 * time.Second}

	_, _, exit := runTest(t, fake, cfg, []string{"finance", "analytics", "orders"})

	assert.Equal(t, 2, exit)
}

func TestTestNode_UnavailableExits5(t *testing.T) {
	fake := &fakeNodeState{testErr: status.Error(codes.Unavailable, "server down")}
	cfg := &config.Config{Timeout: 2 * time.Second}

	_, _, exit := runTest(t, fake, cfg, []string{"finance", "analytics", "orders"})

	assert.Equal(t, 5, exit)
}

func TestTestNode_HumanModeUsesStderr(t *testing.T) {
	fake := &fakeNodeState{testResp: &statev1.TriggerSingleNodeRunResponse{RunId: "run_9"}}
	cfg := &config.Config{Timeout: 2 * time.Second, Human: true}

	stdout, stderr, exit := runTest(t, fake, cfg, []string{"finance", "analytics", "orders"})

	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "run_9")
	assert.Contains(t, stderr, "finance.analytics.orders")
}

func TestTestNode_ThreeArgsSelectLatestMetadata(t *testing.T) {
	fake := &fakeNodeState{testResp: &statev1.TriggerSingleNodeRunResponse{RunId: "run_9"}}
	cfg := &config.Config{Timeout: 2 * time.Second}

	_, _, exit := runTest(t, fake, cfg, []string{"finance", "analytics", "orders"})

	assert.Equal(t, 0, exit)
	assert.Equal(t, "", fake.gotTestSourceRun)
}

func TestTestNode_FourthArgIsTheSnapshotSourceRun(t *testing.T) {
	fake := &fakeNodeState{testResp: &statev1.TriggerSingleNodeRunResponse{RunId: "run_9"}}
	cfg := &config.Config{Timeout: 2 * time.Second}

	_, _, exit := runTest(t, fake, cfg, []string{"finance", "analytics", "orders", snapshotSourceRun})

	assert.Equal(t, 0, exit)
	assert.Equal(t, snapshotSourceRun, fake.gotTestSourceRun)
}

func TestTestNode_FiveArgsExits2(t *testing.T) {
	fake := &fakeNodeState{}
	cfg := &config.Config{Timeout: 2 * time.Second}

	stdout, _, exit := runTest(t, fake, cfg, []string{"finance", "analytics", "orders", snapshotSourceRun, "extra"})

	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
	assert.Equal(t, "", fake.gotTestSourceRun)
}

func TestTestNode_SourceRunNotFoundExits3(t *testing.T) {
	fake := &fakeNodeState{testErr: status.Error(codes.NotFound, "source run not found")}
	cfg := &config.Config{Timeout: 2 * time.Second}

	_, _, exit := runTest(t, fake, cfg, []string{"finance", "analytics", "orders", snapshotSourceRun})

	assert.Equal(t, 3, exit)
}

func TestTestNode_SourceRunNotTerminalExits4(t *testing.T) {
	fake := &fakeNodeState{testErr: status.Error(codes.FailedPrecondition, "source run is not terminal")}
	cfg := &config.Config{Timeout: 2 * time.Second}

	_, _, exit := runTest(t, fake, cfg, []string{"finance", "analytics", "orders", snapshotSourceRun})

	assert.Equal(t, 4, exit)
}

// An explicitly empty fourth argument (a shell variable that was never set)
// is a usage error, not a silent fall-back to latest metadata: the caller
// asked for a snapshot and must not get a run against current code instead.
func TestTestNode_EmptySourceRunArgExits2WithoutCalling(t *testing.T) {
	for _, empty := range []string{"", "   "} {
		fake := &fakeNodeState{}
		cfg := &config.Config{Timeout: 2 * time.Second}

		stdout, _, exit := runTest(t, fake, cfg, []string{"finance", "analytics", "orders", empty})

		assert.Equal(t, 2, exit, "arg %q", empty)
		var env map[string]output.CLIError
		require.NoError(t, json.Unmarshal([]byte(stdout), &env))
		assert.Equal(t, output.CodeUsage, env["error"].Code)
		assert.Equal(t, "", fake.gotTestSvc, "the state service must not be called")
	}
}
