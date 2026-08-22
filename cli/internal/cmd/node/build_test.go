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

func runBuild(t *testing.T, fake client.StateClient, cfg *config.Config, args []string) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cmd := NewBuildCommand(func(context.Context, string) (client.StateClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
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

func TestBuildNode_SuccessEmitsJSON(t *testing.T) {
	fake := &fakeNodeState{buildResp: &statev1.TriggerSingleNodeRunResponse{RunId: "run_9", ScheduleName: "single-node-run-abcd1234"}}
	cfg := &config.Config{Timeout: 2 * time.Second}

	stdout, stderr, exit := runBuild(t, fake, cfg, []string{"finance", "analytics", "orders"})

	assert.Equal(t, 0, exit)
	assert.Empty(t, stderr)
	var payload map[string]string
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	assert.Equal(t, "run_9", payload["run_id"])
	assert.Equal(t, "single-node-run-abcd1234", payload["schedule_name"])
	assert.Equal(t, "finance", fake.gotBuildSvc)
	assert.Equal(t, "analytics", fake.gotBuildSchema)
	assert.Equal(t, "orders", fake.gotBuildTable)
}

func TestBuildNode_ForwardsActorFromConfig(t *testing.T) {
	fake := &fakeNodeState{buildResp: &statev1.TriggerSingleNodeRunResponse{RunId: "run_9"}}
	cfg := &config.Config{Timeout: 2 * time.Second, Actor: "agent-chat-llm"}

	_, _, exit := runBuild(t, fake, cfg, []string{"finance", "analytics", "orders"})

	assert.Equal(t, 0, exit)
	assert.Equal(t, "agent-chat-llm", fake.gotBuildActor)
}

func TestBuildNode_EmptyActorWhenUnset(t *testing.T) {
	fake := &fakeNodeState{buildResp: &statev1.TriggerSingleNodeRunResponse{RunId: "run_9"}}
	cfg := &config.Config{Timeout: 2 * time.Second}

	_, _, _ = runBuild(t, fake, cfg, []string{"finance", "analytics", "orders"})

	assert.Equal(t, "", fake.gotBuildActor)
}

func TestBuildNode_WrongArgCountExits2(t *testing.T) {
	fake := &fakeNodeState{}
	cfg := &config.Config{Timeout: 2 * time.Second}

	stdout, _, exit := runBuild(t, fake, cfg, []string{"finance", "analytics"})

	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestBuildNode_InvalidArgumentExits2(t *testing.T) {
	fake := &fakeNodeState{buildErr: status.Error(codes.InvalidArgument, "node not in topology")}
	cfg := &config.Config{Timeout: 2 * time.Second}

	_, _, exit := runBuild(t, fake, cfg, []string{"finance", "analytics", "orders"})

	assert.Equal(t, 2, exit)
}

func TestBuildNode_UnavailableExits5(t *testing.T) {
	fake := &fakeNodeState{buildErr: status.Error(codes.Unavailable, "server down")}
	cfg := &config.Config{Timeout: 2 * time.Second}

	_, _, exit := runBuild(t, fake, cfg, []string{"finance", "analytics", "orders"})

	assert.Equal(t, 5, exit)
}

func TestBuildNode_HumanModeUsesStderr(t *testing.T) {
	fake := &fakeNodeState{buildResp: &statev1.TriggerSingleNodeRunResponse{RunId: "run_9"}}
	cfg := &config.Config{Timeout: 2 * time.Second, Human: true}

	stdout, stderr, exit := runBuild(t, fake, cfg, []string{"finance", "analytics", "orders"})

	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "run_9")
	assert.Contains(t, stderr, "finance.analytics.orders")
}
