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

func runFullRefresh(t *testing.T, fake client.StateClient, cfg *config.Config, args []string) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cmd := NewFullRefreshCommand(func(context.Context, string) (client.StateClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
	cmd.SetArgs(args)
	err := cmd.Execute()
	if err != nil {
		var cliErr output.CLIError
		if errors.As(err, &cliErr) {
			return outBuf.String(), errBuf.String(), cliErr.ExitCode()
		}
		return outBuf.String(), errBuf.String(), 1
	}
	return outBuf.String(), errBuf.String(), 0
}

func TestFullRefreshNode_SuccessEmitsJSON(t *testing.T) {
	fake := &fakeNodeState{fullRefreshResp: &statev1.TriggerSingleNodeRunResponse{RunId: "run_fr", ScheduleName: "single-node-run-00ff00ff"}}
	stdout, stderr, exit := runFullRefresh(t, fake, &config.Config{Timeout: 2 * time.Second, Actor: "agent-chat-llm"},
		[]string{"finance", "analytics", "orders", "3f9e1c2a-7b4d-4e8f-9a1b-2c3d4e5f6a7b"})

	assert.Equal(t, 0, exit)
	assert.Empty(t, stderr)
	var payload map[string]string
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	assert.Equal(t, map[string]string{"run_id": "run_fr", "schedule_name": "single-node-run-00ff00ff"}, payload)
	assert.Equal(t, []string{"finance", "analytics", "orders", "3f9e1c2a-7b4d-4e8f-9a1b-2c3d4e5f6a7b", "agent-chat-llm"},
		[]string{fake.gotFullRefreshSvc, fake.gotFullRefreshSchema, fake.gotFullRefreshTable, fake.gotFullRefreshSourceRun, fake.gotFullRefreshActor})
}

func TestFullRefreshNode_WrongArgCountIsUsage(t *testing.T) {
	_, _, exit := runFullRefresh(t, &fakeNodeState{}, &config.Config{Timeout: time.Second}, []string{"finance", "analytics"})
	assert.Equal(t, 2, exit)
}

func TestFullRefreshNode_ServerInvalidArgumentIsUsage(t *testing.T) {
	fake := &fakeNodeState{fullRefreshErr: status.Error(codes.InvalidArgument, "bad triple")}
	_, _, exit := runFullRefresh(t, fake, &config.Config{Timeout: time.Second}, []string{"finance", "analytics", "orders"})
	assert.Equal(t, 2, exit)
}

func TestFullRefreshNode_IsMutatingAndSelfDescribing(t *testing.T) {
	cmd := NewFullRefreshCommand(nil, &config.Config{}, &bytes.Buffer{}, &bytes.Buffer{})
	assert.Equal(t, "true", cmd.Annotations["mutating"])
	assert.NotEmpty(t, cmd.Long)
	assert.NotEmpty(t, cmd.Example)
	assert.JSONEq(t, `{"run_id":"string","schedule_name":"string"}`, cmd.Annotations["output_schema"])
}
