package redis

import (
	"encoding/json"
	"testing"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// terminalMsg wraps a typed capacity notification as the JSON "payload" field
// of a Redis message, matching the shape k8s-controller's publisher emits.
func terminalMsg(t *testing.T, e pkgevents.ExecutorJobTerminal, extra map[string]interface{}) goredis.XMessage {
	t.Helper()
	b, err := json.Marshal(e)
	require.NoError(t, err)
	values := map[string]interface{}{"payload": string(b)}
	for k, v := range extra {
		values[k] = v
	}
	return goredis.XMessage{ID: "1-0", Values: values}
}

func TestParseExecutorJobTerminal_DecodesPayload(t *testing.T) {
	depID := uuid.New()
	outboxID := uuid.New()
	evt, err := ParseExecutorJobTerminal(terminalMsg(t, pkgevents.ExecutorJobTerminal{
		ExecutorDeploymentID: depID.String(),
		JobName:              "job-1",
		TerminalStatus:       "succeeded",
		CompletedAt:          "2026-07-16T10:00:00Z",
	}, map[string]interface{}{"outbox_entry_id": outboxID.String()}))
	require.NoError(t, err)

	assert.Equal(t, depID, evt.ExecutorDeploymentID)
	assert.Equal(t, outboxID, evt.OutboxEntryID)
	assert.Equal(t, "job-1", evt.JobName)
	assert.Equal(t, "succeeded", evt.TerminalStatus)
	assert.Equal(t, time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC), evt.CompletedAt.UTC())
}

// TestParseExecutorJobTerminal_RejectsInvalidDeploymentID keeps a message that
// names no releasable row from being retried forever: it can never become valid.
func TestParseExecutorJobTerminal_RejectsInvalidDeploymentID(t *testing.T) {
	for _, id := range []string{"", "not-a-uuid"} {
		_, err := ParseExecutorJobTerminal(terminalMsg(t, pkgevents.ExecutorJobTerminal{
			ExecutorDeploymentID: id, JobName: "job-1", TerminalStatus: "succeeded",
		}, nil))
		assert.Error(t, err, "executor_deployment_id %q must be rejected", id)
	}
}

// TestParseExecutorJobTerminal_RejectsInvalidTerminalStatus guards the contract:
// only a status that genuinely settles the Job may release its slot.
func TestParseExecutorJobTerminal_RejectsInvalidTerminalStatus(t *testing.T) {
	for _, status := range []string{"", "running", "bogus"} {
		_, err := ParseExecutorJobTerminal(terminalMsg(t, pkgevents.ExecutorJobTerminal{
			ExecutorDeploymentID: uuid.New().String(), JobName: "job-1", TerminalStatus: status,
		}, nil))
		assert.Error(t, err, "terminal_status %q must be rejected", status)
	}
}

func TestParseExecutorJobTerminal_AcceptsEveryTerminalStatus(t *testing.T) {
	for _, status := range []string{"succeeded", "failed", "unknown"} {
		evt, err := ParseExecutorJobTerminal(terminalMsg(t, pkgevents.ExecutorJobTerminal{
			ExecutorDeploymentID: uuid.New().String(), JobName: "job-1", TerminalStatus: status,
		}, nil))
		require.NoError(t, err, "terminal_status %q settles a Job", status)
		assert.Equal(t, status, evt.TerminalStatus)
	}
}

// TestParseExecutorJobTerminal_AbsentCompletedAtIsZero keeps a Job that reported
// no completion instant releasable: the slot is freed regardless.
func TestParseExecutorJobTerminal_AbsentCompletedAtIsZero(t *testing.T) {
	evt, err := ParseExecutorJobTerminal(terminalMsg(t, pkgevents.ExecutorJobTerminal{
		ExecutorDeploymentID: uuid.New().String(), JobName: "job-1", TerminalStatus: "unknown",
	}, nil))
	require.NoError(t, err)
	assert.True(t, evt.CompletedAt.IsZero())
}

func TestParseExecutorJobTerminal_MissingPayloadErrors(t *testing.T) {
	_, err := ParseExecutorJobTerminal(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{}})
	assert.Error(t, err)
}

func TestParseExecutorJobTerminal_MalformedOutboxEntryIDErrors(t *testing.T) {
	_, err := ParseExecutorJobTerminal(terminalMsg(t, pkgevents.ExecutorJobTerminal{
		ExecutorDeploymentID: uuid.New().String(), JobName: "job-1", TerminalStatus: "succeeded",
	}, map[string]interface{}{"outbox_entry_id": "nope"}))
	assert.Error(t, err)
}
