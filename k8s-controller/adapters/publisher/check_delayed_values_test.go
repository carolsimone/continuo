package publisher

import (
	"encoding/json"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/k8s-controller/domain/event"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestToValues_CheckDelayed_EmitsPayloadAndFlatCheckAfter verifies a check_delayed
// outbox row is published to check.k8s:v1 as a typed JSON payload, with the
// scheduling timestamp (check_after) kept as a flat sibling field so the binding
// can gate re-delivery before decoding the payload.
func TestToValues_CheckDelayed_EmitsPayloadAndFlatCheckAfter(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	p := NewOutboxPublisher(nil, logger)

	taskID := uuid.New().String()
	scheduleID := uuid.New().String()
	raw, err := json.Marshal(event.JobCheckRequest{
		TaskID:       taskID,
		ScheduleID:   scheduleID,
		ScheduleName: "daily",
		ServiceName:  "svc",
		SchemaName:   "public",
		TableName:    "orders",
		JobName:      "job-1",
		CheckAfter:   12345,
		NodeType:     "dbt-model",
		ImageTag:     "sha-abc",
		RetryCount:   2,
		MaxRetries:   5,
	})
	require.NoError(t, err)

	vals, err := p.toValues(&outbox.Entry{EventType: "check_delayed", Payload: raw})
	require.NoError(t, err)

	// check_after stays a flat field for the consumer-side delay gate.
	assert.Equal(t, "12345", vals["check_after"])
	// Business fields move into the typed JSON payload, not flat keys.
	_, hasFlatTaskID := vals["task_id"]
	assert.False(t, hasFlatTaskID, "task_id must not be a flat field anymore")

	payloadStr, ok := vals["payload"].(string)
	require.True(t, ok, "expected a string payload field")
	var ck pkgevents.CheckK8s
	require.NoError(t, json.Unmarshal([]byte(payloadStr), &ck))
	assert.Equal(t, pkgevents.CheckK8s{
		TaskID:       taskID,
		ScheduleID:   scheduleID,
		ScheduleName: "daily",
		ServiceName:  "svc",
		SchemaName:   "public",
		TableName:    "orders",
		JobName:      "job-1",
		NodeType:     "dbt-model",
		ImageTag:     "sha-abc",
		RetryCount:   2,
		MaxRetries:   5,
	}, ck)
}
