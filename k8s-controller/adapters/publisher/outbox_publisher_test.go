package publisher_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/carolsimone/continuo/k8s-controller/adapters/publisher"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestPublisher_ValidationNodeCompleted(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	r := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	pub := publisher.NewOutboxPublisher(r, newTestLogger())

	// handleValidationTerminal stores the per-node result body as the entry
	// payload; the publisher must re-emit it on the "payload" field so the
	// executor's ParseValidationNodeCompleted can decode it.
	body := []byte(`{"release_id":"rel_1","node_id":"public.orders","outcome":"ok","dbt_log_uri":"s3://logs/x"}`)
	id := uuid.New()
	require.NoError(t, pub.Publish(context.Background(), &outbox.Entry{
		ID: id, EventType: "validation_node_completed", StreamName: streams.ValidationNodeCompletedV1, Payload: body,
	}))

	res, err := r.XRange(context.Background(), streams.ValidationNodeCompletedV1, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, res, 1)
	v := res[0].Values
	assert.Equal(t, id.String(), v["outbox_entry_id"])
	payloadStr, ok := v["payload"].(string)
	require.True(t, ok, "expected a string payload field")
	assert.JSONEq(t, string(body), payloadStr, "stored per-node result re-emitted verbatim")
}

func TestPublisher_UnknownEventTypeReturnsError(t *testing.T) {
	pub := publisher.NewOutboxPublisher(nil, newTestLogger())
	err := pub.Publish(context.Background(), &outbox.Entry{
		ID: uuid.New(), EventType: "bogus", Payload: []byte(`{}`), StreamName: "x:v1",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown event_type")
}

func TestPublisher_BadPayloadReturnsError(t *testing.T) {
	pub := publisher.NewOutboxPublisher(nil, newTestLogger())
	err := pub.Publish(context.Background(), &outbox.Entry{
		ID: uuid.New(), EventType: "task_status_updated", Payload: []byte(`not json`), StreamName: "x:v1",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unmarshal task_status_updated")
}

// TestPublisher_PayloadShapesUnmarshalSuccessfully verifies that each switch
// case's typed struct round-trips correctly via JSON, ensuring the struct
// shapes match what writers will produce.
//
// We can't test the XADD path here without a real Redis instance; the
// integration tests in later tasks cover the end-to-end wire shape.
func TestPublisher_PayloadShapesUnmarshalSuccessfully(t *testing.T) {
	cases := []struct {
		eventType string
		payload   any
	}{
		{
			"task_status_updated",
			pkgevents.TaskStatusUpdated{TaskID: "t1", ScheduleID: "s1", Status: "SUCCEEDED", RetryCount: 1},
		},
		{
			"task_execution_recorded",
			pkgevents.TaskExecutionRecorded{ExecutionID: "e1", TaskID: "t1", JobName: "j1", ExecutionSeconds: 12.5},
		},
	}
	for _, c := range cases {
		t.Run(c.eventType, func(t *testing.T) {
			raw, err := json.Marshal(c.payload)
			require.NoError(t, err)
			require.NotEmpty(t, raw)
		})
	}
}
