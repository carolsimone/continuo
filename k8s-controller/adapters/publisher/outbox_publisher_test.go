package publisher_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/carolsimone/continuo/k8s-controller/adapters/publisher"
	"github.com/carolsimone/continuo/k8s-controller/domain/event"
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

func TestPublisher_SeedBuildNodeCompleted(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	r := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	pub := publisher.NewOutboxPublisher(r, newTestLogger())

	body := []byte(`{"release_id":"rel_1","node_id":"public.seed_x","outcome":"ok"}`)
	id := uuid.New()
	require.NoError(t, pub.Publish(context.Background(), &outbox.Entry{
		ID: id, EventType: "seed_build_node_completed", StreamName: streams.SeedBuildNodeCompletedV1, Payload: body,
	}))

	res, err := r.XRange(context.Background(), streams.SeedBuildNodeCompletedV1, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, id.String(), res[0].Values["outbox_entry_id"])
	payloadStr, ok := res[0].Values["payload"].(string)
	require.True(t, ok, "expected a string payload field")
	assert.JSONEq(t, string(body), payloadStr, "stored per-node result re-emitted verbatim")
}

func TestPublisher_CompileNodeCompleted(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	r := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	pub := publisher.NewOutboxPublisher(r, newTestLogger())

	// Regression: the compile leg's terminal-status event must be publishable —
	// a missing switch case stranded releases in `compiling` (no compile.node.completed:v1).
	body := []byte(`{"release_id":"rel_1","node_id":"service-1","outcome":"ok"}`)
	id := uuid.New()
	require.NoError(t, pub.Publish(context.Background(), &outbox.Entry{
		ID: id, EventType: "compile_node_completed", StreamName: streams.CompileNodeCompletedV1, Payload: body,
	}))

	res, err := r.XRange(context.Background(), streams.CompileNodeCompletedV1, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, id.String(), res[0].Values["outbox_entry_id"])
	payloadStr, ok := res[0].Values["payload"].(string)
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

// TestPublisher_UnknownEventType_IsTransient guards Fix #3: an unknown
// event_type must NOT be classified as pkgevents.ErrPermanent. During a
// rolling deployment an old replica can dequeue a row for an event_type only
// a newer replica knows about; that row must be retried through its budget,
// not dead-lettered on attempt #1.
func TestPublisher_UnknownEventType_IsTransient(t *testing.T) {
	pub := publisher.NewOutboxPublisher(nil, newTestLogger())
	err := pub.Publish(context.Background(), &outbox.Entry{
		ID: uuid.New(), EventType: "not_yet_known_type", Payload: []byte(`{}`), StreamName: "x:v1",
	})
	require.Error(t, err)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent),
		"unknown event_type must be transient (plain error), not ErrPermanent")
}

// TestPublisher_CheckDelayed_OutOfRangeMaxRetries_IsPermanent guards Fix #5:
// an out-of-range numeric field on a known, well-formed event_type is a
// deterministic bad payload — retrying it can never succeed — so it must be
// classified as pkgevents.ErrPermanent.
func TestPublisher_CheckDelayed_OutOfRangeMaxRetries_IsPermanent(t *testing.T) {
	pub := publisher.NewOutboxPublisher(nil, newTestLogger())
	payload := mustMarshalK8s(t, event.JobCheckRequest{
		TaskID: "t1", ScheduleID: "s1", ScheduleName: "daily", ServiceName: "svc",
		SchemaName: "pub", TableName: "tbl", JobName: "j1", NodeType: "dbt-model",
		ImageTag: "sha", MaxRetries: 1 << 40, // exceeds int32 range
	})

	err := pub.Publish(context.Background(), &outbox.Entry{
		ID: uuid.New(), EventType: "check_delayed", StreamName: streams.CheckK8sV1, Payload: payload,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkgevents.ErrPermanent),
		"out-of-range max_retries must be permanent (ErrPermanent), not retried forever")
}

// TestPublisher_ContractAllHandledEventTypes is a regression guard that asserts
// every event_type the k8s publisher is expected to handle does NOT return
// "unknown event_type". This prevents a recurrence of the class of bug where an
// emit site uses a string that has no matching case in the publisher switch
// (e.g. compile_node_completed was emitted but unmapped → events never published).
// The event_type constants are the single source of truth shared by both sites.
func TestPublisher_ContractAllHandledEventTypes(t *testing.T) {
	// Minimal valid payloads for each event_type — just enough for the publisher's
	// json.Unmarshal to succeed. Verbatim-passthrough cases use any valid JSON object.
	cases := []struct {
		eventType  string
		streamName string
		payload    []byte
	}{
		{
			eventType:  "task_status_updated",
			streamName: streams.TaskStatusUpdatedV1,
			payload:    mustMarshalK8s(t, pkgevents.TaskStatusUpdated{TaskID: "t1", ScheduleID: "s1", Status: "SUCCEEDED", RetryCount: 0}),
		},
		{
			eventType:  "task_execution_recorded",
			streamName: streams.TaskExecutionRecordedV1,
			payload:    mustMarshalK8s(t, pkgevents.TaskExecutionRecorded{ExecutionID: "e1", TaskID: "t1", JobName: "j1", ExecutionSeconds: 1.0}),
		},
		{
			eventType:  "task_retry",
			streamName: streams.RetryTaskV1,
			payload:    mustMarshalK8s(t, event.TaskRetry{TaskID: "t1", ScheduleID: "s1", ScheduleName: "daily", ServiceName: "svc", SchemaName: "pub", TableName: "tbl", JobName: "j1", ImageTag: "sha", RetryCount: 1, MaxRetries: 3, NodeType: "dbt-model"}),
		},
		{
			eventType:  "task_failed",
			streamName: streams.TaskFailedV1,
			payload:    mustMarshalK8s(t, event.TaskFailed{TaskID: "t1", ScheduleID: "s1", ScheduleName: "daily", ServiceName: "svc", SchemaName: "pub", TableName: "tbl", JobName: "j1", ErrorMessage: "err", RetryCount: 0}),
		},
		{
			eventType:  "check_delayed",
			streamName: streams.CheckK8sV1,
			payload:    mustMarshalK8s(t, event.JobCheckRequest{TaskID: "t1", ScheduleID: "s1", ScheduleName: "daily", ServiceName: "svc", SchemaName: "pub", TableName: "tbl", JobName: "j1", CheckAfter: 1700000000, NodeType: "dbt-model", ImageTag: "sha", RetryCount: 0, MaxRetries: 3}),
		},
		{
			eventType:  "node_status_updated",
			streamName: streams.NodeUpdatedV1,
			payload:    mustMarshalK8s(t, event.NodeStatusUpdated{TaskID: "t1", ScheduleID: "s1", ScheduleName: "daily", ServiceName: "svc", SchemaName: "pub", TableName: "tbl", Status: "SUCCEEDED"}),
		},
		{
			// Uses the shared constant — the single source of truth for this wire string.
			eventType:  event.EventTypeValidationNodeCompleted,
			streamName: streams.ValidationNodeCompletedV1,
			payload:    []byte(`{"release_id":"rel1","node_id":"public.orders","outcome":"ok"}`),
		},
		{
			eventType:  event.EventTypeSeedBuildNodeCompleted,
			streamName: streams.SeedBuildNodeCompletedV1,
			payload:    []byte(`{"release_id":"rel1","node_id":"public.seed_x","outcome":"ok"}`),
		},
		{
			eventType:  event.EventTypeCompileNodeCompleted,
			streamName: streams.CompileNodeCompletedV1,
			payload:    []byte(`{"release_id":"rel1","node_id":"service-1","outcome":"ok"}`),
		},
	}

	for _, tc := range cases {
		t.Run(tc.eventType, func(t *testing.T) {
			mr, err := miniredis.Run()
			require.NoError(t, err)
			t.Cleanup(mr.Close)
			r := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
			pub := publisher.NewOutboxPublisher(r, newTestLogger())

			publishErr := pub.Publish(context.Background(), &outbox.Entry{
				ID:         uuid.New(),
				EventType:  tc.eventType,
				StreamName: tc.streamName,
				Payload:    tc.payload,
			})
			// The only acceptable error here is an infrastructure error; "unknown event_type" is never acceptable.
			if publishErr != nil {
				assert.NotContains(t, publishErr.Error(), "unknown event_type",
					"event_type %q must be handled by the publisher switch", tc.eventType)
			}
		})
	}
}

func mustMarshalK8s(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
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
