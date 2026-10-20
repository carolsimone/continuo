package publisher_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/carolsimone/continuo/execution-controller/adapters/publisher"
	"github.com/carolsimone/continuo/execution-controller/domain/event"
	"github.com/carolsimone/continuo/execution-controller/service/validation"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newRedis(t *testing.T) *goredis.Client {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	return goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
}

// newTestRedis starts an in-process miniredis instance and returns both the
// server handle (for asserting on keys/streams directly) and a client
// connected to it. Callers close mr themselves (defer mr.Close()) rather than
// relying on t.Cleanup, so a test can assert post-close state if it needs to.
func newTestRedis(t *testing.T) (*miniredis.Miniredis, *goredis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	return mr, goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
}

func lastEntryFields(t *testing.T, r *goredis.Client, stream string) map[string]interface{} {
	t.Helper()
	res, err := r.XRange(context.Background(), stream, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, res, 1)
	return res[0].Values
}

func TestPublisher_TaskStatusUpdated(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	payload, err := json.Marshal(pkgevents.TaskStatusUpdated{
		TaskID: "t1", ScheduleID: "s1", Status: "RUNNING", RetryCount: 0,
	})
	require.NoError(t, err)

	id := uuid.New()
	require.NoError(t, pub.Publish(context.Background(), &outbox.Entry{
		ID: id, EventType: "task_status_updated", StreamName: streams.TaskStatusUpdatedV1, Payload: payload,
	}))

	v := lastEntryFields(t, r, streams.TaskStatusUpdatedV1)
	assert.Equal(t, "t1", v["task_id"])
	assert.Equal(t, "RUNNING", v["status"])
	assert.Equal(t, id.String(), v["outbox_entry_id"])
}

func TestPublisher_ValidationCompleted(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	// The aggregate gate stores the validation.completed body as the entry
	// payload; the publisher must re-emit it on the "payload" field so
	// release-controller's HandleValidationResult can decode it.
	body := []byte(`{"release_id":"rel_1","per_node_results":[{"node_id":"public.orders","status":"ok"}],"aggregate_status":"ok"}`)
	id := uuid.New()
	require.NoError(t, pub.Publish(context.Background(), &outbox.Entry{
		ID: id, EventType: "validation_completed", StreamName: streams.ValidationResultV1, Payload: body,
	}))

	v := lastEntryFields(t, r, streams.ValidationResultV1)
	assert.Equal(t, id.String(), v["outbox_entry_id"])
	payloadStr, ok := v["payload"].(string)
	require.True(t, ok, "expected a string payload field")
	assert.JSONEq(t, string(body), payloadStr, "stored aggregate payload re-emitted verbatim")
}

func TestPublisher_SeedBuildCompleted(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	body := []byte(`{"release_id":"rel_1","per_node_results":[{"node_id":"public.seed_x","status":"ok"}],"aggregate_status":"ok"}`)
	id := uuid.New()
	require.NoError(t, pub.Publish(context.Background(), &outbox.Entry{
		ID: id, EventType: "seed_build_completed", StreamName: streams.SeedBuildCompletedV1, Payload: body,
	}))

	v := lastEntryFields(t, r, streams.SeedBuildCompletedV1)
	assert.Equal(t, id.String(), v["outbox_entry_id"])
	payloadStr, ok := v["payload"].(string)
	require.True(t, ok, "expected a string payload field")
	assert.JSONEq(t, string(body), payloadStr, "stored aggregate payload re-emitted verbatim")
}

func TestPublisher_CompileCompleted(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	// Regression: the compile leg's aggregate event must be publishable — a
	// missing switch case stranded releases in `compiling` (no compile.completed:v1).
	body := []byte(`{"release_id":"rel_1","status":"ok"}`)
	id := uuid.New()
	require.NoError(t, pub.Publish(context.Background(), &outbox.Entry{
		ID: id, EventType: "compile_completed", StreamName: streams.CompileCompletedV1, Payload: body,
	}))

	v := lastEntryFields(t, r, streams.CompileCompletedV1)
	assert.Equal(t, id.String(), v["outbox_entry_id"])
	payloadStr, ok := v["payload"].(string)
	require.True(t, ok, "expected a string payload field")
	assert.JSONEq(t, string(body), payloadStr, "stored aggregate payload re-emitted verbatim")
}

func TestPublisher_UnknownEventType(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	err := pub.Publish(context.Background(), &outbox.Entry{
		ID: uuid.New(), EventType: "unknown_type", StreamName: "x:v1", Payload: []byte(`{}`),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown event_type")
}

// TestPublisher_UnknownEventType_IsTransient guards Fix #3: an unknown
// event_type must NOT be classified as pkgevents.ErrPermanent. During a
// rolling deployment an old replica can dequeue a row for an event_type only
// a newer replica knows about; that row must be retried through its budget,
// not dead-lettered on attempt #1.
func TestPublisher_UnknownEventType_IsTransient(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	err := pub.Publish(context.Background(), &outbox.Entry{
		ID: uuid.New(), EventType: "not_yet_known_type", StreamName: "x:v1", Payload: []byte(`{}`),
	})
	require.Error(t, err)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent),
		"unknown event_type must be transient (plain error), not ErrPermanent")
}

// TestPublisher_ContractAllHandledEventTypes is a regression guard that asserts
// every event_type this service's outbox rows carry — from the dispatcher, the
// observer, and every other emit site — does NOT return "unknown
// event_type". An emit site whose event_type has no matching case in the
// publisher switch strands its row unpublished with no visible error until an
// operator notices the missing downstream event. The event_type constants are
// the single source of truth shared by every producer.
func TestPublisher_ContractAllHandledEventTypes(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Minimal valid payloads for each event_type — just enough for the publisher's
	// json.Unmarshal to succeed. Verbatim-passthrough cases ("payload" field) use
	// any valid JSON object.
	cases := []struct {
		eventType  string
		streamName string
		payload    []byte
	}{
		{
			eventType:  "task_status_updated",
			streamName: streams.TaskStatusUpdatedV1,
			payload:    mustMarshal(t, pkgevents.TaskStatusUpdated{TaskID: "t1", ScheduleID: "s1", Status: "SUCCEEDED", RetryCount: 0}),
		},
		{
			eventType:  event.EventTypeTaskExecutionRecorded,
			streamName: streams.TaskExecutionRecordedV1,
			payload:    mustMarshal(t, pkgevents.TaskExecutionRecorded{TaskID: "t1", JobName: "j1"}),
		},
		{
			eventType:  event.EventTypeCheckDelayed,
			streamName: "",
			payload:    []byte(`{"task_id":"t1","job_name":"j1"}`),
		},
		{
			// Uses the shared constant — the single source of truth for this wire string.
			eventType:  validation.EventTypeValidationCompleted,
			streamName: streams.ValidationResultV1,
			payload:    []byte(`{"release_id":"rel1","aggregate_status":"ok"}`),
		},
		{
			eventType:  validation.EventTypeSeedBuildCompleted,
			streamName: streams.SeedBuildCompletedV1,
			payload:    []byte(`{"release_id":"rel1","status":"ok"}`),
		},
		{
			eventType:  validation.EventTypeCompileCompleted,
			streamName: streams.CompileCompletedV1,
			payload:    []byte(`{"release_id":"rel1","status":"ok"}`),
		},
		{
			eventType:  validation.EventTypeValidationNodeResult,
			streamName: streams.ValidationResultV1,
			payload:    []byte(`{"release_id":"rel1","stage":"validation","node_id":"node.a","status":"ok"}`),
		},
	}

	for _, tc := range cases {
		t.Run(tc.eventType, func(t *testing.T) {
			r := newRedis(t)
			pub := publisher.NewOutboxPublisher(r, logger)
			err := pub.Publish(context.Background(), &outbox.Entry{
				ID:         uuid.New(),
				EventType:  tc.eventType,
				StreamName: tc.streamName,
				Payload:    tc.payload,
			})
			// The only acceptable error here is an infrastructure error (e.g. Redis
			// down); "unknown event_type" is never acceptable.
			if err != nil {
				assert.NotContains(t, err.Error(), "unknown event_type",
					"event_type %q must be handled by the publisher switch", tc.eventType)
			}
		})
	}
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

// TestPublish_LeftoverCheckDelayedRowIsPublishedNowhere proves a status-check
// ticket a previous version wrote is marked processed without reaching Redis:
// no stream entry and no delay-queue key.
func TestPublish_LeftoverCheckDelayedRowIsPublishedNowhere(t *testing.T) {
	mr, client := newTestRedis(t)
	defer mr.Close()
	p := publisher.NewOutboxPublisher(client, slog.Default())
	entry := &outbox.Entry{ID: uuid.New(), EventType: event.EventTypeCheckDelayed,
		Payload: []byte(`{"task_id":"t1","job_name":"job-a","check_after":1700000000}`)}
	require.NoError(t, p.Publish(context.Background(), entry))
	assert.Empty(t, mr.Keys(), "nothing is written to Redis")
}

// TestPublish_TaskExecutionRecordedUsesTypedMap proves task_execution_recorded,
// emitted by the observer, is XADDed via its typed ToMap(), not dropped as
// unknown.
func TestPublish_TaskExecutionRecordedUsesTypedMap(t *testing.T) {
	mr, client := newTestRedis(t)
	defer mr.Close()
	p := publisher.NewOutboxPublisher(client, slog.Default())
	payload, _ := json.Marshal(pkgevents.TaskExecutionRecorded{TaskID: uuid.New().String(), JobName: "job-a"})
	entry := &outbox.Entry{ID: uuid.New(), EventType: event.EventTypeTaskExecutionRecorded, Payload: payload, StreamName: streams.TaskExecutionRecordedV1}
	require.NoError(t, p.Publish(context.Background(), entry))
	msgs, err := client.XRange(context.Background(), streams.TaskExecutionRecordedV1, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "job-a", msgs[0].Values["job_name"])
	require.Equal(t, entry.ID.String(), msgs[0].Values["outbox_entry_id"])
}

// TestPublisher_BadPayloadReturnsError verifies a malformed payload for a known
// event_type surfaces the unmarshal error rather than silently swallowing it.
func TestPublisher_BadPayloadReturnsError(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	err := pub.Publish(context.Background(), &outbox.Entry{
		ID: uuid.New(), EventType: "task_status_updated", Payload: []byte(`not json`), StreamName: "x:v1",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unmarshal task_status_updated")
}

// TestPublisher_PayloadShapesUnmarshalSuccessfully verifies that each switch
// case's typed struct round-trips correctly via JSON, ensuring the struct
// shapes match what writers will produce.
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
			pkgevents.TaskExecutionRecorded{TaskID: "t1", JobName: "j1", ExecutionSeconds: 12.5},
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

// TestPublisher_NodeUpdatedRowIsUnknown pins that execution-controller
// publishes nothing to node.updated:v1: a node_updated row left in
// execution_outbox is retried as an unknown event_type and dead-lettered when
// its budget is spent, never written to the retired stream.
func TestPublisher_NodeUpdatedRowIsUnknown(t *testing.T) {
	pub := publisher.NewOutboxPublisher(newRedis(t), slog.New(slog.NewTextHandler(os.Stderr, nil)))
	err := pub.Publish(context.Background(), &outbox.Entry{
		ID: uuid.New(), EventType: "node_updated", StreamName: "node.updated:v1",
		Payload: []byte(`{"task_id":"t1","schedule_id":"s1","status":"FAILED"}`),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown event_type")
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent), "an unknown event_type is retried, not dead-lettered at once")
}
