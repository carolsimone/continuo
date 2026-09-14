package publisher_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/carolsimone/continuo/execution-controller/adapters/delayqueue"
	"github.com/carolsimone/continuo/execution-controller/adapters/publisher"
	"github.com/carolsimone/continuo/execution-controller/domain/event"
	"github.com/carolsimone/continuo/execution-controller/serialization"
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

func TestPublisher_NodeDeployed(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	payload, err := json.Marshal(serialization.JobDeployedFromDomain(event.JobDeployed{
		TaskID: "t1", ScheduleID: "s1", JobName: "j", NodeType: "dbt-model",
		ImageTag: "sha-abc", Operation: "test", TaskRetryCount: 2, MaxRetries: 5,
	}))
	require.NoError(t, err)

	id := uuid.New()
	require.NoError(t, pub.Publish(context.Background(), &outbox.Entry{
		ID: id, EventType: "node_deployed", StreamName: streams.NodeDeployedV1, Payload: payload,
	}))

	v := lastEntryFields(t, r, streams.NodeDeployedV1)
	// node.deployed:v1 carries a typed JSON payload; outbox_entry_id is a flat sibling.
	assert.Equal(t, id.String(), v["outbox_entry_id"])
	_, hasFlatJobName := v["job_name"]
	assert.False(t, hasFlatJobName, "business fields move into the typed payload, not flat keys")

	payloadStr, ok := v["payload"].(string)
	require.True(t, ok, "expected a string payload field")
	var nd pkgevents.NodeDeployed
	require.NoError(t, json.Unmarshal([]byte(payloadStr), &nd))
	assert.Equal(t, pkgevents.NodeDeployed{
		TaskID: "t1", ScheduleID: "s1", JobName: "j", NodeType: "dbt-model",
		ImageTag: "sha-abc", Operation: "test", TaskRetryCount: 2, MaxRetries: 5,
	}, nd)
}

func TestPublisher_NodeUpdatedFailed(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	payload, err := json.Marshal(serialization.NodeUpdatedFromDomain(event.NodeUpdated{
		TaskID: "t1", ScheduleID: "s1", ScheduleName: "daily", ServiceName: "dbt",
		SchemaName: "public", TableName: "orders", Status: "FAILED",
	}))
	require.NoError(t, err)

	require.NoError(t, pub.Publish(context.Background(), &outbox.Entry{
		ID: uuid.New(), EventType: "node_updated", StreamName: streams.NodeUpdatedV1, Payload: payload,
	}))

	v := lastEntryFields(t, r, streams.NodeUpdatedV1)
	assert.Equal(t, "FAILED", v["status"])
	assert.Equal(t, "orders", v["table_name"])
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

// TestPublisher_NodeDeployed_OutOfRangeMaxRetries_IsPermanent guards Fix #5:
// an out-of-range numeric field on a known, well-formed event_type is a
// deterministic bad payload — retrying it can never succeed — so it must be
// classified as pkgevents.ErrPermanent.
func TestPublisher_NodeDeployed_OutOfRangeMaxRetries_IsPermanent(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	payload, err := json.Marshal(serialization.JobDeployedFromDomain(event.JobDeployed{
		TaskID: "t1", ScheduleID: "s1", JobName: "j", NodeType: "dbt-model",
		ImageTag: "sha-abc", MaxRetries: 1 << 40, // exceeds int32 range
	}))
	require.NoError(t, err)

	pubErr := pub.Publish(context.Background(), &outbox.Entry{
		ID: uuid.New(), EventType: "node_deployed", StreamName: streams.NodeDeployedV1, Payload: payload,
	})
	require.Error(t, pubErr)
	assert.True(t, errors.Is(pubErr, pkgevents.ErrPermanent),
		"out-of-range max_retries must be permanent (ErrPermanent), not retried forever")
}

// TestPublisher_ContractAllHandledEventTypes is a regression guard that asserts
// every event_type the merged execution-controller publisher is expected to
// handle — both the executor-originated types and the k8s-controller-originated
// types it absorbed — does NOT return "unknown event_type". This prevents a
// recurrence of the class of bug where an emit site uses a string that has no
// matching case in the publisher switch (e.g. compile_node_completed was
// emitted but unmapped → events never published). The event_type constants are
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
			eventType:  "node_deployed",
			streamName: streams.NodeDeployedV1,
			payload:    mustMarshal(t, serialization.JobDeployedFromDomain(event.JobDeployed{TaskID: "t1", ScheduleID: "s1", JobName: "j1", NodeType: "dbt-model", ImageTag: "sha-abc"})),
		},
		{
			eventType:  "node_updated",
			streamName: streams.NodeUpdatedV1,
			payload:    mustMarshal(t, serialization.NodeUpdatedFromDomain(event.NodeUpdated{TaskID: "t1", ScheduleID: "s1", ScheduleName: "daily", ServiceName: "svc", SchemaName: "public", TableName: "tbl", Status: "SUCCEEDED"})),
		},
		{
			eventType:  event.EventTypeTaskRetry,
			streamName: streams.RetryTaskV1,
			payload: mustMarshal(t, serialization.TaskRetryFromDomain(event.TaskRetry{
				TaskID: "t1", ScheduleID: "s1", ScheduleName: "daily", ServiceName: "svc",
				SchemaName: "pub", TableName: "tbl", JobName: "j1", ImageTag: "sha",
				RetryCount: 1, MaxRetries: 3, NodeType: "dbt-model",
			})),
		},
		{
			eventType:  event.EventTypeTaskFailed,
			streamName: streams.TaskFailedV1,
			payload: mustMarshal(t, serialization.TaskFailedFromDomain(event.TaskFailed{
				TaskID: "t1", ScheduleID: "s1", ScheduleName: "daily", ServiceName: "svc",
				SchemaName: "pub", TableName: "tbl", JobName: "j1", ErrorMessage: "err", RetryCount: 0,
			})),
		},
		{
			eventType:  event.EventTypeCheckDelayed,
			streamName: streams.CheckK8sV1,
			payload: mustMarshal(t, serialization.JobCheckRequestFromDomain(event.JobCheckRequest{
				TaskID: "t1", ScheduleID: "s1", ScheduleName: "daily", ServiceName: "svc",
				SchemaName: "pub", TableName: "tbl", JobName: "j1", CheckAfter: 1700000000,
				NodeType: "dbt-model", ImageTag: "sha", RetryCount: 0, MaxRetries: 3,
			})),
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
		{
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

// TestPublish_CheckDelayedGoesToDelayQueueNotStream proves a check_delayed
// outbox row is written into the delay queue (ticket + due time) and never
// XADDed to its stream — a not-yet-due check waits off the stream until the
// promoter moves it, instead of self-recirculating.
func TestPublish_CheckDelayedGoesToDelayQueueNotStream(t *testing.T) {
	mr, client := newTestRedis(t)
	defer mr.Close()
	p := publisher.NewOutboxPublisher(client, slog.Default())
	payload, _ := json.Marshal(serialization.JobCheckRequestFromDomain(event.JobCheckRequest{
		TaskID: uuid.New().String(), ScheduleID: uuid.New().String(), JobName: "job-a",
		CheckAfter: time.Now().Add(time.Hour).Unix(), RetryCount: 0, MaxRetries: 2,
	}))
	entry := &outbox.Entry{ID: uuid.New(), EventType: event.EventTypeCheckDelayed, Payload: payload, StreamName: streams.CheckK8sV1}
	require.NoError(t, p.Publish(context.Background(), entry))
	require.False(t, mr.Exists(streams.CheckK8sV1), "check_delayed must not be XADDed")
	require.True(t, mr.Exists(delayqueue.PendingKey))
	require.True(t, mr.Exists(delayqueue.TicketsKey))
}

// TestPublish_TaskExecutionRecordedUsesTypedMap proves task_execution_recorded
// (a k8s-controller-originated event type the merged publisher now also
// handles) is XADDed via its typed ToMap(), not dropped as unknown.
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

// TestPublisher_ValidationNodeCompleted verifies the k8s-controller-originated
// per-node validation result is re-emitted verbatim on the "payload" field so
// the executor's own ParseValidationNodeCompleted can decode it.
func TestPublisher_ValidationNodeCompleted(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	body := []byte(`{"release_id":"rel_1","node_id":"public.orders","outcome":"ok","dbt_log_uri":"s3://logs/x"}`)
	id := uuid.New()
	require.NoError(t, pub.Publish(context.Background(), &outbox.Entry{
		ID: id, EventType: event.EventTypeValidationNodeCompleted, StreamName: streams.ValidationNodeCompletedV1, Payload: body,
	}))

	v := lastEntryFields(t, r, streams.ValidationNodeCompletedV1)
	assert.Equal(t, id.String(), v["outbox_entry_id"])
	payloadStr, ok := v["payload"].(string)
	require.True(t, ok, "expected a string payload field")
	assert.JSONEq(t, string(body), payloadStr, "stored per-node result re-emitted verbatim")
}

// TestPublisher_SeedBuildNodeCompleted verifies the per-seed build terminal
// status is re-emitted verbatim on the "payload" field.
func TestPublisher_SeedBuildNodeCompleted(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	body := []byte(`{"release_id":"rel_1","node_id":"public.seed_x","outcome":"ok"}`)
	id := uuid.New()
	require.NoError(t, pub.Publish(context.Background(), &outbox.Entry{
		ID: id, EventType: event.EventTypeSeedBuildNodeCompleted, StreamName: streams.SeedBuildNodeCompletedV1, Payload: body,
	}))

	v := lastEntryFields(t, r, streams.SeedBuildNodeCompletedV1)
	assert.Equal(t, id.String(), v["outbox_entry_id"])
	payloadStr, ok := v["payload"].(string)
	require.True(t, ok, "expected a string payload field")
	assert.JSONEq(t, string(body), payloadStr, "stored per-node result re-emitted verbatim")
}

// TestPublisher_CompileNodeCompleted verifies the compile leg's terminal-status
// event is publishable — a missing switch case stranded releases in
// `compiling` (no compile.node.completed:v1).
func TestPublisher_CompileNodeCompleted(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	body := []byte(`{"release_id":"rel_1","node_id":"service-1","outcome":"ok"}`)
	id := uuid.New()
	require.NoError(t, pub.Publish(context.Background(), &outbox.Entry{
		ID: id, EventType: event.EventTypeCompileNodeCompleted, StreamName: streams.CompileNodeCompletedV1, Payload: body,
	}))

	v := lastEntryFields(t, r, streams.CompileNodeCompletedV1)
	assert.Equal(t, id.String(), v["outbox_entry_id"])
	payloadStr, ok := v["payload"].(string)
	require.True(t, ok, "expected a string payload field")
	assert.JSONEq(t, string(body), payloadStr, "stored per-node result re-emitted verbatim")
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

// TestPublisher_CheckDelayed_OutOfRangeMaxRetries_IsPermanent guards Fix #5:
// an out-of-range numeric field on a known, well-formed event_type is a
// deterministic bad payload — retrying it can never succeed — so it must be
// classified as pkgevents.ErrPermanent.
func TestPublisher_CheckDelayed_OutOfRangeMaxRetries_IsPermanent(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	payload := mustMarshal(t, serialization.JobCheckRequestFromDomain(event.JobCheckRequest{
		TaskID: "t1", ScheduleID: "s1", ScheduleName: "daily", ServiceName: "svc",
		SchemaName: "pub", TableName: "tbl", JobName: "j1", NodeType: "dbt-model",
		ImageTag: "sha", MaxRetries: 1 << 40, // exceeds int32 range
	}))

	err := pub.Publish(context.Background(), &outbox.Entry{
		ID: uuid.New(), EventType: event.EventTypeCheckDelayed, StreamName: streams.CheckK8sV1, Payload: payload,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkgevents.ErrPermanent),
		"out-of-range max_retries must be permanent (ErrPermanent), not retried forever")
}

// ticketPayload extracts the check payload from the delay-queue ticket envelope
// stored in TicketsKey (the envelope wraps the payload with the outbox entry ID).
func ticketPayload(t *testing.T, raw string) string {
	t.Helper()
	var env struct {
		Payload string `json:"payload"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &env))
	return env.Payload
}

// TestPublish_CheckDelayed_CarriesRunningAnnounced verifies running_announced
// survives the check_delayed → delay-queue typed-payload conversion.
func TestPublish_CheckDelayed_CarriesRunningAnnounced(t *testing.T) {
	mr, client := newTestRedis(t)
	defer mr.Close()
	p := publisher.NewOutboxPublisher(client, slog.Default())

	raw, err := json.Marshal(serialization.JobCheckRequestFromDomain(event.JobCheckRequest{
		TaskID:           uuid.New().String(),
		ScheduleID:       uuid.New().String(),
		JobName:          "job-1",
		RunningAnnounced: true,
	}))
	require.NoError(t, err)

	require.NoError(t, p.Publish(context.Background(), &outbox.Entry{
		ID: uuid.New(), EventType: event.EventTypeCheckDelayed, StreamName: streams.CheckK8sV1, Payload: raw,
	}))

	raw2, err := client.HGet(context.Background(), delayqueue.TicketsKey, "job-1").Result()
	require.NoError(t, err)
	var ck pkgevents.CheckK8s
	require.NoError(t, json.Unmarshal([]byte(ticketPayload(t, raw2)), &ck))
	assert.True(t, ck.RunningAnnounced, "running_announced must survive check_delayed → delay-queue conversion")
}
