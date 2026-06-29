package publisher_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/carolsimone/continuo/executor-controller/adapters/publisher"
	"github.com/carolsimone/continuo/executor-controller/domain/event"
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

	payload, err := json.Marshal(event.JobDeployed{
		TaskID: "t1", ScheduleID: "s1", JobName: "j", NodeType: "dbt-model",
		ImageTag: "sha-abc", TaskRetryCount: 2, MaxRetries: 5,
	})
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
		ImageTag: "sha-abc", TaskRetryCount: 2, MaxRetries: 5,
	}, nd)
}

func TestPublisher_NodeUpdatedFailed(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	payload, err := json.Marshal(event.NodeUpdated{
		TaskID: "t1", ScheduleID: "s1", ScheduleName: "daily", ServiceName: "dbt",
		SchemaName: "public", TableName: "orders", Status: "FAILED",
	})
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
		ID: id, EventType: "validation_completed", StreamName: streams.ValidationCompletedV1, Payload: body,
	}))

	v := lastEntryFields(t, r, streams.ValidationCompletedV1)
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
