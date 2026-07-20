package publisher_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/carolsimone/continuo/k8s-controller/adapters/delayqueue"
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

// TestPublish_CheckDelayed_WritesDelayQueueNotStream proves a check_delayed
// outbox row is HSET+ZADD'd into the delay queue keyed by JobName, and NOT
// XADD'd to check.k8s:v1 — removing the self-recirculating stream timer (#282).
func TestPublish_CheckDelayed_WritesDelayQueueNotStream(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	r := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	pub := publisher.NewOutboxPublisher(r, newTestLogger())

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

	ctx := context.Background()
	require.NoError(t, pub.Publish(ctx, &outbox.Entry{
		ID: uuid.New(), EventType: "check_delayed", StreamName: streams.CheckK8sV1, Payload: raw,
	}))

	// The stream must NOT have received this check.
	xlen, err := r.XLen(ctx, streams.CheckK8sV1).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), xlen, "check_delayed must not XADD the stream anymore")

	// The ZSET holds the due time keyed by JobName.
	score, err := r.ZScore(ctx, delayqueue.PendingKey, "job-1").Result()
	require.NoError(t, err)
	assert.Equal(t, float64(12345), score)

	// The HASH holds the typed CheckK8s payload, business fields intact.
	payloadStr, err := r.HGet(ctx, delayqueue.TicketsKey, "job-1").Result()
	require.NoError(t, err)
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

// TestPublish_CheckDelayed_CarriesRunningAnnounced verifies running_announced
// survives the check_delayed → delay-queue typed-payload conversion.
func TestPublish_CheckDelayed_CarriesRunningAnnounced(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	r := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	pub := publisher.NewOutboxPublisher(r, newTestLogger())

	raw, err := json.Marshal(event.JobCheckRequest{
		TaskID:           uuid.New().String(),
		ScheduleID:       uuid.New().String(),
		JobName:          "job-1",
		RunningAnnounced: true,
	})
	require.NoError(t, err)

	require.NoError(t, pub.Publish(context.Background(), &outbox.Entry{
		ID: uuid.New(), EventType: "check_delayed", StreamName: streams.CheckK8sV1, Payload: raw,
	}))

	payloadStr, err := r.HGet(context.Background(), delayqueue.TicketsKey, "job-1").Result()
	require.NoError(t, err)
	var ck pkgevents.CheckK8s
	require.NoError(t, json.Unmarshal([]byte(payloadStr), &ck))
	assert.True(t, ck.RunningAnnounced, "running_announced must survive check_delayed → delay-queue conversion")
}
