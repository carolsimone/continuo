package publisher

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/execution-controller/domain/event"
	"github.com/carolsimone/continuo/execution-controller/service/validation"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/alicebob/miniredis/v2"
)

var _ outbox.Renderer = (*OutboxPublisher)(nil)

func TestRender_CandidateLegEventIsPayloadField(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	values, err := p.Render(&outbox.Entry{ID: uuid.New(), EventType: validation.EventTypeValidationCompleted, Payload: []byte(`{"release_id":"r1"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if values["payload"] != `{"release_id":"r1"}` {
		t.Fatalf("values = %v", values)
	}
}

func TestRender_CheckDelayedIsNotRenderable(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	if _, err := p.Render(&outbox.Entry{ID: uuid.New(), EventType: event.EventTypeCheckDelayed}); err == nil {
		t.Fatal("want an error: check_delayed has no stream fields")
	}
}

func TestRender_DeadLetterRowOmitsOutboxEntryID(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	values, err := p.Render(&outbox.Entry{
		ID: uuid.New(), EventType: outbox.DeadLetterEventType, StreamName: streams.OutboxDeadLetterV1,
		Payload: []byte(`{"failure_kind":"permanent","original_event_type":"node_updated"}`),
	})
	require.NoError(t, err)
	require.Equal(t, "permanent", values["failure_kind"])
	require.NotContains(t, values, "outbox_entry_id")
}

func TestRender_TypedEventsAreAcceptedByStringifyFields(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	for _, entry := range []*outbox.Entry{
		{ID: uuid.New(), EventType: event.EventTypeTaskStatusUpdated, Payload: []byte(`{"task_id":"t1","schedule_id":"s1","status":"RUNNING"}`)},
		{ID: uuid.New(), EventType: event.EventTypeTaskExecutionRecorded, Payload: []byte(`{"execution_id":"e1","task_id":"t1","execution_seconds":1.5}`)},
	} {
		values, err := p.Render(entry)
		require.NoError(t, err)
		require.NotContains(t, values, "outbox_entry_id")
		_, err = outbox.StringifyFields(values)
		require.NoError(t, err)
	}
}

func TestRender_ReturnsAFreshMapEachCall(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	entry := &outbox.Entry{ID: uuid.New(), EventType: event.EventTypeTaskStatusUpdated, Payload: []byte(`{"task_id":"t1"}`)}
	first, err := p.Render(entry)
	require.NoError(t, err)
	first["task_id"] = "mutated"
	second, err := p.Render(entry)
	require.NoError(t, err)
	require.Equal(t, "t1", second["task_id"])
}

// TestPublish_XAddsExactlyRenderPlusOutboxEntryID guards against Publish
// recomputing its fields apart from Render.
func TestPublish_XAddsExactlyRenderPlusOutboxEntryID(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	p := NewOutboxPublisher(rdb, nil)
	entry := &outbox.Entry{
		ID: uuid.New(), EventType: event.EventTypeTaskStatusUpdated, StreamName: streams.TaskStatusUpdatedV1,
		Payload: []byte(`{"task_id":"t1","schedule_id":"s1","status":"RUNNING","retry_count":2}`),
	}
	rendered, err := p.Render(entry)
	require.NoError(t, err)
	want, err := outbox.StringifyFields(rendered)
	require.NoError(t, err)
	want["outbox_entry_id"] = entry.ID.String()

	require.NoError(t, p.Publish(context.Background(), entry))
	res, err := rdb.XRange(context.Background(), streams.TaskStatusUpdatedV1, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, res, 1)
	got := map[string]string{}
	for k, v := range res[0].Values {
		got[k] = v.(string)
	}
	require.Equal(t, want, got)
}
