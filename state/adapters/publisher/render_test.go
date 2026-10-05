package publisher

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var _ outbox.Renderer = (*OutboxPublisher)(nil)

func TestRender_ReturnsPublishedFieldsWithoutOutboxEntryID(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	body, _ := json.Marshal(map[string]any{"schedule_id": "s1", "nested": map[string]any{"a": 1}})
	values, err := p.Render(&outbox.Entry{ID: uuid.New(), EventType: "query_model", StreamName: "query.model:v1", Payload: body})
	if err != nil {
		t.Fatal(err)
	}
	if values["schedule_id"] != "s1" {
		t.Fatalf("schedule_id = %v", values["schedule_id"])
	}
	if values["nested"] != `{"a":1}` {
		t.Fatalf("nested = %v, want JSON string", values["nested"])
	}
	if _, ok := values["outbox_entry_id"]; ok {
		t.Fatal("Render must not include outbox_entry_id")
	}
}

func TestRender_DeadLetterRowOmitsOutboxEntryID(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	entry := &outbox.Entry{
		ID:         uuid.New(),
		EventType:  outbox.DeadLetterEventType,
		StreamName: streams.OutboxDeadLetterV1,
		Payload:    deadLetterPayload(t, model.DeadLetterKindPermanent, "compile_requested"),
	}
	values, err := p.Render(entry)
	require.NoError(t, err)
	require.Equal(t, string(model.DeadLetterKindPermanent), values["failure_kind"])
	require.NotContains(t, values, "outbox_entry_id")
}

func TestRender_ReturnsAFreshMapEachCall(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	entry := &outbox.Entry{ID: uuid.New(), EventType: "query_model", StreamName: "query.model:v1", Payload: []byte(`{"schedule_id":"s1"}`)}
	first, err := p.Render(entry)
	require.NoError(t, err)
	first["schedule_id"] = "mutated"
	second, err := p.Render(entry)
	require.NoError(t, err)
	require.Equal(t, "s1", second["schedule_id"])
}

func TestRender_ValuesAreAcceptedByStringifyFields(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	values, err := p.Render(&outbox.Entry{ID: uuid.New(), EventType: "query_model", StreamName: "query.model:v1", Payload: []byte(`{"n":3,"ok":true,"gone":null,"nested":[1,2]}`)})
	require.NoError(t, err)
	_, err = outbox.StringifyFields(values)
	require.NoError(t, err)
}

// TestPublish_XAddsExactlyRenderPlusOutboxEntryID guards against Publish
// recomputing its fields apart from Render.
func TestPublish_XAddsExactlyRenderPlusOutboxEntryID(t *testing.T) {
	rdb, cleanup := newTestRedis(t)
	defer cleanup()
	p := NewOutboxPublisher(rdb, newTestLogger())
	entry := &outbox.Entry{ID: uuid.New(), EventType: "query_model", StreamName: streams.QueryModelV1, Payload: []byte(`{"schedule_id":"s1","nested":{"a":1}}`)}

	rendered, err := p.Render(entry)
	require.NoError(t, err)
	want, err := outbox.StringifyFields(rendered)
	require.NoError(t, err)
	want["outbox_entry_id"] = entry.ID.String()

	require.NoError(t, p.Publish(context.Background(), entry))
	res, err := rdb.XRange(context.Background(), streams.QueryModelV1, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, res, 1)
	got := map[string]string{}
	for k, v := range res[0].Values {
		got[k] = v.(string)
	}
	require.Equal(t, want, got)
}
