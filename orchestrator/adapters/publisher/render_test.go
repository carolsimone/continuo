package publisher

import (
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var _ outbox.Renderer = (*OutboxPublisher)(nil)

func TestRender_PayloadEventIsPayloadFieldWithoutOutboxEntryID(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	values, err := p.Render(&outbox.Entry{ID: uuid.New(), EventType: "release_promoted", StreamName: streams.ReleasePromotedV2, Payload: []byte(`{"release_id":"r1"}`)})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"payload": `{"release_id":"r1"}`}, values)
}

func TestRender_CascadeSkippedIsATaskStatusMap(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	values, err := p.Render(&outbox.Entry{ID: uuid.New(), EventType: "cascade_task_skipped", Payload: []byte(`{"TaskID":"t1","ScheduleID":"s1"}`)})
	require.NoError(t, err)
	require.Equal(t, "t1", values["task_id"])
	require.Equal(t, "skipped", values["status"])
	require.NotContains(t, values, "outbox_entry_id")
	_, err = outbox.StringifyFields(values)
	require.NoError(t, err)
}

func TestRender_DeadLetterRowOmitsOutboxEntryID(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	body := []byte(`{"failure_kind":"` + string(model.DeadLetterKindPermanent) + `","original_event_type":"node_updated"}`)
	values, err := p.Render(&outbox.Entry{ID: uuid.New(), EventType: outbox.DeadLetterEventType, StreamName: streams.OutboxDeadLetterV1, Payload: body})
	require.NoError(t, err)
	require.Equal(t, string(model.DeadLetterKindPermanent), values["failure_kind"])
	require.NotContains(t, values, "outbox_entry_id")
}

func TestRender_UnknownEventTypeIsAnError(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	_, err := p.Render(&outbox.Entry{ID: uuid.New(), EventType: "not_yet_known"})
	require.Error(t, err)
}

func TestRender_ReturnsAFreshMapEachCall(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	entry := &outbox.Entry{ID: uuid.New(), EventType: "release_promoted", Payload: []byte(`{}`)}
	first, err := p.Render(entry)
	require.NoError(t, err)
	first["payload"] = "mutated"
	second, err := p.Render(entry)
	require.NoError(t, err)
	require.Equal(t, "{}", second["payload"])
}

// TestPublishValues_AreRenderPlusOutboxEntryID guards against the XADD values
// being computed apart from Render.
func TestPublishValues_AreRenderPlusOutboxEntryID(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	entry := &outbox.Entry{ID: uuid.New(), EventType: "release_promoted", Payload: []byte(`{"release_id":"r1"}`)}
	rendered, err := p.Render(entry)
	require.NoError(t, err)
	args, err := p.xaddArgs(entry)
	require.NoError(t, err)
	want := map[string]any{"outbox_entry_id": entry.ID.String()}
	for k, v := range rendered {
		want[k] = v
	}
	require.Equal(t, want, args.Values)
}
