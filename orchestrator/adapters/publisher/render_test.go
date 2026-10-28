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

func TestRender_DeadLetterRowOmitsOutboxEntryID(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	values, err := p.Render(deadLetterEntry())
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
	entry := deadLetterEntry()
	first, err := p.Render(entry)
	require.NoError(t, err)
	first["failure_kind"] = "mutated"
	second, err := p.Render(entry)
	require.NoError(t, err)
	require.Equal(t, string(model.DeadLetterKindPermanent), second["failure_kind"])
}

// deadLetterEntry is a dead-letter row of a permanent failure, the one event
// type orchestrator publishes.
func deadLetterEntry() *outbox.Entry {
	body := []byte(`{"failure_kind":"` + string(model.DeadLetterKindPermanent) + `","original_event_type":"node_updated"}`)
	return &outbox.Entry{ID: uuid.New(), EventType: outbox.DeadLetterEventType, StreamName: streams.OutboxDeadLetterV1, Payload: body}
}

// TestPublishValues_AreRenderPlusOutboxEntryID guards against the XADD values
// being computed apart from Render.
func TestPublishValues_AreRenderPlusOutboxEntryID(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	entry := deadLetterEntry()
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
