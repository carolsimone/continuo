package publisher_test

import (
	"errors"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/adapters/publisher"
	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// payloadToValuesTestHelper exposes the payloadToValues logic for unit testing
// by constructing an entry and inspecting the fields the publisher would emit.
// We test by creating a publisher with a nil Redis client and calling an
// exported test-seam method. Since payloadToValues is unexported, we test it
// indirectly by round-tripping via the entry struct.

func makeEntry(eventType string, payload []byte) *outbox.Entry {
	return &outbox.Entry{
		ID:            uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		AggregateType: "orchestrator",
		AggregateID:   uuid.New(),
		EventType:     eventType,
		Payload:       payload,
	}
}

// payloadToValuesFor calls the publisher's internal dispatch via a test-local
// helper that mirrors the switch. This lets us validate every case without
// requiring a live Redis connection.
func payloadToValuesFor(t *testing.T, entry *outbox.Entry) map[string]interface{} {
	t.Helper()
	p := publisher.NewOutboxPublisher(nil, nil)
	vals, err := p.PayloadToValuesForTest(entry)
	require.NoError(t, err)
	return vals
}

// A dead-letter row publishes its fields and its outbox_entry_id.
func TestOutboxPublisher_DeadLetterIsItsFieldsAndTheOutboxEntryID(t *testing.T) {
	body := []byte(`{"failure_kind":"` + string(model.DeadLetterKindPermanent) + `","original_event_type":"node_updated"}`)
	entry := makeEntry(outbox.DeadLetterEventType, body)
	vals := payloadToValuesFor(t, entry)

	assert.Equal(t, entry.ID.String(), vals["outbox_entry_id"])
	assert.Equal(t, string(model.DeadLetterKindPermanent), vals["failure_kind"])
}

// TestOutboxPublisher_RunEventTypesAreUnknown pins that orchestrator publishes
// no run event, and no release_promoted row, since the topology swap writes no
// outbox row: a row of one of these types left in orchestrator_outbox is
// retried as an unknown event_type and dead-lettered when its budget is spent.
func TestOutboxPublisher_RunEventTypesAreUnknown(t *testing.T) {
	for _, evtType := range []string{
		"node_ready_for_execution",
		"cascade_task_skipped",
		"run_entries_dispatched",
		"run_entries_dispatch_failed",
		"release_promoted",
	} {
		t.Run(evtType, func(t *testing.T) {
			p := publisher.NewOutboxPublisher(nil, nil)
			_, err := p.PayloadToValuesForTest(makeEntry(evtType, []byte(`{}`)))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unknown event_type")
			assert.False(t, errors.Is(err, pkgevents.ErrPermanent))
		})
	}
}

func TestOutboxPublisher_UnknownEventType_ReturnsError(t *testing.T) {
	entry := makeEntry("unknown_event", []byte(`{}`))
	p := publisher.NewOutboxPublisher(nil, nil)
	_, err := p.PayloadToValuesForTest(entry)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown_event")
}

// TestOutboxPublisher_UnknownEventType_IsTransient guards Fix #3: an unknown
// event_type must NOT be classified as pkgevents.ErrPermanent. During a
// rolling deployment an old replica can dequeue a row for an event_type only
// a newer replica knows about; that row must be retried through its budget,
// not dead-lettered on attempt #1.
func TestOutboxPublisher_UnknownEventType_IsTransient(t *testing.T) {
	entry := makeEntry("not_yet_known_type", []byte(`{}`))
	p := publisher.NewOutboxPublisher(nil, nil)
	_, err := p.PayloadToValuesForTest(entry)
	require.Error(t, err)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent),
		"unknown event_type must be transient (plain error), not ErrPermanent")
}
