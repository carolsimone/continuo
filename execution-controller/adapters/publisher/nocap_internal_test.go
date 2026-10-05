package publisher

import (
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/execution-controller/domain/event"
	"github.com/carolsimone/continuo/execution-controller/serialization"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// TestXAddArgs_DoNotTrim asserts the publisher never trims a stream: the
// dead-letter-controller's trim loop is the only thing that bounds them.
func TestXAddArgs_DoNotTrim(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	payload, err := json.Marshal(serialization.NodeUpdatedFromDomain(event.NodeUpdated{Status: "SUCCEEDED"}))
	if err != nil {
		t.Fatal(err)
	}
	entry := &outbox.Entry{ID: uuid.New(), StreamName: streams.NodeUpdatedV1, EventType: event.EventTypeNodeUpdated, Payload: payload}
	values, err := p.Render(entry)
	if err != nil {
		t.Fatal(err)
	}
	args := p.xaddArgs(entry, values)
	if args.Stream != streams.NodeUpdatedV1 {
		t.Fatalf("stream = %q, want %q", args.Stream, streams.NodeUpdatedV1)
	}
	if args.MaxLen != 0 || args.MinID != "" || args.Approx {
		t.Fatalf("publisher trims: MaxLen=%d MinID=%q Approx=%v", args.MaxLen, args.MinID, args.Approx)
	}
	if args.Values.(map[string]any)["outbox_entry_id"] != entry.ID.String() {
		t.Fatalf("outbox_entry_id missing from %v", args.Values)
	}
}
