package redis

import (
	"testing"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/dead-letter-controller/serialization"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/google/uuid"
)

func TestRedrivePublisher_RenderAddsRedriveFields(t *testing.T) {
	dl := deadletter.DeadLetter{ID: uuid.New(), Source: deadletter.SourceConsumer, Group: "g",
		Fields: map[string]string{"k": "v", "outbox_entry_id": "original"}}
	body, _ := serialization.EncodeRedrive(dl)
	values, err := NewRedrivePublisher(nil).Render(&pkgoutbox.Entry{ID: uuid.New(), EventType: serialization.EventTypeRedrive, Payload: body})
	if err != nil {
		t.Fatal(err)
	}
	if values["k"] != "v" || values[pkgredis.RedriveGroupField] != "g" || values[pkgredis.RedrivenFromField] != dl.ID.String() {
		t.Fatalf("values = %v", values)
	}
	if _, ok := values["outbox_entry_id"]; ok {
		t.Fatal("Render must drop the original outbox_entry_id; Publish sets its own")
	}
}

func TestRedrivePublisher_OutboxRedriveHasNoGroup(t *testing.T) {
	dl := deadletter.DeadLetter{ID: uuid.New(), Source: deadletter.SourceOutbox, Fields: map[string]string{"k": "v"}}
	body, _ := serialization.EncodeRedrive(dl)
	values, err := NewRedrivePublisher(nil).Render(&pkgoutbox.Entry{ID: uuid.New(), EventType: serialization.EventTypeRedrive, Payload: body})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := values[pkgredis.RedriveGroupField]; ok {
		t.Fatalf("an outbox redrive goes to every group: %v", values)
	}
}

func TestRedrivePublisher_RenderReturnsAFreshMapEachCall(t *testing.T) {
	dl := deadletter.DeadLetter{ID: uuid.New(), Source: deadletter.SourceConsumer, Group: "g", Fields: map[string]string{"k": "v"}}
	body, _ := serialization.EncodeRedrive(dl)
	p := NewRedrivePublisher(nil)
	e := &pkgoutbox.Entry{ID: uuid.New(), EventType: serialization.EventTypeRedrive, Payload: body}
	a, _ := p.Render(e)
	a["mutated"] = "x"
	b, _ := p.Render(e)
	if _, ok := b["mutated"]; ok {
		t.Fatal("Render must return a fresh map")
	}
}

func TestRedrivePublisher_RendersItsOwnDeadLetterRows(t *testing.T) {
	e := &pkgoutbox.Entry{ID: uuid.New(), EventType: pkgoutbox.DeadLetterEventType, Payload: []byte(`{"original_stream":"s:v1","attempts":"3"}`)}
	values, err := NewRedrivePublisher(nil).Render(e)
	if err != nil {
		t.Fatal(err)
	}
	if values["original_stream"] != "s:v1" {
		t.Fatalf("values = %v", values)
	}
	if _, ok := values["outbox_entry_id"]; ok {
		t.Fatal("Render must not include outbox_entry_id")
	}
}

func TestRedrivePublisher_UnknownEventTypeAndBadPayloadArePermanent(t *testing.T) {
	p := NewRedrivePublisher(nil)
	for _, e := range []*pkgoutbox.Entry{
		{ID: uuid.New(), EventType: "nope", Payload: []byte(`{}`)},
		{ID: uuid.New(), EventType: serialization.EventTypeRedrive, Payload: []byte(`{}`)},
	} {
		if _, err := p.Render(e); err == nil {
			t.Fatalf("Render(%s) succeeded, want error", e.EventType)
		}
	}
}
