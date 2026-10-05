package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

type renderingPublisher struct {
	values map[string]any
	err    error
}

func (p *renderingPublisher) Publish(context.Context, *Entry) error { return nil }
func (p *renderingPublisher) Render(*Entry) (map[string]any, error) {
	return p.values, p.err
}

func TestStringifyFields_ScalarEncodings(t *testing.T) {
	got, err := StringifyFields(map[string]any{
		"s": "x", "b": []byte("y"), "i": 42, "i64": int64(-7), "u": uint32(9),
		"f": 1.5, "t": true, "fa": false, "nil": nil,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"s": "x", "b": "y", "i": "42", "i64": "-7", "u": "9", "f": "1.5", "t": "1", "fa": "0", "nil": ""}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
}

func TestStringifyFields_RejectsNonScalar(t *testing.T) {
	if _, err := StringifyFields(map[string]any{"m": map[string]any{"a": 1}}); err == nil {
		t.Fatal("want an error for a nested map")
	}
}

func TestBuildDeadLetterEntry_EmbedsRenderedFields(t *testing.T) {
	created := time.Date(2026, 10, 5, 9, 0, 0, 1000, time.UTC)
	failed := &Entry{ID: uuid.New(), AggregateID: uuid.New(), EventType: "node_updated", StreamName: "node.updated:v1", CreatedAt: created}
	pub := &renderingPublisher{values: map[string]any{"node_id": "a.b", "attempt": 2}}
	dl := buildDeadLetterEntry(failed, "state_outbox", pub, model.DeadLetterKindTransientExhausted, errors.New("redis down"), 13)

	var p DeadLetterPayload
	if err := json.Unmarshal(dl.Payload, &p); err != nil {
		t.Fatal(err)
	}
	if p.OutboxTable != "state_outbox" || p.OriginalCreatedAt != created.Format(OriginalCreatedAtLayout) {
		t.Fatalf("payload = %+v", p)
	}
	var fields map[string]string
	if err := json.Unmarshal([]byte(p.OriginalFields), &fields); err != nil {
		t.Fatalf("original_fields: %v", err)
	}
	if fields["node_id"] != "a.b" || fields["attempt"] != "2" {
		t.Fatalf("fields = %v", fields)
	}
	if _, ok := fields["outbox_entry_id"]; ok {
		t.Fatal("original_fields must not carry outbox_entry_id")
	}
}

func TestBuildDeadLetterEntry_UnrenderableRowHasNoFields(t *testing.T) {
	failed := &Entry{ID: uuid.New(), AggregateID: uuid.New(), EventType: "bad", StreamName: "s:v1", CreatedAt: time.Now()}
	pub := &renderingPublisher{err: errors.New("cannot decode")}
	dl := buildDeadLetterEntry(failed, "state_outbox", pub, model.DeadLetterKindPermanent, errors.New("cannot decode"), 1)
	var p DeadLetterPayload
	if err := json.Unmarshal(dl.Payload, &p); err != nil {
		t.Fatal(err)
	}
	if p.OriginalFields != "" {
		t.Fatalf("original_fields = %q, want empty", p.OriginalFields)
	}
}

func TestBuildDeadLetterEntry_PublisherWithoutRendererHasNoFields(t *testing.T) {
	failed := &Entry{ID: uuid.New(), AggregateID: uuid.New(), EventType: "e", StreamName: "s:v1", CreatedAt: time.Now()}
	dl := buildDeadLetterEntry(failed, "t_outbox", nil, model.DeadLetterKindPermanent, errors.New("x"), 1)
	var p DeadLetterPayload
	if err := json.Unmarshal(dl.Payload, &p); err != nil {
		t.Fatal(err)
	}
	if p.OriginalFields != "" {
		t.Fatalf("original_fields = %q, want empty", p.OriginalFields)
	}
}

func TestDecodeDeadLetterFields_RoundTrip(t *testing.T) {
	failed := &Entry{ID: uuid.New(), AggregateID: uuid.New(), EventType: "e", StreamName: "s:v1", CreatedAt: time.Now().UTC()}
	pub := &renderingPublisher{values: map[string]any{"k": "v"}}
	dl := buildDeadLetterEntry(failed, "t_outbox", pub, model.DeadLetterKindTransientExhausted, errors.New("x"), 13)
	values, err := DeadLetterValues(dl)
	if err != nil {
		t.Fatal(err)
	}
	fields, err := StringifyFields(values)
	if err != nil {
		t.Fatal(err)
	}
	p, original, err := DecodeDeadLetterFields(fields)
	if err != nil {
		t.Fatal(err)
	}
	if p.FailedOutboxID != failed.ID.String() || p.Attempts != 13 || original["k"] != "v" {
		t.Fatalf("p = %+v original = %v", p, original)
	}
}

func TestDecodeDeadLetterFields_OldShapeHasNoOriginalFields(t *testing.T) {
	p, original, err := DecodeDeadLetterFields(map[string]string{
		"original_event_type": "e", "original_stream": "s:v1", "original_aggregate_id": uuid.NewString(),
		"failure_kind": "permanent", "error": "x", "attempts": "1", "failed_outbox_id": uuid.NewString(),
		"outbox_entry_id": uuid.NewString(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if original != nil || p.OriginalStream != "s:v1" {
		t.Fatalf("p = %+v original = %v", p, original)
	}
}
