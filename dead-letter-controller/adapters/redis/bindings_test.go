package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/dead-letter-controller/service/handlers"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
)

// The application service the bindings are wired to satisfies the adapter port.
var _ Recorder = (*handlers.Recorder)(nil)

type fakeRecorder struct {
	got []deadletter.DeadLetter
	err error
}

func (f *fakeRecorder) Record(_ context.Context, dl deadletter.DeadLetter) error {
	if f.err != nil {
		return f.err
	}
	f.got = append(f.got, dl)
	return nil
}

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

var at = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

func xmsg(id string, values map[string]any) goredis.XMessage {
	return goredis.XMessage{ID: id, Values: values}
}

func TestConsumerDeadLetterBinding_StoresRedrivableRow(t *testing.T) {
	values, _ := events.ConsumerDeadLetterFields(events.DeadLetteredMessage{
		TenantID: "default", Producer: "state", OccurredAt: at, Stream: "node.updated:v1", Group: "orchestrator-node-updated",
		MessageID: "1759665600000-0", Fields: map[string]string{"k": "v"}, FailureKind: model.DeadLetterKindTransientExhausted,
		Error: "x", DeliveryCount: 5,
	})
	rec := &fakeRecorder{}
	b := NewConsumerDeadLetterBinding(rec, fixedClock{at})
	if err := b.Handle(context.Background(), xmsg("9-0", values)); err != nil {
		t.Fatal(err)
	}
	dl := rec.got[0]
	if dl.Source != deadletter.SourceConsumer || dl.Group != "orchestrator-node-updated" || !dl.Redrivable ||
		dl.DedupKey != deadletter.ConsumerKey("node.updated:v1", "orchestrator-node-updated", "1759665600000-0") ||
		!dl.OriginalAt.Equal(time.UnixMilli(1759665600000).UTC()) || dl.Producer != "state" || dl.DeliveryCount != 5 {
		t.Fatalf("dl = %+v", dl)
	}
}

func TestConsumerDeadLetterBinding_UnreadableStoredNotRedrivable(t *testing.T) {
	rec := &fakeRecorder{}
	b := NewConsumerDeadLetterBinding(rec, fixedClock{at})
	if err := b.Handle(context.Background(), xmsg("1759665600000-7", map[string]any{"garbage": "1"})); err != nil {
		t.Fatalf("an unreadable dead letter must be stored, not returned as an error: %v", err)
	}
	dl := rec.got[0]
	if dl.Redrivable || dl.FailureKind != model.DeadLetterKindPermanent || dl.Stream != streams.ConsumerDeadLetterV1 ||
		dl.DedupKey != deadletter.UnreadableKey(streams.ConsumerDeadLetterV1, "1759665600000-7") || dl.Fields["garbage"] != "1" {
		t.Fatalf("dl = %+v", dl)
	}
}

func TestConsumerDeadLetterBinding_StoreFailureReturnsError(t *testing.T) {
	rec := &fakeRecorder{err: errors.New("dial tcp: connection refused")}
	b := NewConsumerDeadLetterBinding(rec, fixedClock{at})
	if err := b.Handle(context.Background(), xmsg("1-0", map[string]any{"garbage": "1"})); err == nil {
		t.Fatal("a store failure must keep the entry pending")
	}
}

// A decodable dead letter whose store fails is returned too: the same
// pending-until-stored path as an unreadable one, on the readable branch.
func TestConsumerDeadLetterBinding_StoreFailureOfReadableReturnsError(t *testing.T) {
	values, _ := events.ConsumerDeadLetterFields(events.DeadLetteredMessage{
		TenantID: "default", Producer: "state", OccurredAt: at, Stream: "node.updated:v1", Group: "g",
		MessageID: "1759665600000-0", Fields: map[string]string{"k": "v"}, FailureKind: model.DeadLetterKindPermanent,
	})
	rec := &fakeRecorder{err: errors.New("postgres down")}
	if err := NewConsumerDeadLetterBinding(rec, fixedClock{at}).Handle(context.Background(), xmsg("1-0", values)); err == nil {
		t.Fatal("a store failure must keep the entry pending")
	}
}

func TestConsumerDeadLetterBinding_InvalidFailureKindIsUnreadable(t *testing.T) {
	values, _ := events.ConsumerDeadLetterFields(events.DeadLetteredMessage{
		TenantID: "default", Producer: "state", OccurredAt: at, Stream: "s:v1", Group: "g",
		MessageID: "1759665600000-0", Fields: map[string]string{"k": "v"}, FailureKind: model.DeadLetterKind("bogus"),
	})
	rec := &fakeRecorder{}
	if err := NewConsumerDeadLetterBinding(rec, fixedClock{at}).Handle(context.Background(), xmsg("1759665600000-3", values)); err != nil {
		t.Fatal(err)
	}
	if dl := rec.got[0]; dl.Redrivable || dl.DedupKey != deadletter.UnreadableKey(streams.ConsumerDeadLetterV1, "1759665600000-3") {
		t.Fatalf("dl = %+v", dl)
	}
}

func TestOutboxDeadLetterBinding_RedrivableWhenFieldsPresent(t *testing.T) {
	fields := map[string]any{
		"original_event_type": "node_updated", "original_stream": "node.updated:v1", "original_aggregate_id": "00000000-0000-0000-0000-000000000001",
		"failure_kind": "transient_exhausted", "error": "redis down", "attempts": "13",
		"failed_outbox_id": "00000000-0000-0000-0000-000000000002", "original_fields": `{"node_id":"a.b"}`,
		"original_created_at": "2026-10-05T10:00:00.000000001Z", "outbox_table": "state_outbox", "outbox_entry_id": "x",
	}
	rec := &fakeRecorder{}
	if err := NewOutboxDeadLetterBinding(rec, fixedClock{at}).Handle(context.Background(), xmsg("1759665600000-0", fields)); err != nil {
		t.Fatal(err)
	}
	dl := rec.got[0]
	if dl.Source != deadletter.SourceOutbox || !dl.Redrivable || dl.Fields["node_id"] != "a.b" || dl.Group != "" ||
		dl.Producer != "state_outbox" || dl.DedupKey != deadletter.OutboxKey("state_outbox", "00000000-0000-0000-0000-000000000002") ||
		dl.OriginalAt.Format(outbox.OriginalCreatedAtLayout) != "2026-10-05T10:00:00.000000001Z" || dl.DeliveryCount != 13 {
		t.Fatalf("dl = %+v", dl)
	}
}

func TestOutboxDeadLetterBinding_OldShapeNotRedrivable(t *testing.T) {
	fields := map[string]any{
		"original_event_type": "e", "original_stream": "s:v1", "original_aggregate_id": "a",
		"failure_kind": "permanent", "error": "x", "attempts": "1", "failed_outbox_id": "f", "outbox_entry_id": "o",
	}
	rec := &fakeRecorder{}
	if err := NewOutboxDeadLetterBinding(rec, fixedClock{at}).Handle(context.Background(), xmsg("1759665600000-0", fields)); err != nil {
		t.Fatal(err)
	}
	if rec.got[0].Redrivable || !rec.got[0].OriginalAt.Equal(time.UnixMilli(1759665600000).UTC()) {
		t.Fatalf("dl = %+v", rec.got[0])
	}
}

func TestOutboxDeadLetterBinding_UnreadableStoredNotRedrivable(t *testing.T) {
	rec := &fakeRecorder{}
	if err := NewOutboxDeadLetterBinding(rec, fixedClock{at}).Handle(context.Background(), xmsg("1759665600000-2", map[string]any{"attempts": "nope"})); err != nil {
		t.Fatalf("an unreadable dead letter must be stored, not returned as an error: %v", err)
	}
	dl := rec.got[0]
	if dl.Redrivable || dl.Source != deadletter.SourceOutbox || dl.FailureKind != model.DeadLetterKindPermanent ||
		dl.DedupKey != deadletter.UnreadableKey(streams.OutboxDeadLetterV1, "1759665600000-2") {
		t.Fatalf("dl = %+v", dl)
	}
}

func TestOutboxDeadLetterBinding_StoreFailureReturnsError(t *testing.T) {
	rec := &fakeRecorder{err: errors.New("postgres down")}
	if err := NewOutboxDeadLetterBinding(rec, fixedClock{at}).Handle(context.Background(), xmsg("1-0", map[string]any{"x": "y"})); err == nil {
		t.Fatal("a store failure must keep the entry pending")
	}
}

func TestOutboxDeadLetterBinding_FallsBackToClockWithoutAnyTime(t *testing.T) {
	fields := map[string]any{
		"original_event_type": "e", "original_stream": "s:v1", "original_aggregate_id": "a",
		"failure_kind": "permanent", "error": "x", "attempts": "1", "failed_outbox_id": "f",
	}
	rec := &fakeRecorder{}
	if err := NewOutboxDeadLetterBinding(rec, fixedClock{at}).Handle(context.Background(), xmsg("not-an-id", fields)); err != nil {
		t.Fatal(err)
	}
	if !rec.got[0].OriginalAt.Equal(at) {
		t.Fatalf("OriginalAt = %v, want clock %v", rec.got[0].OriginalAt, at)
	}
}
