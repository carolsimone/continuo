package events

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
)

func TestDecodeConsumerDeadLetter_RoundTripsTheWriter(t *testing.T) {
	at := time.Date(2026, 10, 5, 12, 0, 0, 123456000, time.UTC)
	values, err := ConsumerDeadLetterFields(DeadLetteredMessage{
		TenantID: "default", Producer: "state", OccurredAt: at,
		Stream: "node.updated:v1", Group: "orchestrator-node-updated", MessageID: "1759665600000-0",
		Fields: map[string]string{"a": "1"}, FailureKind: model.DeadLetterKindPermanent, Error: "boom", DeliveryCount: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	fields := map[string]string{}
	for k, v := range values {
		fields[k] = v.(string)
	}
	env, dl, err := DecodeConsumerDeadLetter(fields)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.Producer != "state" || !env.OccurredAt.Equal(at) || env.SchemaVersion != 1 || env.TenantID != "default" {
		t.Fatalf("envelope = %+v", env)
	}
	if dl.OriginalStream != "node.updated:v1" || dl.OriginalGroup != "orchestrator-node-updated" ||
		dl.OriginalMessageID != "1759665600000-0" || dl.Fields["a"] != "1" ||
		dl.FailureKind != model.DeadLetterKindPermanent || dl.Error != "boom" || dl.DeliveryCount != 1 {
		t.Fatalf("payload = %+v", dl)
	}
}

// The golden fixture stores each stream field as JSON, with the payload as a
// nested object; the wire form carries the payload as a JSON string.
func TestDecodeConsumerDeadLetter_GoldenFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/consumer_dead_letter_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var fx struct {
		Input struct {
			OccurredAt        time.Time         `json:"occurred_at"`
			OriginalStream    string            `json:"original_stream"`
			OriginalGroup     string            `json:"original_group"`
			OriginalMessageID string            `json:"original_message_id"`
			Fields            map[string]string `json:"fields"`
			FailureKind       string            `json:"failure_kind"`
			DeliveryCount     int64             `json:"delivery_count"`
		} `json:"input"`
		Fields map[string]json.RawMessage `json:"fields"`
	}
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatal(err)
	}
	fields := map[string]string{}
	for k, v := range fx.Fields {
		if k == "payload" {
			fields[k] = string(v)
			continue
		}
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			t.Fatalf("fixture field %s: %v", k, err)
		}
		fields[k] = s
	}
	env, dl, err := DecodeConsumerDeadLetter(fields)
	if err != nil {
		t.Fatalf("decode golden fixture: %v", err)
	}
	if env.Producer != "state" || env.TenantID != "default" || !env.OccurredAt.Equal(fx.Input.OccurredAt) {
		t.Fatalf("envelope = %+v", env)
	}
	if dl.OriginalStream != fx.Input.OriginalStream || dl.OriginalGroup != fx.Input.OriginalGroup ||
		dl.OriginalMessageID != fx.Input.OriginalMessageID || dl.FailureKind != model.DeadLetterKind(fx.Input.FailureKind) ||
		dl.DeliveryCount != fx.Input.DeliveryCount || dl.Fields["status"] != fx.Input.Fields["status"] {
		t.Fatalf("payload = %+v", dl)
	}
}

func TestDecodeConsumerDeadLetter_MissingPayloadIsMalformed(t *testing.T) {
	_, _, err := DecodeConsumerDeadLetter(map[string]string{"event_id": "x", "schema_version": "1"})
	if !errors.Is(err, ErrMalformedEnvelope) {
		t.Fatalf("err = %v, want ErrMalformedEnvelope", err)
	}
}

func TestStreamIDTime(t *testing.T) {
	got, ok := StreamIDTime("1759665600000-3")
	if !ok || !got.Equal(time.UnixMilli(1759665600000).UTC()) {
		t.Fatalf("got %v %v", got, ok)
	}
	if _, ok := StreamIDTime("not-an-id"); ok {
		t.Fatal("want !ok for a non-id")
	}
}
