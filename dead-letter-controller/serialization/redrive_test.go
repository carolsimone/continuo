package serialization

import (
	"testing"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/google/uuid"
)

func TestEncodeRedrive_OutboxEntryID(t *testing.T) {
	for _, c := range []struct {
		source deadletter.Source
		want   string
	}{
		{deadletter.SourceOutbox, "orig-42"},
		{deadletter.SourceConsumer, ""},
		{deadletter.SourceQuarantine, ""},
	} {
		dl := deadletter.DeadLetter{ID: uuid.New(), Source: c.source, Group: "g", FailedOutboxID: "orig-42",
			Fields: map[string]string{"k": "v"}}
		body, err := EncodeRedrive(dl)
		if err != nil {
			t.Fatal(err)
		}
		p, err := DecodeRedrive(body)
		if err != nil {
			t.Fatalf("%s: %v", c.source, err)
		}
		if p.OutboxEntryID != c.want {
			t.Errorf("%s: OutboxEntryID = %q, want %q", c.source, p.OutboxEntryID, c.want)
		}
	}
}
