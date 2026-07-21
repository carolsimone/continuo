package outbox

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

func TestBuildDeadLetterEntry_CarriesOriginContext(t *testing.T) {
	aggID := uuid.New()
	failedID := uuid.New()
	failed := &Entry{
		ID:            failedID,
		AggregateType: "release",
		AggregateID:   aggID,
		EventType:     "compile_requested",
		StreamName:    "compile.requested:v1",
		Payload:       []byte(`{"release_id":"rel-1"}`),
	}
	dl := buildDeadLetterEntry(failed, FailureKindTransientExhausted, errors.New("connection refused"), 10)

	if dl.EventType != DeadLetterEventType {
		t.Fatalf("event_type=%q want %q", dl.EventType, DeadLetterEventType)
	}
	if dl.AggregateType != DeadLetterAggregateType {
		t.Fatalf("aggregate_type=%q want %q (loop guard)", dl.AggregateType, DeadLetterAggregateType)
	}
	if dl.StreamName != streams.OutboxDeadLetterV1 {
		t.Fatalf("stream=%q want %q", dl.StreamName, streams.OutboxDeadLetterV1)
	}
	if dl.AggregateID != aggID {
		t.Fatalf("aggregate_id not carried from failed row")
	}
	var p DeadLetterPayload
	if err := json.Unmarshal(dl.Payload, &p); err != nil {
		t.Fatalf("payload not valid JSON: %v", err)
	}
	if p.OriginalEventType != "compile_requested" || p.OriginalStream != "compile.requested:v1" ||
		p.FailureKind != FailureKindTransientExhausted || p.Attempts != 10 ||
		p.FailedOutboxID != failedID.String() || p.Error != "connection refused" {
		t.Fatalf("payload fields wrong: %+v", p)
	}
}

func TestDeadLetterValues_AreScalars(t *testing.T) {
	failed := &Entry{
		ID: uuid.New(), AggregateType: "release", AggregateID: uuid.New(),
		EventType: "compile_requested", StreamName: "compile.requested:v1",
		Payload: []byte(`{"release_id":"rel-1"}`),
	}
	dl := buildDeadLetterEntry(failed, FailureKindPermanent, errors.New("bad payload"), 1)
	values, err := DeadLetterValues(dl)
	if err != nil {
		t.Fatalf("DeadLetterValues: %v", err)
	}
	if values["failure_kind"] != FailureKindPermanent {
		t.Fatalf("failure_kind field missing/wrong: %v", values["failure_kind"])
	}
	if values["outbox_entry_id"] != dl.ID.String() {
		t.Fatalf("outbox_entry_id must be injected for consumer dedup")
	}
}
