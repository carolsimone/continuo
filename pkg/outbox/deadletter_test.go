package outbox

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestBuildDeadLetterEntry_CarriesOriginContext(t *testing.T) {
	aggID := uuid.New()
	failedID := uuid.New()
	failed := &Entry{
		ID:            failedID,
		AggregateType: "release",
		AggregateID:   aggID,
		EventType:     "compile_requested",
		StreamName:    streams.CompileRequestedV1,
		Payload:       []byte(`{"release_id":"rel-1"}`),
	}
	dl := buildDeadLetterEntry(failed, "test_outbox", nil, model.DeadLetterKindTransientExhausted, errors.New("connection refused"), 10)

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
	if p.OriginalEventType != "compile_requested" || p.OriginalStream != streams.CompileRequestedV1 ||
		p.FailureKind != model.DeadLetterKindTransientExhausted || p.Attempts != 10 ||
		p.FailedOutboxID != failedID.String() || p.Error != "connection refused" {
		t.Fatalf("payload fields wrong: %+v", p)
	}
}

func TestDeadLetterValues_AreScalars(t *testing.T) {
	failed := &Entry{
		ID: uuid.New(), AggregateType: "release", AggregateID: uuid.New(),
		EventType: "compile_requested", StreamName: streams.CompileRequestedV1,
		Payload: []byte(`{"release_id":"rel-1"}`),
	}
	dl := buildDeadLetterEntry(failed, "test_outbox", nil, model.DeadLetterKindPermanent, errors.New("bad payload"), 1)
	values, err := DeadLetterValues(dl)
	if err != nil {
		t.Fatalf("DeadLetterValues: %v", err)
	}
	if values["failure_kind"] != string(model.DeadLetterKindPermanent) {
		t.Fatalf("failure_kind field missing/wrong: %v", values["failure_kind"])
	}
	if values["outbox_entry_id"] != dl.ID.String() {
		t.Fatalf("outbox_entry_id must be injected for consumer dedup")
	}
}

func TestDeadLetterPayload_FailureKindIsTheContractValue(t *testing.T) {
	failed := &Entry{ID: uuid.New(), AggregateID: uuid.New(), StreamName: streams.CompileRequestedV1}
	for kind, wire := range map[model.DeadLetterKind]string{
		model.DeadLetterKindPermanent:          `"failure_kind":"permanent"`,
		model.DeadLetterKindTransientExhausted: `"failure_kind":"transient_exhausted"`,
	} {
		dl := buildDeadLetterEntry(failed, "test_outbox", nil, kind, errors.New("x"), 1)
		assert.Contains(t, string(dl.Payload), wire, "the payload carries the dead_letter_kind wire value")
	}
	assert.Contains(t, streams.All, streams.ConsumerDeadLetterV1)
}
