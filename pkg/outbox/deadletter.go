package outbox

import (
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// Dead-letter markers. A dead-letter row is a normal outbox row whose event_type
// and aggregate_type are these sentinels, so the processor can recognise one and
// never dead-letter a dead-letter (loop guard).
const (
	DeadLetterEventType     = "outbox_dead_letter"
	DeadLetterAggregateType = "outbox_dead_letter"
)

// Failure kinds recorded on a terminal row's dead-letter.
const (
	FailureKindPermanent          = "permanent"
	FailureKindTransientExhausted = "transient_exhausted"
)

// DeadLetterPayload is the JSON body published to streams.OutboxDeadLetterV1 and
// stored in the dead-letter outbox row. Every field is a scalar so it maps
// directly to Redis stream fields.
type DeadLetterPayload struct {
	OriginalEventType   string `json:"original_event_type"`
	OriginalStream      string `json:"original_stream"`
	OriginalAggregateID string `json:"original_aggregate_id"`
	FailureKind         string `json:"failure_kind"`
	Error               string `json:"error"`
	Attempts            int    `json:"attempts"`
	FailedOutboxID      string `json:"failed_outbox_id"`
}

// buildDeadLetterEntry constructs the outbox row that signals a terminal failure
// of `failed`. It is created inside the same transaction that marks `failed`
// failed, so the signal is durable and publishes via the normal machinery —
// immediately if Redis is up, or once it heals. The row is transient-classified
// (a plain XADD); the loop guard (aggregate_type sentinel) prevents recursion.
func buildDeadLetterEntry(failed *Entry, kind string, cause error, attempts int) *Entry {
	payload := DeadLetterPayload{
		OriginalEventType:   failed.EventType,
		OriginalStream:      failed.StreamName,
		OriginalAggregateID: failed.AggregateID.String(),
		FailureKind:         kind,
		Error:               cause.Error(),
		Attempts:            attempts,
		FailedOutboxID:      failed.ID.String(),
	}
	body, _ := json.Marshal(payload) // scalar struct; marshal cannot fail
	return &Entry{
		ID:            uuid.New(),
		AggregateType: DeadLetterAggregateType,
		AggregateID:   failed.AggregateID,
		EventType:     DeadLetterEventType,
		Payload:       body,
		StreamName:    streams.OutboxDeadLetterV1,
		Status:        "pending",
		MaxRetries:    DefaultMaxRetries,
	}
}

// DeadLetterValues decodes a dead-letter row's payload into a scalar field map
// for XADD, injecting outbox_entry_id for consumer-side dedup. Publishers route
// the DeadLetterEventType through this so a typed-switch publisher does not need
// a bespoke case.
func DeadLetterValues(entry *Entry) (map[string]interface{}, error) {
	var fields map[string]interface{}
	if err := json.Unmarshal(entry.Payload, &fields); err != nil {
		return nil, fmt.Errorf("unmarshal dead-letter payload: %w", err)
	}
	fields["outbox_entry_id"] = entry.ID.String()
	return fields, nil
}
