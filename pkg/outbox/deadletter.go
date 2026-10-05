package outbox

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
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

// OriginalCreatedAtLayout formats DeadLetterPayload.OriginalCreatedAt.
const OriginalCreatedAtLayout = time.RFC3339Nano

// DeadLetterPayload is the JSON body published to streams.OutboxDeadLetterV1 and
// stored in the dead-letter outbox row. Every field is a scalar so it maps
// directly to Redis stream fields. FailureKind is a value of the contract's
// dead_letter_kind vocabulary: permanent, or transient_exhausted. OriginalFields
// is a JSON object of the string fields the failed row would have published, or
// "" when the publisher could not render them; OriginalCreatedAt is the failed
// row's created_at; and OutboxTable names the outbox the row came from.
type DeadLetterPayload struct {
	OriginalEventType   string               `json:"original_event_type"`
	OriginalStream      string               `json:"original_stream"`
	OriginalAggregateID string               `json:"original_aggregate_id"`
	FailureKind         model.DeadLetterKind `json:"failure_kind"`
	Error               string               `json:"error"`
	Attempts            int                  `json:"attempts"`
	FailedOutboxID      string               `json:"failed_outbox_id"`
	OriginalFields      string               `json:"original_fields"`
	OriginalCreatedAt   string               `json:"original_created_at"`
	OutboxTable         string               `json:"outbox_table"`
}

// buildDeadLetterEntry constructs the outbox row that signals a terminal failure
// of `failed`. It is created inside the same transaction that marks `failed`
// failed, so the signal is durable and publishes via the normal machinery —
// immediately if Redis is up, or once it heals. When publisher implements
// Renderer, the fields `failed` would have published are embedded so the event
// can be redriven. The row is transient-classified (a plain XADD); the loop
// guard (aggregate_type sentinel) prevents recursion.
func buildDeadLetterEntry(failed *Entry, table string, publisher Publisher, kind model.DeadLetterKind, cause error, attempts int) *Entry {
	payload := DeadLetterPayload{
		OriginalEventType:   failed.EventType,
		OriginalStream:      failed.StreamName,
		OriginalAggregateID: failed.AggregateID.String(),
		FailureKind:         kind,
		Error:               cause.Error(),
		Attempts:            attempts,
		FailedOutboxID:      failed.ID.String(),
		OriginalCreatedAt:   failed.CreatedAt.UTC().Format(OriginalCreatedAtLayout),
		OutboxTable:         table,
		OriginalFields:      renderOriginal(failed, publisher),
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
	}
}

// renderOriginal returns the JSON object of the string fields failed would have
// published, or "" when the publisher cannot render it.
func renderOriginal(failed *Entry, publisher Publisher) string {
	r, ok := publisher.(Renderer)
	if !ok || r == nil {
		return ""
	}
	values, err := r.Render(failed)
	if err != nil {
		return ""
	}
	delete(values, "outbox_entry_id")
	fields, err := StringifyFields(values)
	if err != nil {
		return ""
	}
	body, err := json.Marshal(fields)
	if err != nil {
		return ""
	}
	return string(body)
}

// DecodeDeadLetterFields reads one outbox.dead_letter:v1 entry. It returns the
// payload and the original fields decoded from original_fields, or nil when the
// entry carries none (an unrenderable row, or an entry written before the field
// existed).
func DecodeDeadLetterFields(fields map[string]string) (DeadLetterPayload, map[string]string, error) {
	attempts, err := strconv.Atoi(fields["attempts"])
	if err != nil {
		return DeadLetterPayload{}, nil, fmt.Errorf("attempts %q: %w", fields["attempts"], err)
	}
	p := DeadLetterPayload{
		OriginalEventType:   fields["original_event_type"],
		OriginalStream:      fields["original_stream"],
		OriginalAggregateID: fields["original_aggregate_id"],
		FailureKind:         model.DeadLetterKind(fields["failure_kind"]),
		Error:               fields["error"],
		Attempts:            attempts,
		FailedOutboxID:      fields["failed_outbox_id"],
		OriginalFields:      fields["original_fields"],
		OriginalCreatedAt:   fields["original_created_at"],
		OutboxTable:         fields["outbox_table"],
	}
	if p.OriginalStream == "" || p.FailedOutboxID == "" {
		return DeadLetterPayload{}, nil, errors.New("original_stream and failed_outbox_id are required")
	}
	if !p.FailureKind.IsValid() {
		return DeadLetterPayload{}, nil, fmt.Errorf("failure_kind %q is not a dead_letter_kind", p.FailureKind)
	}
	if p.OriginalFields == "" {
		return p, nil, nil
	}
	var original map[string]string
	if err := json.Unmarshal([]byte(p.OriginalFields), &original); err != nil {
		return DeadLetterPayload{}, nil, fmt.Errorf("original_fields: %w", err)
	}
	return p, original, nil
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
