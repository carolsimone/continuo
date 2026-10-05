// Package serialization holds the wire shapes dead-letter-controller writes to
// its outbox.
package serialization

import (
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
)

// EventTypeRedrive and AggregateTypeDeadLetter mark the outbox row of a redrive.
const (
	EventTypeRedrive        = "dead_letter_redrive"
	AggregateTypeDeadLetter = "dead_letter"
)

// RedrivePayload is the outbox body of a redrive: the stream fields to publish,
// the one group the entry is addressed to (empty for every group), the dead
// letter it came from and, for an outbox-source dead letter, the outbox_entry_id
// of the original event. A consumer or quarantine redrive carries no
// OutboxEntryID: those redrives are meant to be reprocessed, so they publish
// under a fresh id.
type RedrivePayload struct {
	Fields        map[string]string `json:"fields"`
	RedriveGroup  string            `json:"redrive_group,omitempty"`
	RedrivenFrom  string            `json:"redriven_from"`
	OutboxEntryID string            `json:"outbox_entry_id,omitempty"`
}

// EncodeRedrive builds the outbox body that redrives dl. An outbox-source dead
// letter keeps the original event's outbox_entry_id (its FailedOutboxID), so a
// consumer that already processed the event after an ambiguous publish
// deduplicates the redrive.
func EncodeRedrive(dl deadletter.DeadLetter) ([]byte, error) {
	p := RedrivePayload{Fields: dl.Fields, RedriveGroup: dl.TargetGroup(), RedrivenFrom: dl.ID.String()}
	if dl.Source == deadletter.SourceOutbox {
		p.OutboxEntryID = dl.FailedOutboxID
	}
	return json.Marshal(p)
}

// DecodeRedrive reads an outbox body EncodeRedrive wrote.
func DecodeRedrive(b []byte) (RedrivePayload, error) {
	var p RedrivePayload
	if err := json.Unmarshal(b, &p); err != nil {
		return RedrivePayload{}, fmt.Errorf("decode redrive payload: %w", err)
	}
	if len(p.Fields) == 0 || p.RedrivenFrom == "" {
		return RedrivePayload{}, fmt.Errorf("decode redrive payload: fields and redriven_from are required")
	}
	return p, nil
}
