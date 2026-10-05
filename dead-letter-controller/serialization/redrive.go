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
// the one group the entry is addressed to (empty for every group), and the
// dead letter it came from.
type RedrivePayload struct {
	Fields       map[string]string `json:"fields"`
	RedriveGroup string            `json:"redrive_group,omitempty"`
	RedrivenFrom string            `json:"redriven_from"`
}

// EncodeRedrive builds the outbox body that redrives dl.
func EncodeRedrive(dl deadletter.DeadLetter) ([]byte, error) {
	return json.Marshal(RedrivePayload{Fields: dl.Fields, RedriveGroup: dl.TargetGroup(), RedrivenFrom: dl.ID.String()})
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
