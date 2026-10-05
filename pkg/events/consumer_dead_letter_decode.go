package events

import (
	"encoding/json"
	"fmt"
)

// DecodeConsumerDeadLetter reads one consumer.dead_letter:v1 entry.
func DecodeConsumerDeadLetter(fields map[string]string) (Envelope, ConsumerDeadLetter, error) {
	env, payload, err := ParseEnvelope(fields)
	if err != nil {
		return Envelope{}, ConsumerDeadLetter{}, err
	}
	if env.SchemaVersion != ConsumerDeadLetterSchemaVersion {
		return Envelope{}, ConsumerDeadLetter{}, fmt.Errorf("%w: schema_version %d", ErrMalformedEnvelope, env.SchemaVersion)
	}
	var dl ConsumerDeadLetter
	if err := json.Unmarshal(payload, &dl); err != nil {
		return Envelope{}, ConsumerDeadLetter{}, fmt.Errorf("%w: payload: %v", ErrMalformedEnvelope, err)
	}
	return env, dl, nil
}
