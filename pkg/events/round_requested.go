package events

import (
	"encoding/json"
	"fmt"
	"time"
)

// RoundRequestedSchemaVersion is the schema_version of
// round.requested:v1 entries.
const RoundRequestedSchemaVersion = 1

// RoundRequested is the payload of a round.requested:v1
// entry: an operator's request to remediate a rejected release again. The
// round names the later round, and Rejection is the release's
// stored release.rejected:v1 payload, unchanged.
type RoundRequested struct {
	ReleaseID string          `json:"release_id"`
	Round     int             `json:"round"`
	Rejection json.RawMessage `json:"rejection"`
}

// RoundRequestedFields renders p as the Redis fields of one
// round.requested:v1 entry. eventID is the id of the outbox row
// that carries the entry.
func RoundRequestedFields(eventID, tenantID, producer string, occurredAt time.Time, p RoundRequested) (map[string]any, error) {
	env := Envelope{
		EventID:       eventID,
		TenantID:      tenantID,
		OccurredAt:    occurredAt,
		Producer:      producer,
		SchemaVersion: RoundRequestedSchemaVersion,
	}
	return env.Fields(p)
}

// DecodeRoundRequested reads one round.requested:v1
// entry. Every error wraps ErrMalformedEnvelope: an unreadable envelope,
// another schema_version, a payload that is not JSON, an empty release_id, a
// round below 2 (round 1 is the rejection itself, so a retry
// always starts a later round), or a rejection that is missing or not a JSON
// object.
func DecodeRoundRequested(fields map[string]string) (Envelope, RoundRequested, error) {
	env, payload, err := ParseEnvelope(fields)
	if err != nil {
		return Envelope{}, RoundRequested{}, err
	}
	if env.SchemaVersion != RoundRequestedSchemaVersion {
		return Envelope{}, RoundRequested{}, fmt.Errorf("%w: schema_version %d", ErrMalformedEnvelope, env.SchemaVersion)
	}
	var p RoundRequested
	if err := json.Unmarshal(payload, &p); err != nil {
		return Envelope{}, RoundRequested{}, fmt.Errorf("%w: payload: %v", ErrMalformedEnvelope, err)
	}
	if p.ReleaseID == "" {
		return Envelope{}, RoundRequested{}, fmt.Errorf("%w: no release_id", ErrMalformedEnvelope)
	}
	if p.Round < 2 {
		return Envelope{}, RoundRequested{}, fmt.Errorf("%w: round %d for release %s, a retry starts round 2 or later", ErrMalformedEnvelope, p.Round, p.ReleaseID)
	}
	if len(p.Rejection) == 0 || p.Rejection[0] != '{' {
		return Envelope{}, RoundRequested{}, fmt.Errorf("%w: rejection of release %s is not a JSON object", ErrMalformedEnvelope, p.ReleaseID)
	}
	return env, p, nil
}
