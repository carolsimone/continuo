package events

import (
	"encoding/json"
	"fmt"
	"time"
)

// RemediationRetryRequestedSchemaVersion is the schema_version of
// remediation.retry_requested:v1 entries.
const RemediationRetryRequestedSchemaVersion = 1

// RemediationRetryRequested is the payload of a remediation.retry_requested:v1
// entry: an operator's request to remediate a rejected release again. The
// remediation_round names the later round, and Rejection is the release's
// stored release.rejected:v1 payload, unchanged.
type RemediationRetryRequested struct {
	ReleaseID        string          `json:"release_id"`
	RemediationRound int             `json:"remediation_round"`
	Rejection        json.RawMessage `json:"rejection"`
}

// RemediationRetryRequestedFields renders p as the Redis fields of one
// remediation.retry_requested:v1 entry. eventID is the id of the outbox row
// that carries the entry.
func RemediationRetryRequestedFields(eventID, tenantID, producer string, occurredAt time.Time, p RemediationRetryRequested) (map[string]any, error) {
	env := Envelope{
		EventID:       eventID,
		TenantID:      tenantID,
		OccurredAt:    occurredAt,
		Producer:      producer,
		SchemaVersion: RemediationRetryRequestedSchemaVersion,
	}
	return env.Fields(p)
}

// DecodeRemediationRetryRequested reads one remediation.retry_requested:v1
// entry. Every error wraps ErrMalformedEnvelope: an unreadable envelope,
// another schema_version, a payload that is not JSON, an empty release_id, a
// remediation_round below 2 (round 1 is the rejection itself, so a retry
// always starts a later round), or a rejection that is missing or not a JSON
// object.
func DecodeRemediationRetryRequested(fields map[string]string) (Envelope, RemediationRetryRequested, error) {
	env, payload, err := ParseEnvelope(fields)
	if err != nil {
		return Envelope{}, RemediationRetryRequested{}, err
	}
	if env.SchemaVersion != RemediationRetryRequestedSchemaVersion {
		return Envelope{}, RemediationRetryRequested{}, fmt.Errorf("%w: schema_version %d", ErrMalformedEnvelope, env.SchemaVersion)
	}
	var p RemediationRetryRequested
	if err := json.Unmarshal(payload, &p); err != nil {
		return Envelope{}, RemediationRetryRequested{}, fmt.Errorf("%w: payload: %v", ErrMalformedEnvelope, err)
	}
	if p.ReleaseID == "" {
		return Envelope{}, RemediationRetryRequested{}, fmt.Errorf("%w: no release_id", ErrMalformedEnvelope)
	}
	if p.RemediationRound < 2 {
		return Envelope{}, RemediationRetryRequested{}, fmt.Errorf("%w: remediation_round %d for release %s, a retry starts round 2 or later", ErrMalformedEnvelope, p.RemediationRound, p.ReleaseID)
	}
	if len(p.Rejection) == 0 || p.Rejection[0] != '{' {
		return Envelope{}, RemediationRetryRequested{}, fmt.Errorf("%w: rejection of release %s is not a JSON object", ErrMalformedEnvelope, p.ReleaseID)
	}
	return env, p, nil
}
