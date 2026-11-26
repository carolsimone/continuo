package events

import (
	"encoding/json"
	"fmt"
)

// DecodeReleaseParsed reads one release.parsed:v1 entry.
// Every error wraps ErrMalformedEnvelope: an unreadable envelope, another
// schema_version, a payload that is not JSON, no release_id, a status other
// than ok or failed, or an ok entry that names no topology artifact. The
// contract bundle fields, continuo_contracts, the two durations and the count
// by family are optional: an entry from a producer that does not write them
// decodes with empty values.
func DecodeReleaseParsed(fields map[string]string) (Envelope, ReleaseParsed, error) {
	env, payload, err := ParseEnvelope(fields)
	if err != nil {
		return Envelope{}, ReleaseParsed{}, err
	}
	if env.SchemaVersion != ReleaseParsedSchemaVersion {
		return Envelope{}, ReleaseParsed{}, fmt.Errorf("%w: schema_version %d", ErrMalformedEnvelope, env.SchemaVersion)
	}
	var p ReleaseParsed
	if err := json.Unmarshal(payload, &p); err != nil {
		return Envelope{}, ReleaseParsed{}, fmt.Errorf("%w: payload: %v", ErrMalformedEnvelope, err)
	}
	if p.ReleaseID == "" {
		return Envelope{}, ReleaseParsed{}, fmt.Errorf("%w: empty release_id", ErrMalformedEnvelope)
	}
	switch p.Status {
	case ManifestStatusOK:
		if p.TopologyURI == "" || p.TopologySHA256 == "" {
			return Envelope{}, ReleaseParsed{}, fmt.Errorf("%w: ok entry for %s names no topology artifact", ErrMalformedEnvelope, p.ReleaseID)
		}
	case ManifestStatusFailed:
	default:
		return Envelope{}, ReleaseParsed{}, fmt.Errorf("%w: status %q", ErrMalformedEnvelope, p.Status)
	}
	return env, p, nil
}
