package events

import (
	"encoding/json"
	"fmt"
)

// DecodeManifestLoadedCandidate reads one manifest.loaded.candidate:v2 entry.
// Every error wraps ErrMalformedEnvelope: an unreadable envelope, another
// schema_version, a payload that is not JSON, no release_id, a status other
// than ok or failed, or an ok entry that names no topology artifact.
func DecodeManifestLoadedCandidate(fields map[string]string) (Envelope, ManifestLoadedCandidate, error) {
	env, payload, err := ParseEnvelope(fields)
	if err != nil {
		return Envelope{}, ManifestLoadedCandidate{}, err
	}
	if env.SchemaVersion != ManifestLoadedCandidateSchemaVersion {
		return Envelope{}, ManifestLoadedCandidate{}, fmt.Errorf("%w: schema_version %d", ErrMalformedEnvelope, env.SchemaVersion)
	}
	var p ManifestLoadedCandidate
	if err := json.Unmarshal(payload, &p); err != nil {
		return Envelope{}, ManifestLoadedCandidate{}, fmt.Errorf("%w: payload: %v", ErrMalformedEnvelope, err)
	}
	if p.ReleaseID == "" {
		return Envelope{}, ManifestLoadedCandidate{}, fmt.Errorf("%w: empty release_id", ErrMalformedEnvelope)
	}
	switch p.Status {
	case ManifestStatusOK:
		if p.TopologyURI == "" || p.TopologySHA256 == "" {
			return Envelope{}, ManifestLoadedCandidate{}, fmt.Errorf("%w: ok entry for %s names no topology artifact", ErrMalformedEnvelope, p.ReleaseID)
		}
	case ManifestStatusFailed:
	default:
		return Envelope{}, ManifestLoadedCandidate{}, fmt.Errorf("%w: status %q", ErrMalformedEnvelope, p.Status)
	}
	return env, p, nil
}
