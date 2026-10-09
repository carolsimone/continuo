package events

import (
	"encoding/json"
	"fmt"
)

// DecodeReleasePromoted reads one release.promoted:v2 entry. Every error wraps
// ErrMalformedEnvelope: an unreadable envelope, another schema_version, a
// payload that is not JSON, no release_id, a promotion_seq below 1, or no
// topology_uri or topology_sha256.
func DecodeReleasePromoted(fields map[string]string) (Envelope, ReleasePromoted, error) {
	env, payload, err := ParseEnvelope(fields)
	if err != nil {
		return Envelope{}, ReleasePromoted{}, err
	}
	if env.SchemaVersion != ReleasePromotedSchemaVersion {
		return Envelope{}, ReleasePromoted{}, fmt.Errorf("%w: schema_version %d", ErrMalformedEnvelope, env.SchemaVersion)
	}
	var p ReleasePromoted
	if err := json.Unmarshal(payload, &p); err != nil {
		return Envelope{}, ReleasePromoted{}, fmt.Errorf("%w: payload: %v", ErrMalformedEnvelope, err)
	}
	switch {
	case p.ReleaseID == "":
		return Envelope{}, ReleasePromoted{}, fmt.Errorf("%w: empty release_id", ErrMalformedEnvelope)
	case p.PromotionSeq < 1:
		return Envelope{}, ReleasePromoted{}, fmt.Errorf("%w: promotion_seq %d for %s", ErrMalformedEnvelope, p.PromotionSeq, p.ReleaseID)
	case p.TopologyURI == "" || p.TopologySHA256 == "":
		return Envelope{}, ReleasePromoted{}, fmt.Errorf("%w: %s names no topology artifact", ErrMalformedEnvelope, p.ReleaseID)
	}
	return env, p, nil
}
