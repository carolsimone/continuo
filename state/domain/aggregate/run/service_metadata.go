package run

import "encoding/json"

// ServiceMetadata is the per-service topology snapshot pinned on a Run at
// activation time. Stored as a JSON map keyed by service name on
// scheduler_tracker.service_metadata.
type ServiceMetadata struct {
	ManifestVersion string `json:"manifest_version"`
	ImageTag        string `json:"image_tag"`
}

// UnmarshalServiceMetadata is a convenience used by hydration in the postgres
// adapter to decode the JSONB blob into the typed map. Returns an empty map
// (never nil) on empty/invalid input so callers can iterate without nil checks.
func UnmarshalServiceMetadata(raw json.RawMessage) map[string]ServiceMetadata {
	if len(raw) == 0 {
		return map[string]ServiceMetadata{}
	}
	var m map[string]ServiceMetadata
	if err := json.Unmarshal(raw, &m); err != nil || m == nil {
		return map[string]ServiceMetadata{}
	}
	return m
}
