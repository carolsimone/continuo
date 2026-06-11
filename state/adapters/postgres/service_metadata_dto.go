package postgres

import (
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
)

// serviceMetadataDTO is the adapter-side serialization carrier for
// run.ServiceMetadata. The JSON field names live here, keeping the domain value
// object free of encoding tags. It mirrors the shape stored in the
// scheduler_tracker.service_metadata and schedule_catalog.service_metadata
// JSONB columns.
type serviceMetadataDTO struct {
	ManifestVersion string `json:"manifest_version"`
	ImageTag        string `json:"image_tag"`
}

// marshalServiceMetadata encodes a domain service-metadata map into the JSONB
// shape persisted in Postgres. A nil map encodes as an empty JSON object.
func marshalServiceMetadata(meta map[string]run.ServiceMetadata) ([]byte, error) {
	dtos := make(map[string]serviceMetadataDTO, len(meta))
	for svc, m := range meta {
		dtos[svc] = serviceMetadataDTO{ManifestVersion: m.ManifestVersion, ImageTag: m.ImageTag}
	}
	return json.Marshal(dtos)
}

// unmarshalServiceMetadata decodes the JSONB service-metadata blob into the
// typed domain map. Empty input yields an empty (never nil) map.
func unmarshalServiceMetadata(raw []byte) (map[string]run.ServiceMetadata, error) {
	if len(raw) == 0 {
		return map[string]run.ServiceMetadata{}, nil
	}
	var dtos map[string]serviceMetadataDTO
	if err := json.Unmarshal(raw, &dtos); err != nil {
		return nil, fmt.Errorf("decode service_metadata: %w", err)
	}
	out := make(map[string]run.ServiceMetadata, len(dtos))
	for svc, d := range dtos {
		out[svc] = run.ServiceMetadata{ManifestVersion: d.ManifestVersion, ImageTag: d.ImageTag}
	}
	return out, nil
}
