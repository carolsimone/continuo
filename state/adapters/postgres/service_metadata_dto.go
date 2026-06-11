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

// toServiceMetadataDTOs converts a domain service-metadata map into the
// snake_case serialization carrier used everywhere this value crosses a wire or
// storage boundary (the scheduler_tracker/schedule_catalog JSONB columns and
// the scheduler.started:v1 event payload). A nil map yields an empty (non-nil)
// map so it encodes as `{}` rather than `null`.
func toServiceMetadataDTOs(meta map[string]run.ServiceMetadata) map[string]serviceMetadataDTO {
	dtos := make(map[string]serviceMetadataDTO, len(meta))
	for svc, m := range meta {
		dtos[svc] = serviceMetadataDTO{ManifestVersion: m.ManifestVersion, ImageTag: m.ImageTag}
	}
	return dtos
}

// marshalServiceMetadata encodes a domain service-metadata map into the JSONB
// shape persisted in Postgres. A nil map encodes as an empty JSON object.
func marshalServiceMetadata(meta map[string]run.ServiceMetadata) ([]byte, error) {
	return json.Marshal(toServiceMetadataDTOs(meta))
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
