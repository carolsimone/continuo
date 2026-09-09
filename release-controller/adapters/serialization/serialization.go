// Package serialization holds the wire- and persistence-facing DTOs for
// release-controller's boundary types, keeping the domain packages (pipeline,
// release) free of json struct tags. The Postgres repositories (JSONB columns),
// the HTTP read handlers (response bodies), and the Redis manifest binding all
// map through these DTOs, so the on-disk and on-the-wire byte shapes are fixed
// here rather than on the domain types.
package serialization

import (
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
)

// NodeValidationResultDTO is the JSON shape of one pipeline.NodeValidationResult
// as it is stored in the release_pipeline_runs.per_node_results JSONB column and
// returned in the GET /releases and GET /verification-runs response bodies.
type NodeValidationResultDTO struct {
	Stage         string `json:"stage,omitempty"`
	NodeID        string `json:"node_id"`
	Status        string `json:"status"`
	DBTLogURI     string `json:"dbt_log_uri,omitempty"`
	RunResultsURI string `json:"run_results_uri,omitempty"`
	DurationMS    int64  `json:"duration_ms,omitempty"`
	FilePath      string `json:"file_path,omitempty"`
	NodeType      string `json:"node_type,omitempty"`
}

// NodeValidationResultsFromDomain maps a slice of domain results to DTOs. A nil
// input maps to a nil slice so it serialises as JSON null, exactly as a marshal
// of the domain slice did; a non-nil empty slice stays a non-nil empty slice so
// it serialises as [].
func NodeValidationResultsFromDomain(in []pipeline.NodeValidationResult) []NodeValidationResultDTO {
	if in == nil {
		return nil
	}
	out := make([]NodeValidationResultDTO, len(in))
	for i, n := range in {
		out[i] = NodeValidationResultDTO{
			Stage:         n.Stage,
			NodeID:        n.NodeID,
			Status:        n.Status,
			DBTLogURI:     n.DBTLogURI,
			RunResultsURI: n.RunResultsURI,
			DurationMS:    n.DurationMS,
			FilePath:      n.FilePath,
			NodeType:      n.NodeType,
		}
	}
	return out
}

// NodeValidationResultsToDomain maps decoded DTOs back to domain results, with
// the same nil/empty distinction as NodeValidationResultsFromDomain.
func NodeValidationResultsToDomain(in []NodeValidationResultDTO) []pipeline.NodeValidationResult {
	if in == nil {
		return nil
	}
	out := make([]pipeline.NodeValidationResult, len(in))
	for i, d := range in {
		out[i] = pipeline.NodeValidationResult{
			Stage:         d.Stage,
			NodeID:        d.NodeID,
			Status:        d.Status,
			DBTLogURI:     d.DBTLogURI,
			RunResultsURI: d.RunResultsURI,
			DurationMS:    d.DurationMS,
			FilePath:      d.FilePath,
			NodeType:      d.NodeType,
		}
	}
	return out
}
