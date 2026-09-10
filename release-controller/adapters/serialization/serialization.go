// Package serialization holds the wire- and persistence-facing DTOs for
// release-controller's boundary types, keeping the domain packages (pipeline,
// release) free of json struct tags. The Postgres repositories (JSONB columns),
// the HTTP read handlers (response bodies), and the Redis manifest binding all
// map through these DTOs, so the on-disk and on-the-wire byte shapes are fixed
// here rather than on the domain types.
package serialization

import (
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
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

// TransitionDTO is the JSON shape of one pipeline.Transition as it is stored in
// the release_pipeline_runs.transitions JSONB column and returned in the
// transitions field of the GET /releases and GET /verification-runs responses.
// To is a plain string: pipeline.Status marshals identically, and the DTO keeps
// the domain type out of the wire contract.
type TransitionDTO struct {
	To string    `json:"to"`
	At time.Time `json:"at"`
}

// TransitionsFromDomain maps domain transitions to DTOs, preserving the nil vs
// non-nil-empty distinction so the marshalled bytes are unchanged.
func TransitionsFromDomain(in []pipeline.Transition) []TransitionDTO {
	if in == nil {
		return nil
	}
	out := make([]TransitionDTO, len(in))
	for i, t := range in {
		out[i] = TransitionDTO{To: string(t.To), At: t.At}
	}
	return out
}

// TransitionsToDomain maps decoded DTOs back to domain transitions.
func TransitionsToDomain(in []TransitionDTO) []pipeline.Transition {
	if in == nil {
		return nil
	}
	out := make([]pipeline.Transition, len(in))
	for i, d := range in {
		out[i] = pipeline.Transition{To: pipeline.Status(d.To), At: d.At}
	}
	return out
}

// NodeDTO is the JSON shape of one release.Node. A topology of these is stored
// in the release_pipeline_runs.candidate_topology and current_prod.topology_snapshot
// JSONB columns and carried in the topology field of the manifest.loaded.candidate:v1
// message release-controller consumes.
type NodeDTO struct {
	UniqueID             string   `json:"unique_id"`
	SchemaName           string   `json:"schema_name"`
	TableName            string   `json:"table_name"`
	ResolvedRelationID   string   `json:"resolved_relation_id"`
	ServiceName          string   `json:"service_name"`
	NodeType             string   `json:"node_type"`
	ContentHash          string   `json:"content_hash"`
	TestCount            int      `json:"test_count"`
	ImageTag             string   `json:"image_tag"`
	UpstreamUniqueIDs    []string `json:"upstream_unique_ids"`
	Schedule             string   `json:"schedule"`
	OriginalFilePath     string   `json:"original_file_path"`
	CandidateArtifactURI string   `json:"candidate_artifact_uri,omitempty"`
}

// TopologyDTO is the JSON shape of a release.Topology.
type TopologyDTO []NodeDTO

// TopologyFromDomain maps a domain topology to its DTO, preserving the nil vs
// non-nil-empty distinction so the marshalled bytes are unchanged.
func TopologyFromDomain(in release.Topology) TopologyDTO {
	if in == nil {
		return nil
	}
	out := make(TopologyDTO, len(in))
	for i, n := range in {
		out[i] = NodeDTO{
			UniqueID:             n.UniqueID,
			SchemaName:           n.SchemaName,
			TableName:            n.TableName,
			ResolvedRelationID:   n.ResolvedRelationID,
			ServiceName:          n.ServiceName,
			NodeType:             n.NodeType,
			ContentHash:          n.ContentHash,
			TestCount:            n.TestCount,
			ImageTag:             n.ImageTag,
			UpstreamUniqueIDs:    n.UpstreamUniqueIDs,
			Schedule:             n.Schedule,
			OriginalFilePath:     n.OriginalFilePath,
			CandidateArtifactURI: n.CandidateArtifactURI,
		}
	}
	return out
}

// ToDomain maps a decoded topology DTO back to the domain topology.
func (t TopologyDTO) ToDomain() release.Topology {
	if t == nil {
		return nil
	}
	out := make(release.Topology, len(t))
	for i, d := range t {
		out[i] = release.Node{
			UniqueID:             d.UniqueID,
			SchemaName:           d.SchemaName,
			TableName:            d.TableName,
			ResolvedRelationID:   d.ResolvedRelationID,
			ServiceName:          d.ServiceName,
			NodeType:             d.NodeType,
			ContentHash:          d.ContentHash,
			TestCount:            d.TestCount,
			ImageTag:             d.ImageTag,
			UpstreamUniqueIDs:    d.UpstreamUniqueIDs,
			Schedule:             d.Schedule,
			OriginalFilePath:     d.OriginalFilePath,
			CandidateArtifactURI: d.CandidateArtifactURI,
		}
	}
	return out
}
