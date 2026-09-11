package serialization

import (
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/release-controller/service/ports"
)

// ReleaseRejectedJSON renders release.rejected:v1 bodies. It holds the wire
// keys of every rejection shape in one place, so the four legs that reject a
// candidate pass values and never a byte shape.
type ReleaseRejectedJSON struct{}

var _ ports.ReleaseRejectedEncoder = ReleaseRejectedJSON{}

// parseNodeDTO is one failed parse node: the fields the compile leg emits,
// plus the failure kind and the parser's own detail, since a parse failure
// produces no log to fetch. Every key is always present — the parse leg knows
// all of them or knows the node is not locatable, and an empty string says so.
type parseNodeDTO struct {
	NodeID   string `json:"node_id"`
	Status   string `json:"status"`
	Kind     string `json:"kind"`
	Detail   string `json:"detail"`
	FilePath string `json:"file_path"`
	Service  string `json:"service"`
	NodeType string `json:"node_type"`
}

// compileNodeDTO is one compile-leg result. It omits duration_ms (irrelevant
// for compile) and the source location (the remediation service resolves it
// when it reads the S3 logs).
type compileNodeDTO struct {
	NodeID        string `json:"node_id"`
	Status        string `json:"status"`
	DBTLogURI     string `json:"dbt_log_uri,omitempty"`
	RunResultsURI string `json:"run_results_uri,omitempty"`
}

// seedBuildNodeDTO is one seed-build-leg result. file_path and service carry
// the source location from the candidate topology so the remediation agent can
// locate the seed source file without querying GetNodeLocation, which only
// holds promoted topology and cannot find newly-added seeds.
type seedBuildNodeDTO struct {
	NodeID        string `json:"node_id"`
	Status        string `json:"status"`
	DBTLogURI     string `json:"dbt_log_uri,omitempty"`
	RunResultsURI string `json:"run_results_uri,omitempty"`
	FilePath      string `json:"file_path,omitempty"`
	Service       string `json:"service,omitempty"`
}

// changedAncestorDTO is one changed upstream of a failing validation node,
// with the location this candidate declares for it.
type changedAncestorDTO struct {
	NodeID   string `json:"node_id"`
	FilePath string `json:"file_path,omitempty"`
	Service  string `json:"service,omitempty"`
	Depth    int    `json:"depth"`
}

// validationNodeDTO is one validation-leg result: the node's artifacts, the
// candidate facts that locate its source, and — for a failing node — the
// changed ancestors that may be the root cause.
type validationNodeDTO struct {
	NodeID               string               `json:"node_id"`
	Status               string               `json:"status"`
	DBTLogURI            string               `json:"dbt_log_uri,omitempty"`
	RunResultsURI        string               `json:"run_results_uri,omitempty"`
	CandidateArtifactURI string               `json:"candidate_artifact_uri,omitempty"`
	NodeType             string               `json:"node_type,omitempty"`
	FilePath             string               `json:"file_path,omitempty"`
	Service              string               `json:"service,omitempty"`
	ChangedAncestors     []changedAncestorDTO `json:"changed_ancestors,omitempty"`
}

// duplicateNodeDTO is one duplicate-table collision: the claimant a rename
// should target, the contested relation itself, and the competing claimant a
// rename must move away from.
type duplicateNodeDTO struct {
	NodeID        string `json:"node_id"`
	Status        string `json:"status"`
	Service       string `json:"service"`
	FilePath      string `json:"file_path"`
	NodeType      string `json:"node_type"`
	RelationID    string `json:"relation_id"`
	OtherService  string `json:"other_service"`
	OtherFilePath string `json:"other_file_path"`
}

// parseRejectedDTO is the release.rejected:v1 body of a parse failure.
type parseRejectedDTO struct {
	ReleaseID     string         `json:"release_id"`
	Stage         string         `json:"stage"`
	Reason        string         `json:"reason"`
	ErrorDetail   string         `json:"error_detail"`
	FailingNodes  []string       `json:"failing_nodes"`
	PerNode       []parseNodeDTO `json:"per_node"`
	Repo          string         `json:"repo"`
	CommitSHA     string         `json:"commit_sha"`
	CodeBundleURI string         `json:"code_bundle_uri"`
}

// compileRejectedDTO is the release.rejected:v1 body of a compile failure.
type compileRejectedDTO struct {
	ReleaseID     string           `json:"release_id"`
	Stage         string           `json:"stage"`
	Reason        string           `json:"reason"`
	ErrorDetail   string           `json:"error_detail"`
	FailingNodes  []string         `json:"failing_nodes"`
	PerNode       []compileNodeDTO `json:"per_node"`
	Repo          string           `json:"repo"`
	CommitSHA     string           `json:"commit_sha"`
	CodeBundleURI string           `json:"code_bundle_uri"`
}

// seedBuildRejectedDTO is the release.rejected:v1 body of a seed-build
// failure. It additionally names the candidate schema the seeds were being
// built into.
type seedBuildRejectedDTO struct {
	ReleaseID       string             `json:"release_id"`
	Stage           string             `json:"stage"`
	Reason          string             `json:"reason"`
	ErrorDetail     string             `json:"error_detail"`
	FailingNodes    []string           `json:"failing_nodes"`
	PerNode         []seedBuildNodeDTO `json:"per_node"`
	Repo            string             `json:"repo"`
	CommitSHA       string             `json:"commit_sha"`
	CodeBundleURI   string             `json:"code_bundle_uri"`
	CandidateSchema string             `json:"candidate_schema"`
}

// validationRejectedDTO is the release.rejected:v1 body of a validation
// failure. missing_nodes is always an empty array: dropped-projection nodes
// are logged rather than carried here, and the key is kept so the body shape
// is stable for consumers. The validation leg carries no error_detail — its
// evidence is per node.
type validationRejectedDTO struct {
	ReleaseID       string              `json:"release_id"`
	Stage           string              `json:"stage"`
	Reason          string              `json:"reason"`
	FailingNodes    []string            `json:"failing_nodes"`
	MissingNodes    []string            `json:"missing_nodes"`
	AggregateStatus string              `json:"aggregate_status"`
	PerNode         []validationNodeDTO `json:"per_node"`
	Repo            string              `json:"repo"`
	CommitSHA       string              `json:"commit_sha"`
	CodeBundleURI   string              `json:"code_bundle_uri"`
}

// duplicateRejectedDTO is the release.rejected:v1 body of a duplicate-table
// rejection. The check runs between legs rather than inside one, so the body
// carries no stage.
type duplicateRejectedDTO struct {
	ReleaseID     string             `json:"release_id"`
	Reason        string             `json:"reason"`
	ErrorDetail   string             `json:"error_detail"`
	FailingNodes  []string           `json:"failing_nodes"`
	PerNode       []duplicateNodeDTO `json:"per_node"`
	Repo          string             `json:"repo"`
	CommitSHA     string             `json:"commit_sha"`
	CodeBundleURI string             `json:"code_bundle_uri"`
}

// unbuildableUpstreamRejectedDTO is the release.rejected:v1 body of a
// cross-service upstream that cannot be built. It names only the release, the
// reason and the offending edges: there is no per-node evidence a fixer could
// act on, so the body carries none.
type unbuildableUpstreamRejectedDTO struct {
	ReleaseID   string `json:"release_id"`
	Reason      string `json:"reason"`
	ErrorDetail string `json:"error_detail"`
}

// Encode renders rej as the release.rejected:v1 body of its shape.
func (ReleaseRejectedJSON) Encode(rej ports.ReleaseRejection) ([]byte, error) {
	switch rej.Shape {
	case ports.RejectionShapeParse:
		return json.Marshal(parseRejectedDTO{
			ReleaseID:     rej.ReleaseID,
			Stage:         string(ports.RejectionShapeParse),
			Reason:        string(rej.Reason),
			ErrorDetail:   rej.ErrorDetail,
			FailingNodes:  rej.FailingNodes,
			PerNode:       mapNodes(rej.PerNode, parseNode),
			Repo:          rej.Repo,
			CommitSHA:     rej.CommitSHA,
			CodeBundleURI: rej.CodeBundleURI,
		})
	case ports.RejectionShapeCompile:
		return json.Marshal(compileRejectedDTO{
			ReleaseID:     rej.ReleaseID,
			Stage:         string(ports.RejectionShapeCompile),
			Reason:        string(rej.Reason),
			ErrorDetail:   rej.ErrorDetail,
			FailingNodes:  rej.FailingNodes,
			PerNode:       mapNodes(rej.PerNode, compileNode),
			Repo:          rej.Repo,
			CommitSHA:     rej.CommitSHA,
			CodeBundleURI: rej.CodeBundleURI,
		})
	case ports.RejectionShapeSeedBuild:
		return json.Marshal(seedBuildRejectedDTO{
			ReleaseID:       rej.ReleaseID,
			Stage:           string(ports.RejectionShapeSeedBuild),
			Reason:          string(rej.Reason),
			ErrorDetail:     rej.ErrorDetail,
			FailingNodes:    rej.FailingNodes,
			PerNode:         mapNodes(rej.PerNode, seedBuildNode),
			Repo:            rej.Repo,
			CommitSHA:       rej.CommitSHA,
			CodeBundleURI:   rej.CodeBundleURI,
			CandidateSchema: rej.CandidateSchema,
		})
	case ports.RejectionShapeValidation:
		return json.Marshal(validationRejectedDTO{
			ReleaseID:       rej.ReleaseID,
			Stage:           string(ports.RejectionShapeValidation),
			Reason:          string(rej.Reason),
			FailingNodes:    rej.FailingNodes,
			MissingNodes:    []string{},
			AggregateStatus: rej.AggregateStatus,
			PerNode:         mapNodes(rej.PerNode, validationNode),
			Repo:            rej.Repo,
			CommitSHA:       rej.CommitSHA,
			CodeBundleURI:   rej.CodeBundleURI,
		})
	case ports.RejectionShapeDuplicateTable:
		return json.Marshal(duplicateRejectedDTO{
			ReleaseID:     rej.ReleaseID,
			Reason:        string(rej.Reason),
			ErrorDetail:   rej.ErrorDetail,
			FailingNodes:  rej.FailingNodes,
			PerNode:       mapNodes(rej.PerNode, duplicateNode),
			Repo:          rej.Repo,
			CommitSHA:     rej.CommitSHA,
			CodeBundleURI: rej.CodeBundleURI,
		})
	case ports.RejectionShapeUnbuildableUpstream:
		return json.Marshal(unbuildableUpstreamRejectedDTO{
			ReleaseID:   rej.ReleaseID,
			Reason:      string(rej.Reason),
			ErrorDetail: rej.ErrorDetail,
		})
	default:
		return nil, fmt.Errorf("unknown release rejection shape %q", rej.Shape)
	}
}

// mapNodes projects the per-node union onto one shape's DTO, preserving the
// nil vs non-nil-empty distinction: a nil slice serialises as null and a
// non-nil empty one as [], exactly as the caller built it.
func mapNodes[T any](in []ports.RejectedNode, project func(ports.RejectedNode) T) []T {
	if in == nil {
		return nil
	}
	out := make([]T, len(in))
	for i, n := range in {
		out[i] = project(n)
	}
	return out
}

func parseNode(n ports.RejectedNode) parseNodeDTO {
	return parseNodeDTO{
		NodeID:   n.NodeID,
		Status:   n.Status,
		Kind:     n.Kind,
		Detail:   n.Detail,
		FilePath: n.FilePath,
		Service:  n.Service,
		NodeType: n.NodeType,
	}
}

func compileNode(n ports.RejectedNode) compileNodeDTO {
	return compileNodeDTO{
		NodeID:        n.NodeID,
		Status:        n.Status,
		DBTLogURI:     n.DBTLogURI,
		RunResultsURI: n.RunResultsURI,
	}
}

func seedBuildNode(n ports.RejectedNode) seedBuildNodeDTO {
	return seedBuildNodeDTO{
		NodeID:        n.NodeID,
		Status:        n.Status,
		DBTLogURI:     n.DBTLogURI,
		RunResultsURI: n.RunResultsURI,
		FilePath:      n.FilePath,
		Service:       n.Service,
	}
}

func validationNode(n ports.RejectedNode) validationNodeDTO {
	return validationNodeDTO{
		NodeID:               n.NodeID,
		Status:               n.Status,
		DBTLogURI:            n.DBTLogURI,
		RunResultsURI:        n.RunResultsURI,
		CandidateArtifactURI: n.CandidateArtifactURI,
		NodeType:             n.NodeType,
		FilePath:             n.FilePath,
		Service:              n.Service,
		ChangedAncestors:     mapAncestors(n.ChangedAncestors),
	}
}

func duplicateNode(n ports.RejectedNode) duplicateNodeDTO {
	return duplicateNodeDTO{
		NodeID:        n.NodeID,
		Status:        n.Status,
		Service:       n.Service,
		FilePath:      n.FilePath,
		NodeType:      n.NodeType,
		RelationID:    n.RelationID,
		OtherService:  n.OtherService,
		OtherFilePath: n.OtherFilePath,
	}
}

// mapAncestors preserves the nil vs non-nil-empty distinction so a node with
// no changed ancestor omits the key rather than emitting an empty array.
func mapAncestors(in []ports.ChangedAncestor) []changedAncestorDTO {
	if in == nil {
		return nil
	}
	out := make([]changedAncestorDTO, len(in))
	for i, a := range in {
		out[i] = changedAncestorDTO{NodeID: a.NodeID, FilePath: a.FilePath, Service: a.Service, Depth: a.Depth}
	}
	return out
}
