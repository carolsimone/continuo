package serialization

import (
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
)

// RemediationRequestedV2DTO is the JSON shape of the remediation.requested:v2
// payload: one heal trigger per rejected release and remediation round,
// carrying the release's whole healable failing set.
type RemediationRequestedV2DTO struct {
	Source    string `json:"source"`
	ReleaseID string `json:"release_id"`
	// RemediationRound is missing or 0 for round 1.
	RemediationRound int    `json:"remediation_round"`
	Repo             string `json:"repo"`
	CommitSHA        string `json:"commit_sha"`
	// CodeBundleURI is empty when the release never parsed (compile-stage and
	// parse-stage failures).
	CodeBundleURI string             `json:"code_bundle_uri,omitempty"`
	Nodes         []RequestedNodeDTO `json:"nodes"`
}

// RequestedNodeDTO is the JSON shape of one failing node of the
// remediation.requested:v2 payload.
type RequestedNodeDTO struct {
	NodeID               string               `json:"node_id"`
	RelationID           string               `json:"relation_id,omitempty"`
	Category             string               `json:"category"`
	ErrorSignature       string               `json:"error_signature"`
	Reason               string               `json:"reason"`
	ErrorExcerpt         string               `json:"error_excerpt,omitempty"`
	DBTLogURI            string               `json:"dbt_log_uri"`
	CandidateArtifactURI string               `json:"candidate_artifact_uri,omitempty"`
	FilePath             string               `json:"file_path,omitempty"`
	Service              string               `json:"service,omitempty"`
	NodeType             string               `json:"node_type,omitempty"`
	OtherService         string               `json:"other_service,omitempty"`
	OtherFilePath        string               `json:"other_file_path,omitempty"`
	ChangedAncestors     []ChangedAncestorDTO `json:"changed_ancestors,omitempty"`
}

// ChangedAncestorDTO is the JSON shape of one entry of a failing node's
// changed_ancestors.
type ChangedAncestorDTO struct {
	NodeID   string `json:"node_id"`
	FilePath string `json:"file_path,omitempty"`
	Service  string `json:"service,omitempty"`
	Depth    int    `json:"depth,omitempty"`
}

// ToDomain maps a decoded payload to the trigger it carries. A missing or 0
// round is round 1. The tenant is not part of the payload, so TenantID is left
// empty for the caller to set from the message.
func (d RemediationRequestedV2DTO) ToDomain() remediation.Trigger {
	t := remediation.Trigger{
		Source:        d.Source,
		ReleaseID:     d.ReleaseID,
		Round:         max(d.RemediationRound, 1),
		Repo:          d.Repo,
		CommitSHA:     d.CommitSHA,
		CodeBundleURI: d.CodeBundleURI,
		Nodes:         make([]remediation.TriggerNode, 0, len(d.Nodes)),
	}
	for _, n := range d.Nodes {
		node := remediation.TriggerNode{
			NodeID:               n.NodeID,
			RelationID:           n.RelationID,
			ErrorSignature:       n.ErrorSignature,
			Category:             n.Category,
			Reason:               n.Reason,
			ErrorExcerpt:         n.ErrorExcerpt,
			DBTLogURI:            n.DBTLogURI,
			CandidateArtifactURI: n.CandidateArtifactURI,
			FilePath:             n.FilePath,
			Service:              n.Service,
			NodeType:             n.NodeType,
			OtherService:         n.OtherService,
			OtherFilePath:        n.OtherFilePath,
		}
		for _, a := range n.ChangedAncestors {
			node.ChangedAncestors = append(node.ChangedAncestors, remediation.ChangedAncestor(a))
		}
		t.Nodes = append(t.Nodes, node)
	}
	return t
}

// RemediationRequestedV2FromDomain is the payload of a trigger. The tenant is
// not part of the payload: it travels on the message.
func RemediationRequestedV2FromDomain(t remediation.Trigger) RemediationRequestedV2DTO {
	d := RemediationRequestedV2DTO{
		Source:           t.Source,
		ReleaseID:        t.ReleaseID,
		RemediationRound: t.Round,
		Repo:             t.Repo,
		CommitSHA:        t.CommitSHA,
		CodeBundleURI:    t.CodeBundleURI,
		Nodes:            make([]RequestedNodeDTO, 0, len(t.Nodes)),
	}
	for _, n := range t.Nodes {
		node := RequestedNodeDTO{
			NodeID:               n.NodeID,
			RelationID:           n.RelationID,
			Category:             n.Category,
			ErrorSignature:       n.ErrorSignature,
			Reason:               n.Reason,
			ErrorExcerpt:         n.ErrorExcerpt,
			DBTLogURI:            n.DBTLogURI,
			CandidateArtifactURI: n.CandidateArtifactURI,
			FilePath:             n.FilePath,
			Service:              n.Service,
			NodeType:             n.NodeType,
			OtherService:         n.OtherService,
			OtherFilePath:        n.OtherFilePath,
		}
		for _, a := range n.ChangedAncestors {
			node.ChangedAncestors = append(node.ChangedAncestors, ChangedAncestorDTO(a))
		}
		d.Nodes = append(d.Nodes, node)
	}
	return d
}

// EncodeRemediationRequestedV2 is the remediation.requested:v2 payload of a
// trigger, which DecodeRemediationRequestedV2 reads back as the same trigger
// without its tenant.
func EncodeRemediationRequestedV2(t remediation.Trigger) ([]byte, error) {
	raw, err := json.Marshal(RemediationRequestedV2FromDomain(t))
	if err != nil {
		return nil, fmt.Errorf("marshal remediation.requested payload: %w", err)
	}
	return raw, nil
}

// DecodeRemediationRequestedV2 decodes a remediation.requested:v2 payload into
// the trigger it carries, as ToDomain maps it. It errors on a payload that is
// not JSON.
func DecodeRemediationRequestedV2(payload []byte) (remediation.Trigger, error) {
	var d RemediationRequestedV2DTO
	if err := json.Unmarshal(payload, &d); err != nil {
		return remediation.Trigger{}, fmt.Errorf("unmarshal remediation.requested payload: %w", err)
	}
	return d.ToDomain(), nil
}
