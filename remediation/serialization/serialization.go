// Package serialization holds the wire-facing DTO for remediation's
// remediation.requested:v2 trigger, keeping the domain event package free of
// json struct tags. It sits outside adapters/ so the classifier handler (which
// builds and marshals the trigger) may map through it without importing an
// adapter, and outside domain/ so the tags live away from the domain type.
package serialization

import (
	"github.com/carolsimone/continuo/remediation/domain/event"
)

// ChangedAncestorDTO is the JSON shape of one changed upstream of a failing node.
type ChangedAncestorDTO struct {
	NodeID   string `json:"node_id"`
	FilePath string `json:"file_path,omitempty"`
	Service  string `json:"service,omitempty"`
	Depth    int    `json:"depth"`
}

// FailingNodeDTO is the JSON shape of one classified failure in the trigger.
type FailingNodeDTO struct {
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

// RemediationRequestedDTO is the JSON shape of the remediation.requested:v2 payload.
type RemediationRequestedDTO struct {
	EventID          string           `json:"event_id"`
	Source           string           `json:"source"`
	ReleaseID        string           `json:"release_id"`
	RemediationRound int              `json:"remediation_round"`
	Repo             string           `json:"repo"`
	CommitSHA        string           `json:"commit_sha"`
	CodeBundleURI    string           `json:"code_bundle_uri,omitempty"`
	ClassifiedAt     string           `json:"classified_at"`
	Nodes            []FailingNodeDTO `json:"nodes"`
}

func changedAncestorsFromDomain(in []event.ChangedAncestor) []ChangedAncestorDTO {
	if in == nil {
		return nil
	}
	out := make([]ChangedAncestorDTO, len(in))
	for i, a := range in {
		out[i] = ChangedAncestorDTO(a)
	}
	return out
}

func nodesFromDomain(in []event.FailingNode) []FailingNodeDTO {
	if in == nil {
		return nil
	}
	out := make([]FailingNodeDTO, len(in))
	for i, n := range in {
		out[i] = FailingNodeDTO{
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
			ChangedAncestors:     changedAncestorsFromDomain(n.ChangedAncestors),
		}
	}
	return out
}

// RemediationRequestedFromDomain maps the domain trigger to its DTO.
func RemediationRequestedFromDomain(e event.RemediationRequested) RemediationRequestedDTO {
	return RemediationRequestedDTO{
		EventID:          e.EventID,
		Source:           e.Source,
		ReleaseID:        e.ReleaseID,
		RemediationRound: e.RemediationRound,
		Repo:             e.Repo,
		CommitSHA:        e.CommitSHA,
		CodeBundleURI:    e.CodeBundleURI,
		ClassifiedAt:     e.ClassifiedAt,
		Nodes:            nodesFromDomain(e.Nodes),
	}
}

// ToDomain maps a decoded DTO back to the domain trigger.
func (d RemediationRequestedDTO) ToDomain() event.RemediationRequested {
	var nodes []event.FailingNode
	if d.Nodes != nil {
		nodes = make([]event.FailingNode, len(d.Nodes))
		for i, n := range d.Nodes {
			var anc []event.ChangedAncestor
			if n.ChangedAncestors != nil {
				anc = make([]event.ChangedAncestor, len(n.ChangedAncestors))
				for j, a := range n.ChangedAncestors {
					anc[j] = event.ChangedAncestor(a)
				}
			}
			nodes[i] = event.FailingNode{
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
				ChangedAncestors:     anc,
			}
		}
	}
	return event.RemediationRequested{
		EventID:          d.EventID,
		Source:           d.Source,
		ReleaseID:        d.ReleaseID,
		RemediationRound: d.RemediationRound,
		Repo:             d.Repo,
		CommitSHA:        d.CommitSHA,
		CodeBundleURI:    d.CodeBundleURI,
		ClassifiedAt:     d.ClassifiedAt,
		Nodes:            nodes,
	}
}
