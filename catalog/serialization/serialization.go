// Package serialization holds the wire-facing DTOs for catalog's
// json-tagged domain types, keeping the domain packages (domain/event) free of
// struct tags. The redis parsers decode consumed messages through these DTOs
// and map each to its domain event, fixing the byte shapes here.
package serialization

import (
	"github.com/carolsimone/continuo/catalog/domain/event"
)

// RemediationRequestedNodeDTO is the JSON shape of one classified failure.
type RemediationRequestedNodeDTO struct {
	NodeID         string `json:"node_id"`
	Category       string `json:"category"`
	ErrorSignature string `json:"error_signature"`
	Reason         string `json:"reason"`
	ErrorExcerpt   string `json:"error_excerpt"`
	DBTLogURI      string `json:"dbt_log_uri"`
}

// RemediationRequestedDTO is the JSON shape of the remediation.requested:v2 payload.
type RemediationRequestedDTO struct {
	EventID       string                        `json:"event_id"`
	Source        string                        `json:"source"`
	ReleaseID     string                        `json:"release_id"`
	CodeBundleURI string                        `json:"code_bundle_uri"`
	ClassifiedAt  string                        `json:"classified_at"`
	Nodes         []RemediationRequestedNodeDTO `json:"nodes"`
}

// ToDomain maps a decoded DTO back to the domain event.
func (d RemediationRequestedDTO) ToDomain() event.RemediationRequested {
	var nodes []event.RemediationRequestedNode
	if d.Nodes != nil {
		nodes = make([]event.RemediationRequestedNode, len(d.Nodes))
		for i, n := range d.Nodes {
			nodes[i] = event.RemediationRequestedNode(n)
		}
	}
	return event.RemediationRequested{
		EventID:       d.EventID,
		Source:        d.Source,
		ReleaseID:     d.ReleaseID,
		CodeBundleURI: d.CodeBundleURI,
		ClassifiedAt:  d.ClassifiedAt,
		Nodes:         nodes,
	}
}

// PROpenedDTO is the JSON shape of the remediation.pr_opened:v1 payload.
type PROpenedDTO struct {
	ProposalID      string   `json:"proposal_id"`
	ReleaseID       string   `json:"release_id"`
	NodeID          string   `json:"node_id"`
	ResolvedNodeIDs []string `json:"resolved_node_ids"`
	PrURL           string   `json:"pr_url"`
	PrNumber        int      `json:"pr_number"`
	OpenedBy        string   `json:"opened_by"`
	OpenedAt        string   `json:"opened_at"`
	Service         string   `json:"service"`
}

// ToDomain maps a decoded DTO back to the domain event.
func (d PROpenedDTO) ToDomain() event.PROpened {
	return event.PROpened{
		ProposalID:      d.ProposalID,
		ReleaseID:       d.ReleaseID,
		NodeID:          d.NodeID,
		ResolvedNodeIDs: d.ResolvedNodeIDs,
		PrURL:           d.PrURL,
		PrNumber:        d.PrNumber,
		OpenedBy:        d.OpenedBy,
		OpenedAt:        d.OpenedAt,
		Service:         d.Service,
	}
}

// PRClosedEditDTO is the JSON shape of one file edit on a closed PR.
type PRClosedEditDTO struct {
	Path         string `json:"path"`
	TargetNodeID string `json:"target_node_id"`
	Amended      bool   `json:"amended"`
	Diff         string `json:"diff,omitempty"`
}

// PRClosedDTO is the JSON shape of the remediation.pr_closed:v1 payload.
type PRClosedDTO struct {
	ProposalID      string            `json:"proposal_id"`
	ReleaseID       string            `json:"release_id"`
	NodeID          string            `json:"node_id"`
	ResolvedNodeIDs []string          `json:"resolved_node_ids"`
	Service         string            `json:"service,omitempty"`
	PrURL           string            `json:"pr_url"`
	PrNumber        int               `json:"pr_number"`
	Outcome         string            `json:"outcome"`
	ClosedAt        string            `json:"closed_at"`
	Edits           []PRClosedEditDTO `json:"edits,omitempty"`
}

// ToDomain maps a decoded DTO back to the domain event.
func (d PRClosedDTO) ToDomain() event.PRClosed {
	var edits []event.PRClosedEdit
	if d.Edits != nil {
		edits = make([]event.PRClosedEdit, len(d.Edits))
		for i, ed := range d.Edits {
			edits[i] = event.PRClosedEdit(ed)
		}
	}
	return event.PRClosed{
		ProposalID:      d.ProposalID,
		ReleaseID:       d.ReleaseID,
		NodeID:          d.NodeID,
		ResolvedNodeIDs: d.ResolvedNodeIDs,
		Service:         d.Service,
		PrURL:           d.PrURL,
		PrNumber:        d.PrNumber,
		Outcome:         d.Outcome,
		ClosedAt:        d.ClosedAt,
		Edits:           edits,
	}
}
