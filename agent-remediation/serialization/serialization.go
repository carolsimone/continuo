// Package serialization holds the wire-facing DTOs for agent-remediation's
// json-tagged domain events (remediation.proposed:v1, remediation.pr_opened:v1,
// remediation.pr_closed:v1), keeping the domain event package free of struct
// tags. It sits outside adapters/ so the proposal and propose-fix services
// (which build and marshal the triggers) may map through it without importing
// an adapter, and outside domain/ so the tags live away from the domain types.
package serialization

import (
	"github.com/carolsimone/continuo/agent-remediation/domain/event"
)

// PROpenedDTO is the JSON shape of the remediation.pr_opened:v1 payload.
type PROpenedDTO struct {
	ProposalID      string   `json:"proposal_id"`
	ReleaseID       string   `json:"release_id"`
	NodeID          string   `json:"node_id"`
	ResolvedNodeIDs []string `json:"resolved_node_ids"`
	Service         string   `json:"service,omitempty"`
	PrURL           string   `json:"pr_url"`
	PrNumber        int      `json:"pr_number"`
	OpenedBy        string   `json:"opened_by"`
	OpenedAt        string   `json:"opened_at"`
}

// PROpenedFromDomain maps the domain event to its DTO.
func PROpenedFromDomain(e event.PROpened) PROpenedDTO {
	return PROpenedDTO{
		ProposalID:      e.ProposalID,
		ReleaseID:       e.ReleaseID,
		NodeID:          e.NodeID,
		ResolvedNodeIDs: e.ResolvedNodeIDs,
		Service:         e.Service,
		PrURL:           e.PrURL,
		PrNumber:        e.PrNumber,
		OpenedBy:        e.OpenedBy,
		OpenedAt:        e.OpenedAt,
	}
}

// ToDomain maps a decoded DTO back to the domain event.
func (d PROpenedDTO) ToDomain() event.PROpened {
	return event.PROpened{
		ProposalID:      d.ProposalID,
		ReleaseID:       d.ReleaseID,
		NodeID:          d.NodeID,
		ResolvedNodeIDs: d.ResolvedNodeIDs,
		Service:         d.Service,
		PrURL:           d.PrURL,
		PrNumber:        d.PrNumber,
		OpenedBy:        d.OpenedBy,
		OpenedAt:        d.OpenedAt,
	}
}

// ClosedEditDTO is the JSON shape of one file edit on a closed PR.
type ClosedEditDTO struct {
	Path         string `json:"path"`
	TargetNodeID string `json:"target_node_id"`
	Amended      bool   `json:"amended"`
	Diff         string `json:"diff,omitempty"`
}

// PRClosedDTO is the JSON shape of the remediation.pr_closed:v1 payload.
type PRClosedDTO struct {
	ProposalID      string          `json:"proposal_id"`
	ReleaseID       string          `json:"release_id"`
	NodeID          string          `json:"node_id"`
	ResolvedNodeIDs []string        `json:"resolved_node_ids"`
	Service         string          `json:"service,omitempty"`
	PrURL           string          `json:"pr_url"`
	PrNumber        int             `json:"pr_number"`
	Outcome         string          `json:"outcome"`
	ClosedAt        string          `json:"closed_at"`
	Edits           []ClosedEditDTO `json:"edits,omitempty"`
}

// PRClosedFromDomain maps the domain event to its DTO.
func PRClosedFromDomain(e event.PRClosed) PRClosedDTO {
	var edits []ClosedEditDTO
	if e.Edits != nil {
		edits = make([]ClosedEditDTO, len(e.Edits))
		for i, ed := range e.Edits {
			edits[i] = ClosedEditDTO(ed)
		}
	}
	return PRClosedDTO{
		ProposalID:      e.ProposalID,
		ReleaseID:       e.ReleaseID,
		NodeID:          e.NodeID,
		ResolvedNodeIDs: e.ResolvedNodeIDs,
		Service:         e.Service,
		PrURL:           e.PrURL,
		PrNumber:        e.PrNumber,
		Outcome:         e.Outcome,
		ClosedAt:        e.ClosedAt,
		Edits:           edits,
	}
}

// ToDomain maps a decoded DTO back to the domain event.
func (d PRClosedDTO) ToDomain() event.PRClosed {
	var edits []event.ClosedEdit
	if d.Edits != nil {
		edits = make([]event.ClosedEdit, len(d.Edits))
		for i, ed := range d.Edits {
			edits[i] = event.ClosedEdit(ed)
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

// ProposedEditDTO is the JSON shape of one proposed file change.
type ProposedEditDTO struct {
	Path         string `json:"path"`
	ContentURI   string `json:"content_uri"`
	DiffURI      string `json:"diff_uri"`
	TargetNodeID string `json:"target_node_id"`
}

// RemediationProposedDTO is the JSON shape of the remediation.proposed:v1 payload.
type RemediationProposedDTO struct {
	EventID          string            `json:"event_id"`
	Source           string            `json:"source"`
	ReleaseID        string            `json:"release_id"`
	RemediationRound int               `json:"remediation_round"`
	NodeID           string            `json:"node_id"`
	ResolvedNodeIDs  []string          `json:"resolved_node_ids"`
	ErrorSignature   string            `json:"error_signature"`
	ProposedSQLURI   string            `json:"proposed_sql_uri"`
	DiffURI          string            `json:"diff_uri"`
	Edits            []ProposedEditDTO `json:"edits"`
	Rationale        string            `json:"rationale"`
	Confidence       string            `json:"confidence"`
	Model            string            `json:"model"`
	Attempt          int               `json:"attempt"`
	SourceResolved   bool              `json:"source_resolved"`
	ProposedAt       string            `json:"proposed_at"`
}

// RemediationProposedFromDomain maps the domain event to its DTO.
func RemediationProposedFromDomain(e event.RemediationProposed) RemediationProposedDTO {
	var edits []ProposedEditDTO
	if e.Edits != nil {
		edits = make([]ProposedEditDTO, len(e.Edits))
		for i, ed := range e.Edits {
			edits[i] = ProposedEditDTO(ed)
		}
	}
	return RemediationProposedDTO{
		EventID:          e.EventID,
		Source:           e.Source,
		ReleaseID:        e.ReleaseID,
		RemediationRound: e.RemediationRound,
		NodeID:           e.NodeID,
		ResolvedNodeIDs:  e.ResolvedNodeIDs,
		ErrorSignature:   e.ErrorSignature,
		ProposedSQLURI:   e.ProposedSQLURI,
		DiffURI:          e.DiffURI,
		Edits:            edits,
		Rationale:        e.Rationale,
		Confidence:       e.Confidence,
		Model:            e.Model,
		Attempt:          e.Attempt,
		SourceResolved:   e.SourceResolved,
		ProposedAt:       e.ProposedAt,
	}
}

// ToDomain maps a decoded DTO back to the domain event.
func (d RemediationProposedDTO) ToDomain() event.RemediationProposed {
	var edits []event.ProposedEdit
	if d.Edits != nil {
		edits = make([]event.ProposedEdit, len(d.Edits))
		for i, ed := range d.Edits {
			edits[i] = event.ProposedEdit(ed)
		}
	}
	return event.RemediationProposed{
		EventID:          d.EventID,
		Source:           d.Source,
		ReleaseID:        d.ReleaseID,
		RemediationRound: d.RemediationRound,
		NodeID:           d.NodeID,
		ResolvedNodeIDs:  d.ResolvedNodeIDs,
		ErrorSignature:   d.ErrorSignature,
		ProposedSQLURI:   d.ProposedSQLURI,
		DiffURI:          d.DiffURI,
		Edits:            edits,
		Rationale:        d.Rationale,
		Confidence:       d.Confidence,
		Model:            d.Model,
		Attempt:          d.Attempt,
		SourceResolved:   d.SourceResolved,
		ProposedAt:       d.ProposedAt,
	}
}
