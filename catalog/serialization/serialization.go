// Package serialization holds the wire-facing DTOs for catalog's
// json-tagged domain types, keeping the domain packages (domain/event) free of
// struct tags. The redis parsers decode consumed messages through these DTOs
// and map each to its domain event, fixing the byte shapes here.
package serialization

import (
	"github.com/carolsimone/continuo/catalog/domain/event"
)

// PROpenedDTO is the JSON shape of the fix.pr_opened:v1 payload.
type PROpenedDTO struct {
	FixID           string   `json:"fix_id"`
	ReleaseID       string   `json:"release_id"`
	NodeID          string   `json:"node_id"`
	ResolvedNodeIDs []string `json:"resolved_node_ids"`
	PrURL           string   `json:"pr_url"`
	PrNumber        int      `json:"pr_number"`
	OpenedBy        string   `json:"opened_by"`
	OpenedAt        string   `json:"opened_at"`
	Project         string   `json:"project"`
}

// ToDomain maps a decoded DTO back to the domain event.
func (d PROpenedDTO) ToDomain() event.PROpened {
	return event.PROpened{
		FixID:           d.FixID,
		ReleaseID:       d.ReleaseID,
		NodeID:          d.NodeID,
		ResolvedNodeIDs: d.ResolvedNodeIDs,
		PrURL:           d.PrURL,
		PrNumber:        d.PrNumber,
		OpenedBy:        d.OpenedBy,
		OpenedAt:        d.OpenedAt,
		Project:         d.Project,
	}
}

// PRClosedEditDTO is the JSON shape of one file edit on a closed PR.
type PRClosedEditDTO struct {
	Path         string `json:"path"`
	TargetNodeID string `json:"target_node_id"`
	Amended      bool   `json:"amended"`
	Diff         string `json:"diff,omitempty"`
}

// PRClosedDTO is the JSON shape of the fix.pr_closed:v1 payload.
type PRClosedDTO struct {
	FixID           string            `json:"fix_id"`
	ReleaseID       string            `json:"release_id"`
	NodeID          string            `json:"node_id"`
	ResolvedNodeIDs []string          `json:"resolved_node_ids"`
	Project         string            `json:"project,omitempty"`
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
		FixID:           d.FixID,
		ReleaseID:       d.ReleaseID,
		NodeID:          d.NodeID,
		ResolvedNodeIDs: d.ResolvedNodeIDs,
		Project:         d.Project,
		PrURL:           d.PrURL,
		PrNumber:        d.PrNumber,
		Outcome:         d.Outcome,
		ClosedAt:        d.ClosedAt,
		Edits:           edits,
	}
}
