// Package event defines the remediation.pr_opened:v1 payload and deterministic
// identifiers used for outbox dedup.
package event

import "github.com/google/uuid"

const PROpenedEventType = "remediation_pr_opened"

var prOpenedNamespace = uuid.MustParse("7e5a1b8f-9c34-4d7a-b2f5-8e1c6a9d3f2b")

// PROpenedEventID derives a stable id from (releaseID, attempt, service) so a
// re-emission of the same PR-opened fact dedups to one downstream event. Each
// owning-service PR of a split proposal gets its own id; the legacy service ""
// (a whole-proposal PR) reproduces the pre-split (releaseID, attempt) id
// byte-for-byte, so an unsplit proposal's dedup key never shifts.
func PROpenedEventID(releaseID string, attempt int, service string) uuid.UUID {
	name := releaseID + "|" + itoa(attempt)
	if service != "" {
		name += "|" + service
	}
	return uuid.NewSHA1(prOpenedNamespace, []byte(name))
}

// PROpened is the event payload emitted when a remediation PR is successfully opened.
type PROpened struct {
	ProposalID string `json:"proposal_id"`
	ReleaseID  string `json:"release_id"`
	NodeID     string `json:"node_id"`
	// ResolvedNodeIDs is the failing nodes this PR fixes, sorted — the subset
	// of the attempt's fixed nodes this owning service's edits address.
	ResolvedNodeIDs []string `json:"resolved_node_ids"`
	// Service is the owning-service group this PR covers; omitted for a legacy
	// whole-proposal PR.
	Service  string `json:"service,omitempty"`
	PrURL    string `json:"pr_url"`
	PrNumber int    `json:"pr_number"`
	OpenedBy string `json:"opened_by"`
	OpenedAt string `json:"opened_at"`
}
