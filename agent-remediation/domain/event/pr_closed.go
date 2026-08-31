// Package event: remediation.pr_closed:v1 payload and deterministic identifier
// used for outbox dedup.
package event

import "github.com/google/uuid"

const PRClosedEventType = "remediation_pr_closed"

var prClosedNamespace = uuid.MustParse("9c4f6a2d-1e8b-4c53-a7f9-2b6d8e0c4a17")

// PRClosedEventID derives a stable id from (releaseID, attempt, service) so a
// re-emission of the same PR-outcome fact dedups to one downstream event. Each
// owning-service PR of a split proposal gets its own id; the legacy service ""
// (a whole-proposal PR) reproduces the pre-split (releaseID, attempt) id
// byte-for-byte.
func PRClosedEventID(releaseID string, attempt int, service string) uuid.UUID {
	name := releaseID + "|" + itoa(attempt)
	if service != "" {
		name += "|" + service
	}
	return uuid.NewSHA1(prClosedNamespace, []byte(name))
}

// ClosedEdit describes one of a closed PR's file edits at the moment it reached
// its terminal outcome. Amended reports whether a human changed this edit
// before the PR merged (the amend compare that sets it lands with the close
// loop); Diff carries the unified diff of that amendment when one exists.
type ClosedEdit struct {
	Path         string `json:"path"`
	TargetNodeID string `json:"target_node_id"`
	Amended      bool   `json:"amended"`
	Diff         string `json:"diff,omitempty"`
}

// PRClosed is the event payload emitted when a remediation PR reaches a
// terminal outcome on GitHub. Outcome is "merged" or "rejected".
type PRClosed struct {
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
	Outcome  string `json:"outcome"`
	ClosedAt string `json:"closed_at"`
	// Edits is this PR's per-file close detail, including which edits a human
	// amended before merge; omitted when the close loop carries none.
	Edits []ClosedEdit `json:"edits,omitempty"`
}
