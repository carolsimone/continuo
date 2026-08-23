// Package event: remediation.pr_closed:v1 payload and deterministic identifier
// used for outbox dedup.
package event

import "github.com/google/uuid"

const PRClosedEventType = "remediation_pr_closed"

var prClosedNamespace = uuid.MustParse("9c4f6a2d-1e8b-4c53-a7f9-2b6d8e0c4a17")

// PRClosedEventID derives a stable id from (releaseID, nodeID, attempt) so a
// re-emission of the same PR-outcome fact dedups to one downstream event.
func PRClosedEventID(releaseID, nodeID string, attempt int) uuid.UUID {
	return uuid.NewSHA1(prClosedNamespace, []byte(releaseID+"|"+nodeID+"|"+itoa(attempt)))
}

// PRClosed is the event payload emitted when a remediation PR reaches a
// terminal outcome on GitHub. Outcome is "merged" or "rejected".
type PRClosed struct {
	ProposalID string `json:"proposal_id"`
	ReleaseID  string `json:"release_id"`
	NodeID     string `json:"node_id"`
	PrURL      string `json:"pr_url"`
	PrNumber   int    `json:"pr_number"`
	Outcome    string `json:"outcome"`
	ClosedAt   string `json:"closed_at"`
}
