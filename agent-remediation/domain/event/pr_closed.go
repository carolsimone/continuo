// Package event: remediation.pr_closed:v1 payload and deterministic identifier
// used for outbox dedup.
package event

import "github.com/google/uuid"

const PRClosedEventType = "remediation_pr_closed"

var prClosedNamespace = uuid.MustParse("9c4f6a2d-1e8b-4c53-a7f9-2b6d8e0c4a17")

// PRClosedEventID derives a stable id from (releaseID, attempt, service) so a
// re-emission of the same PR-outcome fact dedups to one downstream event. Each
// owning-service PR of a split proposal gets its own id; the unsplit group's
// empty service (a whole-proposal PR) keys its id on (releaseID, attempt)
// alone.
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
// loop); Diff is always the edit's proposal-time unified diff, filled from the
// same source regardless of Amended — the precedent read renders the
// merged-truth diff instead when the edit was amended.
type ClosedEdit struct {
	Path         string
	TargetNodeID string
	Amended      bool
	Diff         string
}

// PRClosed is the event payload emitted when a remediation PR reaches a
// terminal outcome on GitHub. Outcome is "merged" or "rejected".
type PRClosed struct {
	ProposalID string
	ReleaseID  string
	NodeID     string
	// ResolvedNodeIDs is the failing nodes this PR fixes, sorted — the subset
	// of the attempt's fixed nodes this owning service's edits address.
	ResolvedNodeIDs []string
	// Service is the owning-service group this PR covers; omitted (its DTO field
	// is omitempty) for a legacy whole-proposal PR.
	Service  string
	PrURL    string
	PrNumber int
	Outcome  string
	ClosedAt string
	// Edits is this PR's per-file close detail, including which edits a human
	// amended before merge; omitted (its DTO field is omitempty) when the close
	// loop carries none.
	Edits []ClosedEdit
}
