// Package event defines the remediation.pr_opened:v1 payload and deterministic
// identifiers used for outbox dedup.
package event

import "github.com/google/uuid"

const PROpenedEventType = "remediation_pr_opened"

var prOpenedNamespace = uuid.MustParse("7e5a1b8f-9c34-4d7a-b2f5-8e1c6a9d3f2b")

// PROpenedEventID derives a stable id from (releaseID, attempt, project) so a
// re-emission of the same PR-opened fact dedups to one downstream event. Each
// owning-project PR of a split proposal gets its own id; the legacy project ""
// (a whole-proposal PR) reproduces the pre-split (releaseID, attempt) id
// byte-for-byte, so an unsplit proposal's dedup key never shifts.
func PROpenedEventID(releaseID string, attempt int, project string) uuid.UUID {
	name := releaseID + "|" + itoa(attempt)
	if project != "" {
		name += "|" + project
	}
	return uuid.NewSHA1(prOpenedNamespace, []byte(name))
}

// PROpened is the event payload emitted when a remediation PR is successfully opened.
type PROpened struct {
	ProposalID string
	ReleaseID  string
	NodeID     string
	// ResolvedNodeIDs is the failing nodes this PR fixes, sorted — the subset
	// of the attempt's fixed nodes this owning project's edits address.
	ResolvedNodeIDs []string
	// Project is the owning-project group this PR covers; omitted (its DTO field
	// is omitempty) for a legacy whole-proposal PR.
	Project  string
	PrURL    string
	PrNumber int
	OpenedBy string
	OpenedAt string
}
