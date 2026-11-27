// Package event defines the fix.pr_opened:v1 payload and deterministic
// identifiers used for outbox dedup.
package event

import "github.com/google/uuid"

const PROpenedEventType = "fix_pr_opened"

var prOpenedNamespace = uuid.MustParse("7e5a1b8f-9c34-4d7a-b2f5-8e1c6a9d3f2b")

// PROpenedEventID derives a stable id from (releaseID, round, try, project) so
// a re-emission of the same PR-opened fact dedups to one downstream event. Each
// owning-project PR of a split fix gets its own id; the empty project (a PR
// that covers the whole fix) keys its id on (releaseID, round, try) alone.
func PROpenedEventID(releaseID string, round, try int, project string) uuid.UUID {
	name := releaseID + "|" + itoa(round) + "|" + itoa(try)
	if project != "" {
		name += "|" + project
	}
	return uuid.NewSHA1(prOpenedNamespace, []byte(name))
}

// PROpened is the event payload emitted when a remediation PR is successfully opened.
type PROpened struct {
	FixID     string
	ReleaseID string
	NodeID    string
	// ResolvedNodeIDs is the failing nodes this PR fixes, sorted — the subset
	// of the try's fixed nodes this owning project's edits address.
	ResolvedNodeIDs []string
	// Project is the owning-project group this PR covers; omitted (its DTO field
	// is omitempty) for a PR that covers the whole fix.
	Project  string
	PrURL    string
	PrNumber int
	OpenedBy string
	OpenedAt string
}
