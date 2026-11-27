package ports

import "context"

// FixPR is one owning project's pull request of a fix. A fix opens one pull
// request per owning project, so a fix carries a set of these rather than a
// single pull request.
type FixPR struct {
	Project string
	PRState string // "" | opening | open | merged | rejected | failed
	PRURL   string
}

// FixSummary is the slice of a fix row the retry decision needs: one
// remediation attempt at a node of the release.
type FixSummary struct {
	ID      string
	NodeID  string
	Attempt int
	Status  string // generating | verifying | proposed | skipped | failed | escalated
	// PullRequests is one entry per owning project whose pull request has
	// been claimed, in project order. Empty for a fix that never entered the
	// pull-request lifecycle.
	PullRequests []FixPR
	// PRProjects is the owning-project groups the fix splits into; the empty
	// string names the group of a fix that is not split by project.
	PRProjects []string
	// RemediationRound is the release's remediation round this attempt belongs
	// to. 0 on an attempt that names no round, which the retry decision treats
	// the same as round 1.
	RemediationRound int
}

// FixReader lists the fix rows recorded for a release.
type FixReader interface {
	ListFixesForRelease(ctx context.Context, releaseID string) ([]FixSummary, error)
}
