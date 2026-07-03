package ports

import (
	"context"
	"time"
)

// PRStatus is the observed state of a GitHub pull request.
type PRStatus struct {
	// Closed reports whether the PR is closed on GitHub (merged or not).
	Closed bool
	// Merged reports whether the closed PR was merged.
	Merged bool
	// ClosedAt is when GitHub closed the PR (the merge time for merged PRs).
	// Zero while the PR is open or when GitHub omitted the timestamp.
	ClosedAt time.Time
}

// PullRequestStatusChecker reads the current state of a pull request so the
// reconciler can mirror terminal outcomes onto the proposal row.
type PullRequestStatusChecker interface {
	PRStatus(ctx context.Context, repo string, number int) (PRStatus, error)
}
