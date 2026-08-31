package ports

import (
	"context"
	"errors"
	"time"
)

// ErrPermissionDenied signals that the credential used to read pull request
// status lacks the required GitHub permission (HTTP 401/403). It is a
// persistent, human-actionable failure — distinct from a transient error or a
// genuinely still-open PR — so the reconciler can surface it as degraded rather
// than retrying silently forever.
var ErrPermissionDenied = errors.New("pull request status: permission denied")

// PRStatus is the observed state of a GitHub pull request.
type PRStatus struct {
	// Closed reports whether the PR is closed on GitHub (merged or not).
	Closed bool
	// Merged reports whether the closed PR was merged.
	Merged bool
	// ClosedAt is when GitHub closed the PR (the merge time for merged PRs).
	// Zero while the PR is open or when GitHub omitted the timestamp.
	ClosedAt time.Time
	// MergeCommitSHA is the commit a merged PR produced on the base branch; the
	// amend compare reads each edited file at this ref to check it against the
	// proposal. Empty for an unmerged (rejected) PR.
	MergeCommitSHA string
}

// PullRequestStatusChecker reads the current state of a pull request so the
// reconciler can mirror terminal outcomes onto the proposal row.
type PullRequestStatusChecker interface {
	PRStatus(ctx context.Context, repo string, number int) (PRStatus, error)
}
