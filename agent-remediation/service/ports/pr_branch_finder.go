package ports

import (
	"context"
	"errors"
	"time"
)

// ErrInvalidRepo reports a repository reference that is not owner/name. It is
// a property of the row, not of GitHub: a lookup with it can never succeed on
// a retry, so a caller treats it as permanent.
var ErrInvalidRepo = errors.New("repository is not owner/name")

// PullRequestRef identifies a pull request found on GitHub.
type PullRequestRef struct {
	// Number is the pull request number.
	Number int
	// URL is the pull request's web URL.
	URL string
	// CreatedAt is when GitHub created the pull request. Zero when GitHub
	// omitted the timestamp.
	CreatedAt time.Time
}

// PullRequestBranchFinder looks up an existing pull request by its head
// branch, so the reconciler's opening sweep can recover a proposal whose PR
// was created on GitHub but never recorded onto the proposal row.
type PullRequestBranchFinder interface {
	// FindByBranch returns the pull request open (or, if closed, most recently
	// created) for repo's head branch. found is false when no pull request
	// exists for the branch — a genuine "nothing to recover" result, not an
	// error.
	FindByBranch(ctx context.Context, repo, branch string) (ref PullRequestRef, found bool, err error)
}
