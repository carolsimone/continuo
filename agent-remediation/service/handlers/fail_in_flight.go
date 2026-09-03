package handlers

import (
	"context"
	"fmt"
)

// FailInFlight finalizes a release's in-flight 'generating' proposal row as
// 'failed', recording reason, and returns how many rows moved.
//
// It is the recovery path for a remediation trigger the stream consumer
// abandoned — poison-dropped after exhausting its redelivery budget — once
// markGenerating had already committed the in-flight row. The consumer's drop
// creates no state of its own, so only the owning service can close out the row
// it left behind; without this the row reports a fix as forever "generating"
// and the release's "Try again" action stays blocked behind that phantom
// in-flight attempt.
//
// It is idempotent: FailGenerating filters on status, so a row already in a
// terminal state (a redundant drop notification, or the verification
// reconciler that closed it first) is left exactly as it is and zero rows
// move.
func FailInFlight(ctx context.Context, deps Deps, releaseID, reason string) (int, error) {
	u := deps.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return 0, fmt.Errorf("begin (fail in-flight): %w", err)
	}
	defer func() { _ = u.Rollback() }()

	n, err := u.ProposalRepo().FailGenerating(ctx, releaseID, reason)
	if err != nil {
		return 0, fmt.Errorf("fail in-flight generating row: %w", err)
	}
	if n == 0 {
		return 0, nil
	}
	if err := u.Commit(); err != nil {
		return 0, fmt.Errorf("commit (fail in-flight): %w", err)
	}
	return n, nil
}
