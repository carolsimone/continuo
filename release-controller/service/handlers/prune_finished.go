package handlers

import (
	"context"
	"fmt"
)

// PruneFinishedChecks deletes terminal checks of either kind older than retentionDays,
// preserving any check still named by the live release or by a production pointer.
func PruneFinishedChecks(ctx context.Context, d *Deps, retentionDays int) (int, error) {
	cutoff := d.Clock.Now().AddDate(0, 0, -retentionDays)

	u := d.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer u.Rollback() //nolint:errcheck

	cp, err := u.LiveReleaseRepo().Get(ctx)
	if err != nil {
		return 0, fmt.Errorf("get live release: %w", err)
	}

	pointers, err := u.ProductionPointerRepo().List(ctx)
	if err != nil {
		return 0, fmt.Errorf("list production pointers: %w", err)
	}

	// Collect all release IDs that must not be deleted.
	seen := map[string]struct{}{}
	if cp != nil && cp.ReleaseID() != "" {
		seen[cp.ReleaseID()] = struct{}{}
	}
	for _, pointer := range pointers {
		if pointer.ReleaseID() != "" {
			seen[pointer.ReleaseID()] = struct{}{}
		}
	}
	keepIDs := make([]string, 0, len(seen))
	for id := range seen {
		keepIDs = append(keepIDs, id)
	}

	n, err := u.ReleaseCheckRepo().DeleteFinishedBefore(ctx, cutoff, keepIDs)
	if err != nil {
		return 0, fmt.Errorf("delete resolved before: %w", err)
	}
	if err := u.Commit(); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return n, nil
}
