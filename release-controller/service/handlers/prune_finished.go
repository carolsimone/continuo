package handlers

import (
	"context"
	"fmt"
)

// PruneFinishedRuns deletes terminal runs of either kind older than retentionDays,
// preserving any run still referenced by current_prod or a service_prod row.
func PruneFinishedRuns(ctx context.Context, d *Deps, retentionDays int) (int, error) {
	cutoff := d.Clock.Now().AddDate(0, 0, -retentionDays)

	u := d.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer u.Rollback() //nolint:errcheck

	cp, err := u.CurrentProdRepo().Get(ctx)
	if err != nil {
		return 0, fmt.Errorf("get current prod: %w", err)
	}

	sps, err := u.ServiceProdRepo().List(ctx)
	if err != nil {
		return 0, fmt.Errorf("list service_prod: %w", err)
	}

	// Collect all release IDs that must not be deleted.
	seen := map[string]struct{}{}
	if cp != nil && cp.ReleaseID() != "" {
		seen[cp.ReleaseID()] = struct{}{}
	}
	for _, sp := range sps {
		if sp.ReleaseID() != "" {
			seen[sp.ReleaseID()] = struct{}{}
		}
	}
	keepIDs := make([]string, 0, len(seen))
	for id := range seen {
		keepIDs = append(keepIDs, id)
	}

	n, err := u.RunRepo().DeleteFinishedBefore(ctx, cutoff, keepIDs)
	if err != nil {
		return 0, fmt.Errorf("delete resolved before: %w", err)
	}
	if err := u.Commit(); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return n, nil
}
