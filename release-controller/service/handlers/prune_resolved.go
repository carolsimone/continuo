package handlers

import (
	"context"
	"fmt"
)

// PruneResolvedReleases deletes terminal releases older than retentionDays,
// preserving any release still referenced by current_prod or by a service_prod
// row. Returns the number deleted. Runs in its own transaction. The cutoff is
// taken from the injected Clock so the time source stays consistent with the
// other handlers and is deterministic under test.
func PruneResolvedReleases(ctx context.Context, d *Deps, retentionDays int) (int, error) {
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
