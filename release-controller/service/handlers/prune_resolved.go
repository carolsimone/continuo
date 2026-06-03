package handlers

import (
	"context"
	"fmt"
)

// PruneResolvedReleases deletes terminal releases older than retentionDays,
// never the current_prod release. Returns the number deleted. Runs in its own
// transaction. The cutoff is taken from the injected Clock so the time source
// stays consistent with the other handlers and is deterministic under test.
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
	keep := ""
	if cp != nil {
		keep = cp.ReleaseID()
	}

	n, err := u.ReleaseRepo().DeleteResolvedBefore(ctx, cutoff, keep)
	if err != nil {
		return 0, fmt.Errorf("delete resolved before: %w", err)
	}
	if err := u.Commit(); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return n, nil
}
