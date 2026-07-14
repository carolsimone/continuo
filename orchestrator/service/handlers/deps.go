package handlers

import (
	"context"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
)

// SnapshotService is the handler-local interface for the snapshot application
// service. Defined here so handler tests can substitute a fake.
type SnapshotService interface {
	Snapshot(ctx context.Context, p snapshot.Params) ([]snapshot.TaskProjection, error)
	// SourceOperation returns the operation ("" | "test" | "build") stamped on
	// the source run's :Run node. A run's operation is immutable, so this
	// standalone read is race-free. Used by the derived-run handler to inherit
	// the source's operation onto the rerun/rebase it materialises.
	SourceOperation(ctx context.Context, sourceRunID string) (string, error)
}
