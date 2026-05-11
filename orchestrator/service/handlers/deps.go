package handlers

import (
	"context"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
)

// SnapshotService is the handler-local interface for the snapshot application
// service. Defined here so handler tests can substitute a fake.
type SnapshotService interface {
	Snapshot(ctx context.Context, p snapshot.Params) ([]snapshot.TaskProjection, error)
}
