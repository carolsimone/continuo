package repository

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain/codeversion"
	"github.com/carolsimone/continuo/orchestrator/domain/topology"
	"github.com/google/uuid"
)

// CancelledSchedulesRepository tracks schedule IDs that have been cancelled by
// an upstream control-plane signal. Used to short-circuit terminal-state
// processing for already-cancelled runs.
type CancelledSchedulesRepository interface {
	Insert(ctx context.Context, scheduleID uuid.UUID) error
	Exists(ctx context.Context, scheduleID uuid.UUID) (bool, error)
	DeleteExpired(ctx context.Context, ttl time.Duration) (int64, error)
}

// TopologyStateRepository tracks the monotonic topology_generation counter.
type TopologyStateRepository interface {
	IncrementGeneration(ctx context.Context) (int64, error)
	GetGeneration(ctx context.Context) (int64, error)
}

// TopologyRepository is the write interface for the topology graph.
type TopologyRepository interface {
	SetServiceMetadata(ctx context.Context, serviceMetadata map[string]map[string]string, topologyGeneration int64) error
}

// ReleasePromotionRepository performs the atomic Neo4j topology swap triggered
// by release.promoted:v1. Implementations MUST:
//  1. Run all writes inside a single Neo4j explicit transaction.
//  2. Read :Meta {key:'current_release'} first and short-circuit (return
//     false, nil) if release_id already matches.
//  3. Otherwise TRUNCATE :Table + :DEPENDS_ON, recreate from `nodes`, and
//     MERGE the :Meta singleton to the new release_id within the same tx.
type ReleasePromotionRepository interface {
	// PromoteRelease atomically swaps the current topology to the one carried
	// by nodes. Returns (changed=true) when the swap was performed; (false)
	// when current_release already matched and the call was a no-op.
	PromoteRelease(
		ctx context.Context,
		releaseID string,
		nodes []topology.ReleasePromotedTopologyNode,
		now time.Time,
	) (changed bool, err error)
}

// CodeVersionRepository writes the code-version history behind the :Table
// topology. Implementations decide what to write by comparing each incoming
// node's content_hash against the version the graph currently marks as current
// — never against a flag carried by the event — so any later release converges a
// graph that missed a write.
type CodeVersionRepository interface {
	// WriteVersions ingests one release's versions. It is idempotent: replaying
	// the same input the second time writes nothing.
	WriteVersions(ctx context.Context, in codeversion.WriteInput) (codeversion.WriteResult, error)
}
