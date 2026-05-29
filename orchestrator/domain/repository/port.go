package repository

import (
	"context"
	"encoding/json"
	"time"

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

// RejectedTopologyRepository writes forensics rows for permanently-rejected
// manifest.loaded:v1 messages. Used from a non-transactional context — the
// consumer ACKs after this call regardless of outcome, so a failed Insert
// must NOT turn a permanent error into a transient one.
type RejectedTopologyRepository interface {
	// Insert writes a forensics row. payload must be valid JSON.
	Insert(ctx context.Context, messageID, reason string, payload json.RawMessage) error
}

// TopologyStateRepository tracks the monotonic topology_generation counter.
type TopologyStateRepository interface {
	IncrementGeneration(ctx context.Context) (int64, error)
	GetGeneration(ctx context.Context) (int64, error)
}

// TopologyRepository is the write/read interface for the topology graph.
// Implementations are responsible for snapshot atomicity and cross-generation
// consistency.
type TopologyRepository interface {
	ApplySnapshot(ctx context.Context, nodes []*topology.TopologyNode, topologyGeneration int64) error
	SetServiceMetadata(ctx context.Context, serviceMetadata map[string]map[string]string, topologyGeneration int64) error
	GetScheduleGraph(ctx context.Context, scheduleName string) ([]*topology.Node, []*topology.UpstreamDependency, error)
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
