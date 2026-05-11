package snapshot

import "context"

// TopologyReader is the read-side port for snapshot policies. Implementations
// live in the Neo4j adapter and are bound to a single Neo4j ManagedTransaction
// so reads from this port and the subsequent SnapshotWriter call commit in one
// Cypher tx.
type TopologyReader interface {
	// LoadLatestSourceDAG returns every active :Table in the given schedule plus
	// every active :Table any of those transitively DEPENDS_ON (upstream seeds,
	// possibly in other schedules). Tables that share a seed's schedule but are
	// not part of this DAG are excluded. Empty schedule name returns empty map.
	LoadLatestSourceDAG(ctx context.Context, scheduleName string) (map[FQN]LatestTableRow, error)

	// LoadSourceTasks loads the source :Run's :EXECUTES set keyed by FQN. Used
	// by SourcePinnedDAG and RebasePartition.
	LoadSourceTasks(ctx context.Context, sourceRunID string) (map[FQN]SourceTaskRow, error)

	// DescendantsInLatestTopology walks DEPENDS_ON in :Table space (not restricted
	// to any source run) and returns transitive descendants of the start FQN.
	// Inactive :Table nodes are excluded. Used by RebasePartition.
	DescendantsInLatestTopology(ctx context.Context, start FQN) ([]FQN, error)

	// DescendantsInSourceRun walks DEPENDS_ON (in :Table space) and restricts the
	// result to nodes that are also in the source run's :EXECUTES set — preserves
	// source's pinned topology even if latest has drifted. Used by SourcePinnedDAG.
	DescendantsInSourceRun(ctx context.Context, sourceRunID string, start FQN) ([]FQN, error)

	// LoadSingleLatestTable returns the latest :Table row for one FQN. The bool
	// is false (with no error) when the table doesn't exist or is inactive.
	LoadSingleLatestTable(ctx context.Context, fqn FQN) (LatestTableRow, bool, error)

	// LoadSingleTableFromSourceRun returns the (image_tag, manifest_version) for
	// the FQN as pinned in the source :Run's :EXECUTES edge, plus the :Table's
	// schedule_name and node_type. The bool is false (with no error) when the
	// source run has no :EXECUTES edge to a :Table with that identity.
	LoadSingleTableFromSourceRun(ctx context.Context, sourceRunID string, fqn FQN) (LatestTableRow, bool, error)
}

// SnapshotWriter is the write-side port for snapshot policies. Implementations
// MUST be transactionally consistent with the TopologyReader they're paired with
// — both bound to the same Neo4j ManagedTransaction by the TxRunner.
type SnapshotWriter interface {
	// WriteRunAndExecutesEdges writes the :Run node and one :EXECUTES edge per
	// projection entry. Idempotent on rerun (MERGE preserves existing rows).
	// Returns an error wrapped with prefix "snapshot_writer:" on Cypher failure
	// or invariant violation (wrote N edges, expected M).
	WriteRunAndExecutesEdges(ctx context.Context, p Params, projection []TaskProjection) error
}

// TxRunner opens a single Neo4j write transaction and hands the caller a paired
// TopologyReader + SnapshotWriter scoped to that tx. fn runs inside the tx; if
// it returns nil the tx commits, otherwise it rolls back. Implementations live
// in the adapter (only the adapter knows about Neo4j sessions).
type TxRunner interface {
	Run(ctx context.Context, fn func(TopologyReader, SnapshotWriter) error) error
}
