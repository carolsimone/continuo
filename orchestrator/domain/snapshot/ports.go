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

	// DescendantsInLatestTopologyBatch returns, for each start FQN, its transitive
	// DEPENDS_ON descendants in :Table space (not restricted to any source run).
	// Inactive :Table nodes are excluded. One Cypher round trip for the whole
	// batch (UNWIND over the starts). Every start appears as a key, mapping to a
	// possibly-empty slice. Used by RebasePartition.
	DescendantsInLatestTopologyBatch(ctx context.Context, starts []FQN) (map[FQN][]FQN, error)

	// DescendantsInSourceRunBatch returns, for each start FQN, its transitive
	// DEPENDS_ON descendants restricted to the source run's :EXECUTES set —
	// preserving source's pinned topology even if latest has drifted. One Cypher
	// round trip for the whole batch. Used by SourcePinnedDAG.
	DescendantsInSourceRunBatch(ctx context.Context, sourceRunID string, starts []FQN) (map[FQN][]FQN, error)

	// ImmediateDescendantsInLatestTopologyBatch returns, for each start FQN, its
	// one-hop DEPENDS_ON dependents (nodes that directly depend on start) among
	// active :Table nodes. Used to compute the rebase dispatch frontier: blocking
	// must follow only immediate edges, because the run aggregate
	// unblocks/cascades along immediate in-run edges — a node blocked via a
	// transitive-only path would never be reached. One Cypher round trip for the
	// whole batch.
	ImmediateDescendantsInLatestTopologyBatch(ctx context.Context, starts []FQN) (map[FQN][]FQN, error)

	// ImmediateDescendantsInSourceRunBatch returns, for each start FQN, its
	// one-hop DEPENDS_ON dependents restricted to the source run's :EXECUTES set.
	// The source-run counterpart of ImmediateDescendantsInLatestTopologyBatch,
	// used by SourcePinnedDAG to compute the dispatch frontier. One Cypher round
	// trip for the whole batch.
	ImmediateDescendantsInSourceRunBatch(ctx context.Context, sourceRunID string, starts []FQN) (map[FQN][]FQN, error)

	// LoadSingleLatestTable returns the latest :Table row for one FQN. The bool
	// is false (with no error) when the table doesn't exist or is inactive.
	LoadSingleLatestTable(ctx context.Context, fqn FQN) (LatestTableRow, bool, error)

	// LoadSingleTableFromSourceRun returns the (image_tag, manifest_version,
	// test_count) for the FQN as pinned in the source :Run's :EXECUTES edge,
	// plus the :Table's schedule_name and node_type. test_count is read from the
	// pinned edge, not the current :Table, so a later promotion that changes the
	// node's tests cannot retroactively change the no_tests gate for a stale
	// (snapshot_of_run) rerun; TestCountKnown is false when the source edge
	// predates test_count capture. The bool is false (with no error) when the
	// source run has no :EXECUTES edge to a :Table with that identity.
	LoadSingleTableFromSourceRun(ctx context.Context, sourceRunID string, fqn FQN) (LatestTableRow, bool, error)

	// SourceRunOperation returns the source :Run's operation property
	// ("" | "run" | "test" | "build"). Returns "" (no error) when the run
	// doesn't exist or has no operation stamped. Used by the SourcePinnedDAG
	// and RebasePartition selectors to reject a rerun/rebase whose source was
	// a test run (see ErrRerunOfTestUnsupported).
	SourceRunOperation(ctx context.Context, sourceRunID string) (string, error)
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
