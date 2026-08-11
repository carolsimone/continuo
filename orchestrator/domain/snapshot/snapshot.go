// Package snapshot defines the unified Snapshot routine that produces a :Run
// node in Neo4j with one :EXECUTES edge per task in the projection.
//
// Five concrete shapes are supported via Selector strategies:
//   - LatestFullDAG    — cron / trigger
//   - SingleNode       — single-node run
//   - SourcePinnedDAG  — rerun from failed/cancelled
//   - RebasePartition  — rebase from failed/cancelled
//   - NodeSet          — an explicit list of nodes (the seeds a release changed)
//
// The selection logic is pure Go, unit-testable against an in-memory
// TopologyReader; the write path is confined to one kind-agnostic Cypher
// transaction in the adapter (snapshot.SnapshotWriter).
package snapshot

import (
	"errors"

	"github.com/google/uuid"
)

// ErrEmptyProjection is returned when a selector produces zero TaskProjection
// entries. Handlers map this to run.entries.dispatch_failed:v1.
var ErrEmptyProjection = errors.New("snapshot: empty projection")

// ErrTargetNotFound is returned when a selector that resolves a specific target
// node cannot find it in the latest topology or in the source run's :EXECUTES
// set. Handlers map this to run.entries.dispatch_failed:v1.
var ErrTargetNotFound = errors.New("snapshot: target table not found")

// ErrNoTests is returned by the SingleNode and LatestFullDAG selectors when
// Operation is "test" and no resolved target (the single node, or any node in
// the whole-DAG projection) has a known positive dbt test count — either a
// known zero, or an unset count (topology written before test_count capture).
// In both cases there is no test we can confirm exists to run, so the run is
// gated rather than dispatched. Handlers map this to
// run.entries.dispatch_failed:v1 with reason "no_tests". A re-release
// backfills a concrete test_count, after which a genuinely-tested node
// becomes runnable again.
var ErrNoTests = errors.New("snapshot: node has no tests")

// ErrRerunOfTestUnsupported is returned by the SourcePinnedDAG (rerun) and
// RebasePartition (rebase) selectors when the source :Run's operation is
// "test". A rerun/rebase derives its dispatch from the source run's
// TaskProjection, which carries no per-task operation — so a rerun of a test
// run would silently issue `dbt run` against the failed nodes instead of
// `dbt test`, either mutating models or falsely reporting success. Rather
// than preserve/rerun-as-test, rerun/rebase of a test run is rejected
// outright: the caller triggers a fresh `node test` / `schedule test`
// instead. Handlers map this to run.entries.dispatch_failed:v1 with reason
// "rerun_of_test_unsupported".
var ErrRerunOfTestUnsupported = errors.New("snapshot: rerun/rebase of a test run is not supported")

// Params is the input to a snapshot.
type Params struct {
	RunID        string
	ScheduleName string     // required by LatestFullDAG and RebasePartition; ignored by SourcePinnedDAG and SingleNode
	Kind         string     // "cron" | "trigger" | "rerun" | "single_node_run" | "rebase" | "promote_seed"
	SourceRunID  *uuid.UUID // nil for cron/trigger and latest-mode single-node-run
	InitiatedBy  string     // user who initiated the run, or "system"; stamped on the :Run node
	Operation    string     // "" | "run" | "test" | "build"; consumed by SingleNode to gate zero-test TEST runs
	Selector     Selector
	Cancelled    bool // schedule was already cancelled at snapshot time → writer stamps the :Run terminal on create
}

// TaskProjection is one task's place in a run's projection. Each entry becomes
// one :EXECUTES edge from the new :Run to its :Table.
type TaskProjection struct {
	TaskID              uuid.UUID
	ServiceName         string
	SchemaName          string
	TableName           string
	ScheduleName        string // schedule_name on the :Table node we MATCH against
	NodeType            string
	InitialStatus       string // "PENDING" | "SUCCEEDED"
	ImageTag            string
	ManifestVersion     string
	// ContentHash is the code fingerprint stamped on this task's :EXECUTES edge:
	// the latest topology's hash for a fresh run, or the source run's pinned hash
	// for a rerun, rebase-inherited, or snapshot-of-run task.
	ContentHash         string
	TestCount           int
	TestCountKnown      bool       // true iff the pinned source (:Table or source :EXECUTES edge) had a test_count property
	InheritedFromTaskID *uuid.UUID // non-nil iff InitialStatus == "SUCCEEDED" via inherit; root pointer
	MaxRetries          int32
	// ReadyToDispatch marks a PENDING node as part of the run's initial dispatch
	// frontier: every one of its upstreams is satisfied (inherited-SUCCEEDED or
	// outside the run), so it can run immediately. Blocked PENDING nodes (an
	// upstream is itself rebased/PENDING) are left to the run aggregate, which
	// dispatches them via NodeUnblocked or cascade-skips them when an upstream
	// fails. Always false for inherited (non-PENDING) rows.
	ReadyToDispatch bool
}
