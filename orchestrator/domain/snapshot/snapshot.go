// Package snapshot defines the unified Snapshot routine that produces a :Run
// node in Neo4j with one :EXECUTES edge per task in the projection.
//
// Four concrete shapes are supported via Selector strategies:
//   - LatestFullDAG    — cron / trigger
//   - SingleNode       — single-node run
//   - SourcePinnedDAG  — rerun from failed/cancelled
//   - RebasePartition  — rebase from failed/cancelled
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

// Params is the input to a snapshot.
type Params struct {
	RunID        string
	ScheduleName string     // required by LatestFullDAG and RebasePartition; ignored by SourcePinnedDAG and SingleNode
	Kind         string     // "cron" | "trigger" | "rerun" | "single_node_run" | "rebase"
	SourceRunID  *uuid.UUID // nil for cron/trigger and latest-mode single-node-run
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
