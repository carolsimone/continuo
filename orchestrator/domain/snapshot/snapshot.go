// Package snapshot defines the unified Snapshot routine that produces a :Run
// node in Neo4j with one :EXECUTES edge per task in the projection.
//
// Three concrete shapes are supported via Selector strategies:
//   - LatestFullDAG    — cron / trigger
//   - SingleNode       — Feature 4 (single-node-run)
//   - SourcePinnedDAG  — rerun from failed/cancelled
//   - RebasePartition  — Feature 2 (rebase from failed/cancelled)
//
// The plan/materialise split puts all selection logic in pure-Go code that's
// unit-testable against an in-memory TopologyReader, and confines the write
// path to one kind-agnostic Cypher tx in the adapter.
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
}
