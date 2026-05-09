// Package snapshot provides the unified Snapshot routine that produces a :Run
// node in Neo4j with one :EXECUTES edge per task in the projection.
//
// Three concrete shapes are supported via TaskSetSelector strategies:
//   - LatestFullDAG    — cron / trigger
//   - SingleNode       — Feature 4 (single-node-run)
//   - RebasePartition  — Feature 2 (rebase from failed/cancelled)
//
// The plan/materialise split puts all selection logic in pure-Go code that's
// unit-testable against Neo4j fixtures, and confines the write path to one
// kind-agnostic Cypher tx.
package snapshot

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// ErrEmptyProjection is returned when a selector produces zero TaskProjection
// entries. The caller (orchestrator handler) typically converts this into a
// run.entries.dispatch_failed:v1 outbox event so the run is finalised as failed.
var ErrEmptyProjection = errors.New("snapshot: empty projection")

// ErrTargetNotFound is returned when a selector that resolves a specific target
// node (e.g. SingleNode) cannot find it in the latest topology or in the source
// run's :EXECUTES set. Mapped to run.entries.dispatch_failed:v1 by the handler.
var ErrTargetNotFound = errors.New("snapshot: target table not found")

// Params is the input to Snapshot.
type Params struct {
	RunID        string
	ScheduleName string     // required by LatestFullDAG and RebasePartition (identifies the schedule whose DAG is snapshotted); ignored by SourcePinnedDAG and SingleNode
	Kind         string     // "cron" | "trigger" | "rerun" | "single_node_run" | "rebase"
	SourceRunID  *uuid.UUID // nil for cron/trigger and latest-mode single-node-run
	Selector     TaskSetSelector
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
	InitialStatus       string     // Neo4j edge status: "PENDING" | "SUCCEEDED"
	ImageTag            string
	ManifestVersion     string
	InheritedFromTaskID *uuid.UUID // non-nil iff InitialStatus == "SUCCEEDED" via inherit; root pointer
	MaxRetries          int32
}

// TaskSetSelector picks the projection for a given run. Implementations MUST be
// read-only against Neo4j — the materialiser owns all writes. The selector is
// invoked inside the Snapshot transaction so any reads are consistent with the
// subsequent write.
type TaskSetSelector interface {
	SelectTasks(ctx context.Context, tx neo4j.ManagedTransaction, params Params) ([]TaskProjection, error)
}
