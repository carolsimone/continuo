package run

import (
	"github.com/google/uuid"
)

// NodeKey is a value object that uniquely identifies a table within the run.
type NodeKey struct {
	ServiceName string
	SchemaName  string
	TableName   string
}

// RunStatus is the lifecycle status of the Run aggregate root.
type RunStatus string

const (
	RunStatusInitialized RunStatus = "INITIALIZED"
	RunStatusInProgress  RunStatus = "IN_PROGRESS"
	RunStatusSucceeded   RunStatus = "SUCCEEDED"
	RunStatusFailed      RunStatus = "FAILED"
	RunStatusCancelled   RunStatus = "CANCELLED"
	// RunStatusSkipped is the terminal projection of a run that had no work to
	// do (a Test run with no tests). Benign non-failure.
	RunStatusSkipped RunStatus = "SKIPPED"
)

// IsTerminal reports whether the status is a terminal run outcome. The aggregate
// vocabulary is the canonical uppercase enum above; the neo4j adapter is
// responsible for translating any stored or wire casing into one of these
// values on rehydration, so this is an exact comparison with no casing concerns.
func (s RunStatus) IsTerminal() bool {
	return s == RunStatusSucceeded || s == RunStatusFailed ||
		s == RunStatusCancelled || s == RunStatusSkipped
}

// RunNode is an entity within the Run aggregate, representing one table's
// execution slot in this run.
type RunNode struct {
	Key             NodeKey
	TaskID          uuid.UUID
	Status          string // domain.NodeStatus values: PENDING, RUNNING, SUCCEEDED, FAILED, SKIPPED
	ScheduleName    string
	NodeType        string
	ManifestVersion string
	ImageTag        string
	Upstreams       []NodeKey // used to check if all upstreams are terminal (unblocking)
	Downstreams     []NodeKey // immediate downstream keys (cascade skip traversal)
}

func (n *RunNode) isTerminal() bool {
	return n.Status == "SUCCEEDED" || n.Status == "FAILED" || n.Status == "SKIPPED"
}
