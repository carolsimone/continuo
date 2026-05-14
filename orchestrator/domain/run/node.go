package run

import "github.com/google/uuid"

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
)

func (s RunStatus) IsTerminal() bool {
	return s == RunStatusSucceeded || s == RunStatusFailed
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
