package run

import "github.com/google/uuid"

// DomainEvent is the sealed interface for events emitted by the Run aggregate.
type DomainEvent interface{ domainEvent() }

// NodeCascadeSkipped is emitted for each downstream node that is forced into
// SKIPPED status because an upstream node failed.
type NodeCascadeSkipped struct {
	Key    NodeKey
	TaskID uuid.UUID
}

func (NodeCascadeSkipped) domainEvent() {}

// NodeUnblocked is emitted when all upstreams of a node are terminal and the
// node transitions from blocked to ready for execution.
type NodeUnblocked struct {
	Key             NodeKey
	TaskID          uuid.UUID
	ScheduleName    string
	NodeType        string
	ManifestVersion string
	ImageTag        string
	// Operation is the run's operation ("" | "test" | "build"), carried so the
	// downstream unblock dispatch runs the same dbt verb as the frontier.
	Operation string
}

func (NodeUnblocked) domainEvent() {}

// RunFinalized is emitted when all nodes in the run have reached a terminal
// status and the run itself transitions to Succeeded or Failed.
type RunFinalized struct {
	RunID          string
	ScheduleName   string
	TerminalStatus string // "SUCCEEDED" or "FAILED"
}

func (RunFinalized) domainEvent() {}
