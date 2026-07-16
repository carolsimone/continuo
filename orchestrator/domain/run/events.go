package run

import (
	pkgModel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

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
	// DBTUniqueID is the node's dbt identity ("model.finance.orders"), used to
	// select the model inside a hydrated manifest.
	DBTUniqueID string
	// RuntimeManifestRef is the artifact the run pinned for this node when it
	// was snapshotted, carried so the unblock dispatch targets the same artifact
	// the rest of the run used.
	pkgModel.RuntimeManifestRef
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
