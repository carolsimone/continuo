package domain

import (
	"time"

	pkgModel "github.com/carolsimone/continuo/pkg/domain/model"
)

type Criticality string

const (
	CriticalityUnspecified Criticality = "UNSPECIFIED"
	CriticalityRegulatory  Criticality = "REGULATORY"
	CriticalityCore        Criticality = "CORE"
	CriticalitySecondary   Criticality = "SECONDARY"
)

type NodeStatus string

const (
	NodeStatusPending   NodeStatus = "PENDING"
	NodeStatusRunning   NodeStatus = "RUNNING"
	NodeStatusSucceeded NodeStatus = "SUCCEEDED"
	NodeStatusFailed    NodeStatus = "FAILED"
	NodeStatusSkipped   NodeStatus = "SKIPPED"
)

func (s NodeStatus) IsTerminal() bool {
	return s == NodeStatusSucceeded || s == NodeStatusFailed || s == NodeStatusSkipped
}

type GraphEdge struct {
	FromNodeID string // "{service_name}.{schema_name}.{table_name}"
	ToNodeID   string
}

type RunSummary struct {
	RunID          string
	ScheduleName   string
	TerminalStatus string
	CreatedAt      time.Time
	CompletedAt    time.Time
}

type TableNode struct {
	TableName       string
	SchemaName      string
	ServiceName     string
	Owner           string
	ScheduleName    string
	Criticality     Criticality
	LastUpdatedAt   time.Time
	CreatedAt       time.Time
	NodeType        string
	Status          string
	TaskID          string
	ManifestVersion string
	ImageTag        string
}

type ScheduleGraph struct {
	Nodes              []*TableNode
	Edges              []*GraphEdge
	TopologyGeneration int64 // :TopologyRoot.topology_generation at query time; 0 when unknown.
}

// CascadeTaskSkipped is the event payload written to task.status.updated:v1 outbox entries
// for downstream nodes that become unreachable due to an upstream failure.
type CascadeTaskSkipped struct {
	TaskID     string
	ScheduleID string
}

// NodeReadyForExecution is the event payload written to query.model:v1 outbox entries
type NodeReadyForExecution struct {
	ScheduleID      string `json:"schedule_id"`
	ScheduleName    string `json:"schedule_name"`
	ServiceName     string `json:"service_name"`
	SchemaName      string `json:"schema_name"`
	TableName       string `json:"table_name"`
	TaskID          string `json:"task_id"`
	JobName         string `json:"job_name"`
	NodeType        string `json:"node_type"`
	ManifestVersion string `json:"manifest_version"`
	ImageTag        string `json:"image_tag"`
	// DBTUniqueID is the node's dbt identity ("model.finance.orders"), which the
	// executor uses to select the model inside a hydrated manifest. Empty (and
	// omitted from the wire) for a node whose release pinned no runtime manifest.
	DBTUniqueID string `json:"dbt_unique_id,omitempty"`
	// RuntimeManifestRef names the prebuilt artifact a reusable worker hydrates
	// to execute this node. Its fields are omitempty, so a node with no pinned
	// manifest produces a wire shape identical to a pre-runtime-manifest
	// dispatch and executes down the per-node Job path.
	pkgModel.RuntimeManifestRef
	// Mode is omitempty so normal production messages are wire-identical (empty
	// string is not serialised). Set to events.ModePromoteSeed for promote-seed
	// jobs so k8s-controller can suppress the production lifecycle for them.
	Mode string `json:"mode,omitempty"`
	// Operation is omitempty so normal (dbt run/seed/snapshot) messages are
	// wire-identical. Set to "test" for single-node TEST runs so the executor
	// runs `dbt test` instead of the default verb for the node's NodeType.
	Operation string `json:"operation,omitempty"`
}
