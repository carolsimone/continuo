package domain

import (
	"time"
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

// Outbox event_type routing keys for orchestrator_outbox rows. Each value is the
// event_type stored on the row and matched by the publisher's payloadToValues
// switch; defining them here (next to the payload structs) gives the emit site
// (service/handlers) and the publisher adapter (adapters/publisher) one source of
// truth. Values are the wire-stored event_type strings and must not change.
const (
	EventTypeNodeReadyForExecution    = "node_ready_for_execution"
	EventTypeCascadeTaskSkipped       = "cascade_task_skipped"
	EventTypeRunEntriesDispatched     = "run_entries_dispatched"
	EventTypeRunEntriesDispatchFailed = "run_entries_dispatch_failed"
	EventTypeReleasePromoted          = "release_promoted"
)

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
	// Operation is omitempty so normal (dbt run/seed/snapshot) messages are
	// wire-identical. Set to "test" for single-node TEST runs so the executor
	// runs `dbt test` instead of the default verb for the node's NodeType.
	Operation string `json:"operation,omitempty"`
}
