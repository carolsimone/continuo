package command

import (
	"github.com/google/uuid"
)

// HandleNodeCompletedCmd carries the data for a node-completed event.
type HandleNodeCompletedCmd struct {
	TaskID       uuid.UUID
	ScheduleID   uuid.UUID
	ScheduleName string
	ServiceName  string
	SchemaName   string
	TableName    string
	Status       string
}

// InitializeRunCmd carries the data for a run initialization command.
type InitializeRunCmd struct {
	ScheduleName string       `json:"schedule_name"`
	RunID        string       `json:"run_id"`
	RerunTarget  *RerunTarget `json:"rerun_target,omitempty"`
}

// RerunTarget specifies the node to rerun from.
type RerunTarget struct {
	ServiceName string `json:"service_name"`
	SchemaName  string `json:"schema_name"`
	TableName   string `json:"table_name"`
}

// HandleRerunCmd carries the data for a rerun command.
type HandleRerunCmd struct {
	ScheduleName string
	RunID        string
	ServiceName  string
	SchemaName   string
	TableName    string
}

// NodePayload is the serialized form of a TableNode in the outbox payload.
type NodePayload struct {
	TableName    string `json:"table_name"`
	SchemaName   string `json:"schema_name"`
	ServiceName  string `json:"service_name"`
	Owner        string `json:"owner"`
	ScheduleName string `json:"schedule_name"`
	Criticality  string `json:"criticality"`
	NodeType     string `json:"node_type"`
	Status       string `json:"status"`
	TaskID       string `json:"task_id"`
}

// IngestTopologyCmd carries the data for a topology ingestion command.
type IngestTopologyCmd struct {
	Nodes []TopologyNodePayload
}

// TopologyNodePayload is the inbound representation of a single topology node.
type TopologyNodePayload struct {
	ServiceName     string              `json:"service_name"`
	SchemaName      string              `json:"schema_name"`
	TableName       string              `json:"table_name"`
	Owner           string              `json:"owner"`
	ScheduleName    string              `json:"schedule_name"`
	Criticality     string              `json:"criticality"`
	NodeType        string              `json:"node_type"`
	ManifestVersion string              `json:"manifest_version"`
	ImageTag        string              `json:"image_tag"`
	Dependencies    []DependencyPayload `json:"dependencies"`
}

// DependencyPayload is the inbound representation of an upstream dependency.
type DependencyPayload struct {
	ServiceName string `json:"service_name"`
	SchemaName  string `json:"schema_name"`
	TableName   string `json:"table_name"`
}

// SingleNodeRunRequest is the parsed payload of a trigger.single_node_run:v1
// message — everything orchestrator needs to snapshot a one-task run for the
// identified node. RunID equals the synthesised scheduler_tracker.schedule_id
// (string UUID); ScheduleName is the "single-node-run-<short-uuid>" identifier
// minted by state when it accepted the gRPC TriggerSingleNodeRun call.
type SingleNodeRunRequest struct {
	RunID          string // == ScheduleID.String()
	ScheduleName   string
	ServiceName    string
	SchemaName     string
	TableName      string
	MetadataSource string // "latest" | "snapshot_of_run"
	SourceRunID    string // empty in latest mode
}
