package command

import "github.com/google/uuid"

// RunInitialized carries the node lists from the orchestrator after it has
// performed the graph snapshot and resolved init nodes for a run.
type RunInitialized struct {
	RunID     uuid.UUID
	AllNodes  []NodePayload
	RootNodes []NodePayload
	SeedNodes []NodePayload
}

func (RunInitialized) isCommand() {}

// NodePayload is the wire-format of a single node inside the
// run.initialized:v1 and rerun.ready:v1 event payloads.
type NodePayload struct {
	TableName    string `json:"table_name"`
	SchemaName   string `json:"schema_name"`
	ServiceName  string `json:"service_name"`
	Owner        string `json:"owner"`
	ScheduleName string `json:"schedule_name"`
	Criticality  string `json:"criticality"`
	NodeType     string `json:"node_type"`
	Status       string `json:"status"`
}
