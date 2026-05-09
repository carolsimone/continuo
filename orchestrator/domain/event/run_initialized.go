package event

// RunInitializedNode is the wire-format representation of a TableNode
// embedded in the run.initialized:v1 outbox payload. The orchestrator
// produces []RunInitializedNode slices (all_nodes, root_nodes, seed_nodes)
// and json.Marshals them as part of the outbound payload.
type RunInitializedNode struct {
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
