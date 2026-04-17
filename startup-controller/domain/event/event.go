package event

// Event is a marker interface for all events
type Event interface {
	isEvent()
}

// NodeReadyForExecution represents an event indicating a node is ready for execution
type NodeReadyForExecution struct {
	ScheduleID   string `json:"schedule_id"`
	ScheduleName string `json:"schedule_name"`
	ServiceName  string `json:"service_name"`
	SchemaName   string `json:"schema_name"`
	TableName    string `json:"table_name"`
	TaskID       string `json:"task_id"`
	JobName      string `json:"job_name"`
	NodeType     string `json:"node_type"` // new
}

func (NodeReadyForExecution) isEvent() {}
