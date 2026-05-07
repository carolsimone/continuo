package events

// DispatchedTask is one row in RunEntriesDispatched.AllTasks.
type DispatchedTask struct {
	TaskID          string `json:"task_id"`
	ServiceName     string `json:"service_name"`
	SchemaName      string `json:"schema_name"`
	TableName       string `json:"table_name"`
	NodeType        string `json:"node_type"`
	MaxRetries      int32  `json:"max_retries"`
	ManifestVersion string `json:"manifest_version"`
	ImageTag        string `json:"image_tag"`
}

// RunEntriesDispatched — stream: run.entries.dispatched:v1
// Published by: orchestrator
// Consumed by: state
type RunEntriesDispatched struct {
	ScheduleID     string           `json:"schedule_id"`
	ScheduleName   string           `json:"schedule_name"`
	AllTasks       []DispatchedTask `json:"all_tasks"`
	TotalTaskCount int32            `json:"total_task_count"`
}

// RunRerunDispatched — stream: run.rerun.dispatched:v1
// Published by: orchestrator
// Consumed by: state
type RunRerunDispatched struct {
	ScheduleID   string   `json:"schedule_id"`
	ScheduleName string   `json:"schedule_name"`
	TasksToReset []string `json:"tasks_to_reset"`
	EntryTaskID  string   `json:"entry_task_id"`
}

// RunInitialized — stream: run.initialized:v1
// Published by: orchestrator (single-node run path)
// Consumed by: state (to register the synthesised run)
type RunInitialized struct {
	ScheduleID   string `json:"schedule_id"`
	ScheduleName string `json:"schedule_name"`
}

// RunFinalized — stream: run.finalized:v1
// Published by: state
// Consumed by: UI, analytics
type RunFinalized struct {
	ScheduleID   string `json:"schedule_id"`
	ScheduleName string `json:"schedule_name"`
	Status       string `json:"status"` // succeeded | failed
}
