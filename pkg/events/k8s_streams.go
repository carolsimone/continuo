package events

// NodeDeployed — stream: node.deployed:v1
// Published by: executor-controller (after a K8s Job create succeeds)
// Consumed by: k8s-controller (to start watching the Job's status)
//
// Carried as the JSON `payload` field of the Redis message; transport metadata
// (outbox_entry_id) travels as a flat sibling field for consumer-side dedup.
// The task-level retry count is named task_retry_count to distinguish it from
// outbox delivery retries.
type NodeDeployed struct {
	TaskID         string `json:"task_id"`
	ScheduleID     string `json:"schedule_id"`
	ScheduleName   string `json:"schedule_name"`
	ServiceName    string `json:"service_name"`
	SchemaName     string `json:"schema_name"`
	TableName      string `json:"table_name"`
	JobName        string `json:"job_name"`
	NodeType       string `json:"node_type"`
	ImageTag       string `json:"image_tag"`
	TaskRetryCount int32  `json:"task_retry_count"`
	MaxRetries     int32  `json:"max_retries"`
}

// CheckK8s — stream: check.k8s:v1
// Published and consumed by: k8s-controller (the delayed status-recheck self-loop)
//
// Carried as the JSON `payload` field of the Redis message. The delay timestamp
// (check_after) and dedup metadata (outbox_entry_id) travel as flat sibling
// fields: check_after gates re-delivery in the binding before the payload is
// decoded, so it is not part of this typed payload.
type CheckK8s struct {
	TaskID       string `json:"task_id"`
	ScheduleID   string `json:"schedule_id"`
	ScheduleName string `json:"schedule_name"`
	ServiceName  string `json:"service_name"`
	SchemaName   string `json:"schema_name"`
	TableName    string `json:"table_name"`
	JobName      string `json:"job_name"`
	NodeType     string `json:"node_type"`
	ImageTag     string `json:"image_tag"`
	RetryCount   int32  `json:"retry_count"`
	MaxRetries   int32  `json:"max_retries"`
}
