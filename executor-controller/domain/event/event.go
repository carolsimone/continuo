package event

// Event is a marker interface for all events
type Event interface {
	isEvent()
}

// JobDeployed is the payload of an executor_outbox row whose event_type is
// "node_deployed". The dispatcher writes it after a deploy succeeds; the
// publisher reads it to build the node.deployed:v1 typed wire event
// (pkg/events.NodeDeployed). Stream: node.deployed:v1.
type JobDeployed struct {
	TaskID         string `json:"task_id"`
	ScheduleID     string `json:"schedule_id"`
	ScheduleName   string `json:"schedule_name"`
	ServiceName    string `json:"service_name"`
	SchemaName     string `json:"schema_name"`
	TableName      string `json:"table_name"`
	JobName        string `json:"job_name"`
	NodeType       string `json:"node_type"`
	ImageTag       string `json:"image_tag"`
	TaskRetryCount int    `json:"task_retry_count"` // task-level retry count (not outbox delivery retries)
	MaxRetries     int    `json:"max_retries"`      // maximum task retries allowed
}

func (JobDeployed) isEvent() {}

// NodeUpdated is the payload of an executor_outbox row whose event_type is
// "node_updated". The dispatcher writes it (status FAILED) when a deploy
// exhausts its retry budget, so orchestrator's HandleNodeCompleted advances
// the schedule. Stream: node.updated:v1.
type NodeUpdated struct {
	TaskID       string `json:"task_id"`
	ScheduleID   string `json:"schedule_id"`
	ScheduleName string `json:"schedule_name"`
	ServiceName  string `json:"service_name"`
	SchemaName   string `json:"schema_name"`
	TableName    string `json:"table_name"`
	Status       string `json:"status"`
}

func (NodeUpdated) isEvent() {}

// ToMap converts NodeUpdated to a flat map for Redis stream publishing.
func (e NodeUpdated) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"task_id":       e.TaskID,
		"schedule_id":   e.ScheduleID,
		"schedule_name": e.ScheduleName,
		"service_name":  e.ServiceName,
		"schema_name":   e.SchemaName,
		"table_name":    e.TableName,
		"status":        e.Status,
	}
}
