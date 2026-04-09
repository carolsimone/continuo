package event

// Event is a marker interface for all events
type Event interface {
	isEvent()
}

// JobCheckRequest represents an event for delayed job status re-check
type JobCheckRequest struct {
	OutboxEntryID string `json:"outbox_entry_id"`
	TaskID        string `json:"task_id"`
	ScheduleID    string `json:"schedule_id"`
	ScheduleName  string `json:"schedule_name"`
	ServiceName   string `json:"service_name"`
	Schema        string `json:"schema"`
	TableName     string `json:"table_name"`
	JobName       string `json:"job_name"`
	CheckAfter    int64  `json:"check_after"` // Unix timestamp for delayed processing
	NodeType      string `json:"node_type"`
}

func (JobCheckRequest) isEvent() {}

// ToMap converts JobCheckRequest event to a map for Redis publishing
func (e JobCheckRequest) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"outbox_entry_id": e.OutboxEntryID,
		"task_id":         e.TaskID,
		"schedule_id":     e.ScheduleID,
		"schedule_name":   e.ScheduleName,
		"service_name":    e.ServiceName,
		"schema":          e.Schema,
		"table_name":      e.TableName,
		"job_name":        e.JobName,
		"check_after":     e.CheckAfter,
		"node_type":       e.NodeType,
	}
}

// TaskFailed represents an event that a task has permanently failed
type TaskFailed struct {
	TaskID       string `json:"task_id"`
	ScheduleID   string `json:"schedule_id"`
	ScheduleName string `json:"schedule_name"`
	ServiceName  string `json:"service_name"`
	Schema       string `json:"schema"`
	TableName    string `json:"table_name"`
	JobName      string `json:"job_name"`
	ErrorMessage string `json:"error_message"`
	RetryCount   int    `json:"retry_count"`
}

func (TaskFailed) isEvent() {}

// ToMap converts TaskFailed event to a map for Redis publishing
func (e TaskFailed) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"task_id":       e.TaskID,
		"schedule_id":   e.ScheduleID,
		"schedule_name": e.ScheduleName,
		"service_name":  e.ServiceName,
		"schema":        e.Schema,
		"table_name":    e.TableName,
		"job_name":      e.JobName,
		"error_message": e.ErrorMessage,
		"retry_count":   e.RetryCount,
	}
}

// TaskRetry represents an event that a task should be retried
type TaskRetry struct {
	TaskID       string `json:"task_id"`
	ScheduleID   string `json:"schedule_id"`
	ScheduleName string `json:"schedule_name"`
	ServiceName  string `json:"service_name"`
	Schema       string `json:"schema"`
	TableName    string `json:"table_name"`
	JobName      string `json:"job_name"`
	RetryCount   int    `json:"retry_count"`
	NodeType     string `json:"node_type"`
}

func (TaskRetry) isEvent() {}

// ToMap converts TaskRetry event to a map for Redis publishing
func (e TaskRetry) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"task_id":       e.TaskID,
		"schedule_id":   e.ScheduleID,
		"schedule_name": e.ScheduleName,
		"service_name":  e.ServiceName,
		"schema":        e.Schema,
		"table_name":    e.TableName,
		"job_name":      e.JobName,
		"retry_count":   e.RetryCount,
		"node_type":     e.NodeType,
	}
}

// NodeStatusUpdated represents an event that a node's status has changed
type NodeStatusUpdated struct {
	TaskID       string `json:"task_id"`
	ScheduleID   string `json:"schedule_id"`
	ScheduleName string `json:"schedule_name"`
	ServiceName  string `json:"service_name"`
	Schema       string `json:"schema"`
	TableName    string `json:"table_name"`
	Status       string `json:"status"`
}

func (NodeStatusUpdated) isEvent() {}

// ToMap converts NodeStatusUpdated event to a map for Redis publishing
func (e NodeStatusUpdated) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"task_id":       e.TaskID,
		"schedule_id":   e.ScheduleID,
		"schedule_name": e.ScheduleName,
		"service_name":  e.ServiceName,
		"schema":        e.Schema,
		"table_name":    e.TableName,
		"status":        e.Status,
	}
}
