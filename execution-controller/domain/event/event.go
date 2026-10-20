package event

// Event is a marker interface for all events
type Event interface {
	isEvent()
}

// Outbox event types. Each names the typed payload stored in an execution_outbox
// row; the publisher routes on it.
const (
	EventTypeTaskStatusUpdated     = "task_status_updated"
	EventTypeTaskExecutionRecorded = "task_execution_recorded"
	// EventTypeCheckDelayed names the status-check tickets a previous version
	// wrote; the publisher marks them processed without publishing them.
	EventTypeCheckDelayed = "check_delayed"
)
