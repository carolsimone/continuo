package event

// Event is a marker interface for all events
type Event interface {
	isEvent()
}

// Outbox event types. Each names the typed payload stored in an execution_outbox
// row; the publisher routes on it.
const (
	// EventTypeTaskStatusReport names a task.status.updated:v2 report: its
	// row's payload is the report, the envelope is rendered on publish.
	EventTypeTaskStatusReport = "task_status_report"
)
