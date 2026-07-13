package events

// DefaultTaskMaxRetries is the canonical retry budget that the orchestrator
// stamps onto every DispatchedTask. It MUST match k8s-controller's
// DefaultTaskMaxRetries (k8s-controller/config/config.go) and
// executor-controller's deploy_handler default — otherwise state's
// HasRetryableFailedTaskTx and the k8s retry loop drift apart and runs
// finalize as failed mid-retry.
const DefaultTaskMaxRetries int32 = 2

// DispatchedTask is one row in RunEntriesDispatched.AllTasks.
type DispatchedTask struct {
	TaskID              string `json:"task_id"`
	ServiceName         string `json:"service_name"`
	SchemaName          string `json:"schema_name"`
	TableName           string `json:"table_name"`
	NodeType            string `json:"node_type"`
	MaxRetries          int32  `json:"max_retries"`
	ManifestVersion     string `json:"manifest_version"`
	ImageTag            string `json:"image_tag"`
	Status              string `json:"status,omitempty"`                 // "pending" (default) | "succeeded" (inherited)
	InheritedFromTaskID string `json:"inherited_from_task_id,omitempty"` // empty for rebased; root task_id (uuid) for inherited
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

// DispatchFailedReason is the closed set of reasons emitted with
// run.entries.dispatch_failed:v1. Most values correspond 1:1 to a snapshot
// package sentinel, mapped by the orchestrator's dispatchFailedReason mapper
// (orchestrator/service/handlers/dispatch_failed.go). InvalidNodeType is the
// exception: it is raised directly when a dispatch node carries an unparseable
// node_type, a permanent data defect that no retry can fix, so the run must
// fail fast rather than sit until the watchdog cancels it.
type DispatchFailedReason string

const (
	DispatchFailedReasonTargetNotFound  DispatchFailedReason = "target_not_found"
	DispatchFailedReasonEmptyProjection DispatchFailedReason = "empty_projection"
	DispatchFailedReasonInvalidNodeType DispatchFailedReason = "invalid_node_type"
	// DispatchFailedReasonNoTests is emitted when a single-node TEST run
	// targets a node with zero dbt tests: there is no work for `dbt test`
	// to do, so the orchestrator fails the dispatch fast instead of
	// scheduling a pointless Job.
	DispatchFailedReasonNoTests DispatchFailedReason = "no_tests"
)

// RunEntriesDispatchFailed — stream: run.entries.dispatch_failed:v1
// Published by: orchestrator when it cannot produce dispatch work for a
// run. Symmetric to RunEntriesDispatched: that one creates tasks +
// flips scheduler_tracker to running; this one writes no tasks + flips
// scheduler_tracker to failed.
// Consumed by: state.
type RunEntriesDispatchFailed struct {
	ScheduleID   string               `json:"schedule_id"`
	ScheduleName string               `json:"schedule_name"`
	Reason       DispatchFailedReason `json:"reason"`
}

// RunFinalized — stream: run.finalized:v1
// Published by: state
// Consumed by: UI, analytics
type RunFinalized struct {
	ScheduleID   string `json:"schedule_id"`
	ScheduleName string `json:"schedule_name"`
	Status       string `json:"status"` // succeeded | failed | cancelled
}
