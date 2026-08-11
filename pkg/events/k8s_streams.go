package events

// ModeCompile is the job mode for a release's compile leg (dbt compile +
// manifest upload, plus the parse-export/rehearsal initContainers). executor
// stamps it as a Job label; k8s-controller reads the label to route the
// terminal status to the compile-specific handling path.
const ModeCompile = "compile"

// ModeValidation is the job mode for a release's per-node validation
// (`dbt --empty`) Jobs. executor stamps it as a Job label; k8s-controller
// reads the label to route the terminal status to the validation path and to
// suppress production task-status announcements for these synthetic tasks.
const ModeValidation = "validation"

// ModeSeedBuild is the job mode for a release's seed-build Jobs (materializing
// new/changed seeds into the candidate schema). executor stamps it as a Job
// label; k8s-controller reads the label to route the terminal status to the
// seed-build path and to suppress production task-status announcements for
// these synthetic tasks.
const ModeSeedBuild = "seed_build"

// NodeDeployed — stream: node.deployed:v1
// Published by: executor-controller (after a K8s Job create succeeds)
// Consumed by: k8s-controller (to start watching the Job's status)
//
// Carried as the JSON `payload` field of the Redis message; transport metadata
// (outbox_entry_id) travels as a flat sibling field for consumer-side dedup.
// The task-level retry count is named task_retry_count to distinguish it from
// outbox delivery retries.
type NodeDeployed struct {
	TaskID       string `json:"task_id"`
	ScheduleID   string `json:"schedule_id"`
	ScheduleName string `json:"schedule_name"`
	ServiceName  string `json:"service_name"`
	SchemaName   string `json:"schema_name"`
	TableName    string `json:"table_name"`
	JobName      string `json:"job_name"`
	NodeType     string `json:"node_type"`
	ImageTag     string `json:"image_tag"`
	// Operation is the dbt verb this Job runs (e.g. "test"). Empty for a normal
	// production `dbt run`, whose wire format is unchanged. It rides the durable
	// check/retry chain so a retry after the Job is TTL-reaped still rebuilds the
	// same verb — it is never re-derived from Job metadata that may be gone.
	Operation      string `json:"operation,omitempty"`
	TaskRetryCount int32  `json:"task_retry_count"`
	MaxRetries     int32  `json:"max_retries"`
}

// CheckK8s — stream: check.k8s:v1
// Published and consumed by: k8s-controller (the delayed status-recheck)
//
// This is the typed payload the promoter XADDs to check.k8s:v1 once a
// delay-queue ticket becomes due. It travels in the `payload` field, alongside a
// flat `outbox_entry_id` sibling field (carried on the ticket) for consumer-side
// dedup. The delay itself (check_after) lives as the ZSET score on the delay
// queue, not in this stream payload.
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
	// Operation is the dbt verb this Job runs (e.g. "test"); empty for a normal
	// production `dbt run`. It travels in the durable payload (delay-queue
	// ticket → promoted stream message) so a check that lands after the Job is
	// gone still retains the verb for retry.
	Operation  string `json:"operation,omitempty"`
	RetryCount int32  `json:"retry_count"`
	MaxRetries int32  `json:"max_retries"`
	// RunningAnnounced is true once k8s-controller has announced this attempt as
	// RUNNING on task.status.updated:v1. It rides the delay-queue ticket /
	// promoted stream message so RUNNING is emitted exactly once per attempt; a
	// fresh node.deployed:v1 (a new attempt) carries no such field and so resets
	// it to false.
	RunningAnnounced bool `json:"running_announced"`
}
