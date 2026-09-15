package events

// ModePromoteSeed is the legacy job mode for prod seed builds triggered on
// promotion. Nothing produces it any more: promoted seeds now run as an ordinary
// run, so their Jobs carry no mode label and route through the production
// lifecycle that owns retries, task executions, and status.
//
// It is retained only to drain work queued by a previous version during a
// rolling upgrade — in-flight query.model:v1 messages and deployments
// rows whose job_params still carry it. Those tasks have synthetic IDs with no
// matching row in state, so announcing their lifecycle would wedge state's
// consumer on a run it cannot load. Remove this once no such work can remain.
const ModePromoteSeed = "promote_seed"

// ModeCompile is the job mode for a release's compile leg (dbt compile +
// manifest upload, plus the parse-export/rehearsal initContainers). the
// dispatcher stamps it as a Job label; the job-status handler reads it to
// route the terminal status to the compile-specific handling path.
const ModeCompile = "compile"

// ModeValidation is the job mode for a release's per-node validation
// (`dbt --empty`) Jobs. the dispatcher stamps it as a Job label; the
// job-status handler reads it to route the terminal status to the validation
// path and to suppress production task-status announcements for these
// synthetic tasks.
const ModeValidation = "validation"

// ModeSeedBuild is the job mode for a release's seed-build Jobs (materializing
// new/changed seeds into the candidate schema). the dispatcher stamps it as a
// Job label; the job-status handler reads it to route the terminal status to
// the seed-build path and to suppress production task-status announcements
// for these synthetic tasks.
const ModeSeedBuild = "seed_build"

// CheckK8s — stream: check.k8s:v1
// Published and consumed by: execution-controller (the dispatcher writes the
// first ticket after a Job is created; the job-status handler re-schedules a
// still-running Job's next check the same way)
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
	// RunningAnnounced is true once the job-status handler has announced this
	// attempt as RUNNING on task.status.updated:v1. It rides the delay-queue ticket /
	// promoted stream message so RUNNING is emitted exactly once per attempt: the
	// dispatcher writes the first ticket for a fresh attempt with this false, and
	// the handler carries it forward unchanged on every re-schedule until RUNNING
	// is announced.
	RunningAnnounced bool `json:"running_announced"`
}
