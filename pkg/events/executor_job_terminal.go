package events

// ExecutorJobTerminal — stream: executor.job.terminal:v1
// Published by: k8s-controller (once a Job it was asked to check settles)
// Consumed by: executor-controller (to release the Job's execution slot)
//
// It carries capacity accounting only. Every business outcome of a Job — task
// status, per-node validation results, retries — travels on its own stream; a
// consumer of this event writes no task or node lifecycle state. The executor
// row is named directly by ExecutorDeploymentID rather than rediscovered from
// the Job, so a Job that is TTL-reaped before its terminal is observed still
// releases the slot it held.
type ExecutorJobTerminal struct {
	ExecutorDeploymentID string `json:"executor_deployment_id"`
	JobName              string `json:"job_name"`
	TerminalStatus       string `json:"terminal_status"`
	CompletedAt          string `json:"completed_at"`
}
