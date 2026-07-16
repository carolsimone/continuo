package model

// ExecutionMode is how a Deployment reaches a dbt process. Jobs-mode work gets
// one Kubernetes Job per task; workers-mode work is claimed from a pool of
// long-lived worker pods that pull tasks under a lease.
type ExecutionMode string

const (
	ExecutionModeJobs    ExecutionMode = "jobs"
	ExecutionModeWorkers ExecutionMode = "workers"
)

// Valid reports whether m is one of the paths the executor_deployments
// execution_mode CHECK constraint accepts. Unlike ExecutionPath, the empty mode
// is invalid: every Deployment is created on a definite path.
func (m ExecutionMode) Valid() bool {
	switch m {
	case ExecutionModeJobs, ExecutionModeWorkers:
		return true
	}
	return false
}

// ExecutionPath is how a worker invokes dbt for a claimed task. Native runs dbt
// in-process against a hydrated manifest; the wrapper paths shell out to a
// team-supplied entrypoint, either with a command this executor pinned
// (wrapper_required) or one it cannot introspect (wrapper_opaque).
type ExecutionPath string

const (
	ExecutionPathNative          ExecutionPath = "native"
	ExecutionPathWrapperRequired ExecutionPath = "wrapper_required"
	ExecutionPathWrapperOpaque   ExecutionPath = "wrapper_opaque"
)

// Valid reports whether p is one of the paths the executor_deployments
// execution_path CHECK constraint accepts. The empty path is valid: it is the
// unset state of a task no worker has claimed, stored as NULL.
func (p ExecutionPath) Valid() bool {
	switch p {
	case "", ExecutionPathNative, ExecutionPathWrapperRequired, ExecutionPathWrapperOpaque:
		return true
	}
	return false
}
