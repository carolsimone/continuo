// Package deploy holds the domain port for executing a K8s deploy and the
// JobSpec value object that describes one. The application service depends on
// this port; the k8s adapter implements it. No infrastructure types leak here.
package deploy

import "context"

// JobSpec is the domain description of a single deploy operation. It carries
// only what the domain knows about the work — never adapter concerns such as
// the Kubernetes namespace or label selector.
type JobSpec struct {
	JobName      string
	TaskID       string
	ScheduleID   string
	ScheduleName string
	ServiceName  string
	SchemaName   string
	TableName    string
	NodeType     string
	ImageTag     string
}

// Deployer is the driven port the dispatcher uses to deploy work and observe
// how many deploys are currently in flight.
type Deployer interface {
	// Deploy executes the job described by spec. Implementations must be
	// idempotent by job name so a redelivery is a no-op.
	Deploy(ctx context.Context, spec JobSpec) error
	// CountActive returns the number of deploys currently running.
	CountActive(ctx context.Context) (int, error)
}
