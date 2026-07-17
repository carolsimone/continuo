package ports

import "context"

// PodTerminator stops one worker pod.
//
// A lease fences a worker at the executor's boundary: once the lease is gone or
// the task is terminal, nothing the worker sends is applied. That leaves the dbt
// process itself running against the warehouse while the task's execution slot is
// handed to somebody else, so the paths that take a task away from a live worker
// stop its pod as well as fence its reports.
type PodTerminator interface {
	// DeletePod removes one worker pod, but only if it is still the pod that
	// podUID identifies. A pod name is reused as a Deployment replaces its pods,
	// so deleting by name alone could take out a healthy successor; naming the
	// UID makes a delete that arrives late a no-op instead. A pod that is
	// already gone is not an error — its absence is the outcome asked for.
	DeletePod(ctx context.Context, podName, podUID string) error
}
