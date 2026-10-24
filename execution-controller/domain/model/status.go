package model

import "slices"

// Status is the lifecycle state of a Deployment. A deployment waits in
// pending (or blocked, for a candidate node whose in-set upstreams have not all
// succeeded) for as long as it takes. It holds an execution slot while
// reserved, starting, running or terminating, and ends done (its attempt
// ended, or was stopped before it could start), failed (it could not be deployed) or skipped (an in-set upstream
// failed). reserved is not a waiting state: it lasts from the claim that took
// the slot to the launch that creates the Job.
type Status string

const (
	StatusPending Status = "pending"
	StatusBlocked Status = "blocked"
	// StatusReserved holds a slot; the Job is not created yet.
	StatusReserved Status = "reserved"
	// StatusStarting holds a slot; the Job is created.
	StatusStarting Status = "starting"
	// StatusRunning holds a slot; the Job's main container started.
	StatusRunning Status = "running"
	// StatusTerminating holds a slot; execution-controller deleted the Job to
	// end the attempt itself and waits for its pods to be gone.
	StatusTerminating Status = "terminating"
	// StatusDone holds no slot: the attempt ended and its outcome is recorded.
	StatusDone    Status = "done"
	StatusFailed  Status = "failed"
	StatusSkipped Status = "skipped"
)

// AllStatuses lists every status a deployment can have. The migration's CHECK
// constraint on deployments.status must list exactly these
// (TestExecutionSchemaMatchesContract).
func AllStatuses() []Status {
	return []Status{StatusPending, StatusBlocked, StatusReserved, StatusStarting,
		StatusRunning, StatusTerminating, StatusDone, StatusFailed, StatusSkipped}
}

// InFlightStatuses lists the statuses that hold an execution slot. Queries
// bind this set; the deployments trigger and in-flight index repeat it and are
// pinned to it by TestExecutionSchemaMatchesContract.
func InFlightStatuses() []Status {
	return []Status{StatusReserved, StatusStarting, StatusRunning, StatusTerminating}
}

// InFlight reports whether a deployment in this status holds an execution slot.
func (s Status) InFlight() bool { return slices.Contains(InFlightStatuses(), s) }

// transitions is the deployment state machine: for each status, the statuses
// a deployment may move to. A status with no entry is terminal.
//
//	pending     → reserved                   the claim takes a slot
//	pending     → done                       the attempt was stopped before its claim
//	pending     → terminating                the attempt was stopped, and a Job of it
//	                                         that no launch recorded exists
//	blocked     → pending | skipped          in-set upstreams succeeded | one failed
//	reserved    → starting                   the Job was created
//	reserved    → pending                    a transient deploy failure, or a stale
//	                                         reservation, gives the slot back
//	reserved    → failed                     the deployment cannot be deployed
//	reserved    → done                       the attempt was stopped before its Job was created
//	reserved    → terminating                the attempt was stopped, and a Job of it
//	                                         that no launch recorded exists
//	starting    → running                    the main container started
//	starting    → terminating                a start error or the start timeout:
//	                                         execution-controller deleted the Job
//	starting    → done                       the attempt ended before it was seen running
//	running     → terminating | done         the Job was deleted | the attempt ended
//	terminating → done                       the attempt's pods are gone
var transitions = map[Status][]Status{
	StatusPending:     {StatusReserved, StatusDone, StatusTerminating},
	StatusBlocked:     {StatusPending, StatusSkipped},
	StatusReserved:    {StatusStarting, StatusPending, StatusFailed, StatusDone, StatusTerminating},
	StatusStarting:    {StatusRunning, StatusTerminating, StatusDone},
	StatusRunning:     {StatusTerminating, StatusDone},
	StatusTerminating: {StatusDone},
}

// CanMove reports whether the state machine allows a deployment in from to
// move to to.
func CanMove(from, to Status) bool { return slices.Contains(transitions[from], to) }

// StatusStrings converts statuses to the text values the deployments table
// stores, for binding as a query parameter.
func StatusStrings(ss []Status) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = string(s)
	}
	return out
}
