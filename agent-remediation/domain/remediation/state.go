// Package remediation holds the state of one remediation run: the run itself,
// each try it makes, each cluster of failing nodes in a try, and each step
// that works on a cluster. Every level has a closed set of statuses and a
// table of the moves between them.
package remediation

import "slices"

// RemediationStatus is the state of a whole remediation run.
type RemediationStatus string

const (
	// StatusRunning: a worker may claim the run and work on it.
	StatusRunning RemediationStatus = "running"
	// StatusDone: the run reached its outcome.
	StatusDone RemediationStatus = "done"
	// StatusSystemFailure: the run stopped on a failure of the system, not of a fix.
	// A redrive moves it back to running.
	StatusSystemFailure RemediationStatus = "system_failure"
)

var legalRemediationMoves = map[RemediationStatus][]RemediationStatus{
	StatusRunning:       {StatusDone, StatusSystemFailure},
	StatusSystemFailure: {StatusRunning},
}

// AllRemediationStatuses lists every status a remediation run can hold.
func AllRemediationStatuses() []RemediationStatus {
	return []RemediationStatus{StatusRunning, StatusDone, StatusSystemFailure}
}

// CanMoveTo reports whether a run in this status may move to `to`.
func (s RemediationStatus) CanMoveTo(to RemediationStatus) bool {
	return slices.Contains(legalRemediationMoves[s], to)
}

// TryStatus is the state of one try of a remediation run.
type TryStatus string

const (
	TryDrafting  TryStatus = "drafting"
	TryChecking  TryStatus = "checking"
	TryVerifying TryStatus = "verifying"
	TryPassed    TryStatus = "passed"
	TryFailed    TryStatus = "failed"
)

var legalTryMoves = map[TryStatus][]TryStatus{
	TryDrafting:  {TryChecking, TryFailed},
	TryChecking:  {TryVerifying, TryFailed},
	TryVerifying: {TryPassed, TryFailed},
}

// AllTryStatuses lists every status a try can hold.
func AllTryStatuses() []TryStatus {
	return []TryStatus{TryDrafting, TryChecking, TryVerifying, TryPassed, TryFailed}
}

// CanMoveTo reports whether a try in this status may move to `to`.
func (s TryStatus) CanMoveTo(to TryStatus) bool {
	return slices.Contains(legalTryMoves[s], to)
}

// ClusterStatus is the state of one cluster of failing nodes inside a try.
type ClusterStatus string

const (
	ClusterGathering ClusterStatus = "gathering"
	ClusterProposing ClusterStatus = "proposing"
	// ClusterPreflight: a draft is under the pre-flight checks. A failed check
	// sends the cluster back to ClusterProposing for a revision round.
	ClusterPreflight  ClusterStatus = "preflight"
	ClusterChecked    ClusterStatus = "checked"
	ClusterHandedOver ClusterStatus = "handed_over"
	ClusterFellBack   ClusterStatus = "fell_back"
)

var legalClusterMoves = map[ClusterStatus][]ClusterStatus{
	ClusterGathering: {ClusterProposing, ClusterHandedOver, ClusterFellBack},
	ClusterProposing: {ClusterPreflight, ClusterHandedOver, ClusterFellBack},
	ClusterPreflight: {ClusterChecked, ClusterProposing, ClusterHandedOver, ClusterFellBack},
}

// AllClusterStatuses lists every status a cluster can hold.
func AllClusterStatuses() []ClusterStatus {
	return []ClusterStatus{
		ClusterGathering, ClusterProposing, ClusterPreflight,
		ClusterChecked, ClusterHandedOver, ClusterFellBack,
	}
}

// CanMoveTo reports whether a cluster in this status may move to `to`.
func (s ClusterStatus) CanMoveTo(to ClusterStatus) bool {
	return slices.Contains(legalClusterMoves[s], to)
}

// StepStatus is the state of one step of a flow.
type StepStatus string

const (
	StepRunning StepStatus = "running"
	// StepWaiting: the step started something outside the worker (a
	// verification run) and waits for its answer.
	StepWaiting StepStatus = "waiting"
	StepDone    StepStatus = "done"
	// StepFailed: the step failed. A retry moves it back to StepRunning.
	StepFailed StepStatus = "failed"
)

var legalStepMoves = map[StepStatus][]StepStatus{
	StepRunning: {StepDone, StepFailed, StepWaiting},
	StepWaiting: {StepDone, StepFailed},
	StepFailed:  {StepRunning},
}

// AllStepStatuses lists every status a step can hold.
func AllStepStatuses() []StepStatus {
	return []StepStatus{StepRunning, StepWaiting, StepDone, StepFailed}
}

// CanMoveTo reports whether a step in this status may move to `to`.
func (s StepStatus) CanMoveTo(to StepStatus) bool {
	return slices.Contains(legalStepMoves[s], to)
}
