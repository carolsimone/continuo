package run

import "errors"

// Aggregate-level errors. Returned by Run methods to indicate a domain-level
// rejection; adapters translate them to gRPC codes / consumer ACK policies.
var (
	// ErrAlreadyTerminal is returned by mutating methods invoked on a Run
	// whose status is terminal: SUCCEEDED, FAILED, CANCELLED or SKIPPED.
	ErrAlreadyTerminal = errors.New("run is already in a terminal state")

	// ErrAlreadyCancelling is returned by Run.Cancel on a run that is already
	// cancelling: the first cancel's stops stand, and the second writes
	// nothing.
	ErrAlreadyCancelling = errors.New("run is already cancelling")

	// ErrSourceMustBeTerminallyFailedOrCancelled is returned by
	// CanBeRerunSource / CanBeRebaseSource when the source run is not in
	// {FAILED, CANCELLED}.
	ErrSourceMustBeTerminallyFailedOrCancelled = errors.New(
		"source run must be in terminal FAILED or CANCELLED state")

	// ErrNothingToRerun is returned by CanBeRerunSource when the source
	// run has zero non-SUCCEEDED tasks. Its message is distinct from
	// ErrNothingToRebase so logs and gRPC status text identify which trigger
	// the source was rejected for.
	ErrNothingToRerun = errors.New("source run has no non-SUCCEEDED tasks to rerun")

	// ErrNothingToRebase is returned by CanBeRebaseSource for the same
	// underlying reason, with distinct message text from ErrNothingToRerun.
	ErrNothingToRebase = errors.New("source run has no non-SUCCEEDED tasks to rebase")

	// ErrInvalidKind is returned by constructors when Kind is not one of the
	// allowed wire-stable values.
	ErrInvalidKind = errors.New("invalid run kind")

	// ErrScheduleNameRequired is returned by constructors when schedule_name
	// is empty.
	ErrScheduleNameRequired = errors.New("schedule_name is required")

	// ErrRunIDRequired is returned by NewPromotedSeedsRun when the caller-supplied
	// run id is the nil UUID. That constructor takes its id rather than generating
	// one, so an unset id would otherwise collide across every promotion.
	ErrRunIDRequired = errors.New("run id is required")

	// ErrRunAlreadyExists is returned by RunRepository.SaveRun when a newly
	// constructed Run's id is already persisted; nothing is written. Only a
	// constructor that takes its id (NewPromotedSeedsRun) can produce it: the id
	// is derived from the triggering event, so a second delivery of that event
	// finds the run the first one created.
	ErrRunAlreadyExists = errors.New("run already exists")

	// ErrReleaseIDRequired is returned by NewPromotedSeedsRun when the release
	// that triggered the run is not identified.
	ErrReleaseIDRequired = errors.New("release_id is required")

	// ErrNoNodes is returned by Run.Start when the plan holds no task. A run
	// with no tasks would never reach a terminal state on its own.
	ErrNoNodes = errors.New("run requires at least one node")

	// ErrAlreadyPlanned is returned by Run.Start when the run's tasks are
	// already stored: a run is planned once.
	ErrAlreadyPlanned = errors.New("run is already planned")

	// ErrPlanNodeNotInGraph is returned by Run.Start when the plan names a
	// node the graph it was chosen from does not hold.
	ErrPlanNodeNotInGraph = errors.New("planned node is not in the graph")

	// ErrPlanReadyNotPending is returned by Run.Start when the plan lists a
	// ready task that is not one of its pending tasks.
	ErrPlanReadyNotPending = errors.New("ready task is not a pending task of the plan")

	// ErrScheduleHasActiveRun is returned by the SchedulePolicy domain
	// service when the named schedule already has a PENDING or RUNNING run.
	ErrScheduleHasActiveRun = errors.New("schedule already has an active run")

	// ErrScheduleNotInCatalog is returned by the SchedulePolicy when the
	// named schedule has no active row in schedule_catalog.
	ErrScheduleNotInCatalog = errors.New("schedule not found in catalog")

	// ErrTaskNotHeld is returned by Run.RecordAttemptReport when the run the
	// report names holds no task with the report's task id: no run holds it,
	// or another run does. A run's tasks are stored in the transaction that
	// creates it, before any of them is dispatched, and a task never moves to
	// another run, so such a report never becomes valid; the stream binding
	// dead-letters it at once.
	ErrTaskNotHeld = errors.New("the run holds no such task")

	// ErrAttemptNotIssued is returned by Run.RecordAttemptReport when the task
	// has no row for the reported attempt: state never dispatched it. state
	// writes an attempt's row in the transaction that dispatches it, so such a
	// report never becomes valid; the stream binding dead-letters it at once.
	ErrAttemptNotIssued = errors.New("state never dispatched the reported attempt")

	// ErrStopNotRequested is returned by Run.RecordAttemptReport for a stopped
	// report of an open attempt state never asked to stop: only the current
	// attempt of a stopping task is stopped on request, so such a report never
	// becomes valid.
	ErrStopNotRequested = errors.New("state never asked the reported attempt to stop")

	// ErrInvalidRunFailureReason is returned by Run.MarkDispatchTerminal when
	// it is asked to fail a run with no failure reason or one that
	// RunFailureReasons does not list.
	ErrInvalidRunFailureReason = errors.New("a failed run needs a declared failure reason")

	// ErrTaskNotFound is returned by Run.HasTaskAt and by TaskCollection
	// implementations when a specific task lookup yields nothing.
	ErrTaskNotFound = errors.New("task not found")
)
