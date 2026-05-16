package run

import "errors"

// Aggregate-level errors. Returned by Run methods to indicate a domain-level
// rejection; adapters translate them to gRPC codes / consumer ACK policies.
var (
	// ErrAlreadyTerminal is returned by mutating methods invoked on a Run
	// whose status is in {SUCCEEDED, FAILED, CANCELLED}.
	ErrAlreadyTerminal = errors.New("run is already in a terminal state")

	// ErrSourceMustBeTerminallyFailedOrCancelled is returned by
	// CanBeRerunSource / CanBeRebaseSource when the source run is not in
	// {FAILED, CANCELLED}.
	ErrSourceMustBeTerminallyFailedOrCancelled = errors.New(
		"source run must be in terminal FAILED or CANCELLED state")

	// ErrNothingToRerun is returned by CanBeRerunSource when the source
	// run has zero non-SUCCEEDED tasks.
	ErrNothingToRerun = errors.New("source run has no non-SUCCEEDED tasks")

	// ErrNothingToRebase is returned by CanBeRebaseSource for the same reason.
	ErrNothingToRebase = errors.New("source run has no non-SUCCEEDED tasks")

	// ErrInvalidKind is returned by constructors when Kind is not one of the
	// allowed wire-stable values.
	ErrInvalidKind = errors.New("invalid run kind")

	// ErrScheduleNameRequired is returned by constructors when schedule_name
	// is empty.
	ErrScheduleNameRequired = errors.New("schedule_name is required")

	// ErrScheduleHasActiveRun is returned by the SchedulePolicy domain
	// service when the named schedule already has a PENDING or RUNNING run.
	ErrScheduleHasActiveRun = errors.New("schedule already has an active run")

	// ErrScheduleNotInCatalog is returned by the SchedulePolicy when the
	// named schedule has no active row in schedule_catalog.
	ErrScheduleNotInCatalog = errors.New("schedule not found in catalog")

	// ErrTaskRowNotProjected is returned by Run.RecordTaskStatus when the
	// task_tracker row referenced by the event does not yet exist. The
	// adapter binding interprets this as a transient error and the message
	// is redelivered.
	ErrTaskRowNotProjected = errors.New("task_tracker row not yet projected")

	// ErrTaskNotFound is returned by Run.HasTaskAt and by TaskCollection
	// implementations when a specific task lookup yields nothing.
	ErrTaskNotFound = errors.New("task not found")
)
