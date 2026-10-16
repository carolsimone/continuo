package handlers

import (
	"errors"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/runplan"
	"github.com/carolsimone/continuo/state/service/runstart"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// planRefusal maps a refusal of a new run's selection to the status the RPC
// returns: a node the run names that its release does not hold is NotFound, a
// full refresh of a node type that cannot be rebuilt from scratch is
// InvalidArgument, and every other refusal is FailedPrecondition. ok is false
// when err is not a refusal.
func planRefusal(err error) (st *status.Status, ok bool) {
	switch {
	case errors.Is(err, runplan.ErrTargetNotFound):
		return status.New(codes.NotFound, err.Error()), true
	case errors.Is(err, runplan.ErrFullRefreshUnsupported):
		return status.New(codes.InvalidArgument, err.Error()), true
	case errors.Is(err, runplan.ErrEmptySelection),
		errors.Is(err, runplan.ErrNoTests),
		errors.Is(err, runplan.ErrRerunOfTestUnsupported),
		errors.Is(err, runplan.ErrInvalidNodeType),
		errors.Is(err, runplan.ErrNothingToRerun),
		errors.Is(err, runstart.ErrSourceHasNoRelease):
		return status.New(codes.FailedPrecondition, err.Error()), true
	}
	return nil, false
}

// rebaseRefusal maps a refusal of TriggerRebase or PreviewRebase to the
// status the RPC returns: a source run that does not exist is NotFound; a
// source that cannot be rebased, a schedule with an active run and a
// selection refusal are FailedPrecondition with the error's text; and a live
// release other than the one the caller previewed is FailedPrecondition with
// exactly runstart.ErrLiveReleaseMoved's text, which callers compare. ok is
// false when err is not a refusal.
func rebaseRefusal(err error) (st *status.Status, ok bool) {
	switch {
	case errors.Is(err, runstart.ErrLiveReleaseMoved):
		return status.New(codes.FailedPrecondition, runstart.ErrLiveReleaseMoved.Error()), true
	case errors.Is(err, postgres.ErrNotFound):
		return status.New(codes.NotFound, "source run not found"), true
	case errors.Is(err, run.ErrSourceMustBeTerminallyFailedOrCancelled),
		errors.Is(err, run.ErrNothingToRebase),
		errors.Is(err, run.ErrScheduleHasActiveRun):
		return status.New(codes.FailedPrecondition, err.Error()), true
	}
	return planRefusal(err)
}
