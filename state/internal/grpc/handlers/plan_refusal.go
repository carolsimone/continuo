package handlers

import (
	"errors"

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
