package grpc

import (
	"errors"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// toStatus maps a use-case error to its gRPC status. The CLI maps these codes
// to its exit codes: InvalidArgument → 2, NotFound → 3, FailedPrecondition → 4.
func toStatus(err error) error {
	switch {
	case errors.Is(err, deadletter.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, deadletter.ErrExpired), errors.Is(err, deadletter.ErrNotRedrivable):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, deadletter.ErrReasonRequired), errors.Is(err, deadletter.ErrNoIDs), errors.Is(err, deadletter.ErrInvalidFilter):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
