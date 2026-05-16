package handlers

import (
	"context"
	"errors"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"google.golang.org/grpc/metadata"
)

const callerIDMetadataKey = "x-caller-id"

// callerFromContext extracts the CallerID from the incoming gRPC metadata.
// Returns an error if the x-caller-id header is absent.
func callerFromContext(ctx context.Context) (run.CallerID, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", errors.New("missing gRPC metadata")
	}
	values := md.Get(callerIDMetadataKey)
	if len(values) == 0 {
		return "", errors.New("missing x-caller-id metadata")
	}
	return run.CallerID(values[0]), nil
}
