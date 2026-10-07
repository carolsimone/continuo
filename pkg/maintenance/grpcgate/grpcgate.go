// Package grpcgate refuses chosen gRPC methods while maintenance mode is on.
package grpcgate

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/carolsimone/continuo/pkg/maintenance"
)

// UnaryServerInterceptor refuses every call to one of methods (full method
// names, e.g. "/state.v1.StateService/TriggerSchedule") when enabled is true:
// the call answers UNAVAILABLE with maintenance.Message and the trailer
// maintenance.TrailerKey=true, and its handler never runs. Every other method,
// and every method when enabled is false, passes through untouched.
func UnaryServerInterceptor(enabled bool, methods ...string) grpc.UnaryServerInterceptor {
	gated := make(map[string]struct{}, len(methods))
	for _, m := range methods {
		gated[m] = struct{}{}
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !enabled {
			return handler(ctx, req)
		}
		if _, ok := gated[info.FullMethod]; !ok {
			return handler(ctx, req)
		}
		_ = grpc.SetTrailer(ctx, metadata.Pairs(maintenance.TrailerKey, "true"))
		return nil, status.Error(codes.Unavailable, maintenance.Message)
	}
}
