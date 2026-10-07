package client

import (
	"context"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/cli/internal/output"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func invokerReturning(trailer metadata.MD, err error) grpc.UnaryInvoker {
	return func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, opts ...grpc.CallOption) error {
		for _, o := range opts {
			if t, ok := o.(grpc.TrailerCallOption); ok {
				*t.TrailerAddr = trailer
			}
		}
		return err
	}
}

func TestMaintenanceInterceptor_WrapsAMarkedRefusal(t *testing.T) {
	refusal := status.Error(codes.Unavailable, "continuo is in maintenance mode: new work is not accepted until it is turned off")
	err := maintenanceInterceptor(context.Background(), "/m", nil, nil, nil,
		invokerReturning(metadata.Pairs(maintenanceTrailerKey, "true"), refusal))
	if !errors.Is(err, output.ErrMaintenance) {
		t.Fatalf("want ErrMaintenance, got %v", err)
	}
}

func TestMaintenanceInterceptor_LeavesOtherErrorsAlone(t *testing.T) {
	outage := status.Error(codes.Unavailable, "connection refused")
	err := maintenanceInterceptor(context.Background(), "/m", nil, nil, nil, invokerReturning(nil, outage))
	if errors.Is(err, output.ErrMaintenance) || status.Code(err) != codes.Unavailable {
		t.Fatalf("an unmarked UNAVAILABLE must pass through unchanged, got %v", err)
	}
}
