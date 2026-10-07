package client

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/cli/internal/output"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// maintenanceTrailerKey is the trailer a continuo service sets to "true" when
// it refuses a call because maintenance mode is on.
const maintenanceTrailerKey = "continuo-maintenance"

// maintenanceInterceptor reads every reply's trailer and, when the call was
// refused for maintenance, wraps the error with output.ErrMaintenance so the
// CLI reports code "maintenance" instead of a retryable "unavailable".
func maintenanceInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	var trailer metadata.MD
	err := invoker(ctx, method, req, reply, cc, append(opts, grpc.Trailer(&trailer))...)
	if err != nil && len(trailer.Get(maintenanceTrailerKey)) > 0 && trailer.Get(maintenanceTrailerKey)[0] == "true" {
		return fmt.Errorf("%w: %w", output.ErrMaintenance, err)
	}
	return err
}
