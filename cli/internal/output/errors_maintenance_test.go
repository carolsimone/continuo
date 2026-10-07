package output

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFromGRPC_MaintenanceIsNotRetryable(t *testing.T) {
	err := fmt.Errorf("%w: %w", ErrMaintenance, status.Error(codes.Unavailable, "continuo is in maintenance mode: new work is not accepted until it is turned off"))
	got := FromGRPC(err)
	if got.Code != CodeMaintenance || got.Retryable || got.ExitCode() != 7 {
		t.Fatalf("got %+v exit %d, want code maintenance, retryable false, exit 7", got, got.ExitCode())
	}
	if got.Message != "continuo is in maintenance mode: new work is not accepted until it is turned off" {
		t.Fatalf("message %q must be the server's", got.Message)
	}
}

// An UNAVAILABLE without the maintenance marker is an outage, not maintenance.
func TestFromGRPC_PlainUnavailableStaysUnavailable(t *testing.T) {
	got := FromGRPC(status.Error(codes.Unavailable, "connection refused"))
	if got.Code != CodeUnavailable || !got.Retryable {
		t.Fatalf("got %+v, want unavailable and retryable", got)
	}
	if errors.Is(status.Error(codes.Unavailable, "x"), ErrMaintenance) {
		t.Fatal("a bare status must not match ErrMaintenance")
	}
}
