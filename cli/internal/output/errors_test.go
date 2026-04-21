package output

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFromGRPC_NotFound(t *testing.T) {
	e := FromGRPC(status.Error(codes.NotFound, "schedule 'x' not found"))
	assert.Equal(t, CodeNotFound, e.Code)
	assert.Equal(t, "schedule 'x' not found", e.Message)
	assert.False(t, e.Retryable)
	assert.Equal(t, 3, e.ExitCode())
}

func TestFromGRPC_FailedPreconditionIsConflict(t *testing.T) {
	e := FromGRPC(status.Error(codes.FailedPrecondition, "already running"))
	assert.Equal(t, CodeConflict, e.Code)
	assert.True(t, e.Retryable)
	assert.Equal(t, 4, e.ExitCode())
}

func TestFromGRPC_AlreadyExistsIsConflict(t *testing.T) {
	e := FromGRPC(status.Error(codes.AlreadyExists, "dup"))
	assert.Equal(t, CodeConflict, e.Code)
}

func TestFromGRPC_UnavailableIsRetryable(t *testing.T) {
	e := FromGRPC(status.Error(codes.Unavailable, "server down"))
	assert.Equal(t, CodeUnavailable, e.Code)
	assert.True(t, e.Retryable)
	assert.Equal(t, 5, e.ExitCode())
}

func TestFromGRPC_DeadlineExceededIsUnavailable(t *testing.T) {
	e := FromGRPC(status.Error(codes.DeadlineExceeded, "timeout"))
	assert.Equal(t, CodeUnavailable, e.Code)
	assert.True(t, e.Retryable)
}

func TestFromGRPC_InvalidArgumentIsUsage(t *testing.T) {
	e := FromGRPC(status.Error(codes.InvalidArgument, "bad input"))
	assert.Equal(t, CodeUsage, e.Code)
	assert.Equal(t, 2, e.ExitCode())
}

func TestFromGRPC_InternalIsInternal(t *testing.T) {
	e := FromGRPC(status.Error(codes.Internal, "boom"))
	assert.Equal(t, CodeInternal, e.Code)
	assert.Equal(t, 6, e.ExitCode())
}

func TestFromGRPC_UnknownGRPCBucketsToInternal(t *testing.T) {
	e := FromGRPC(status.Error(codes.Unknown, "???"))
	assert.Equal(t, CodeInternal, e.Code)
}

func TestFromGRPC_NonGRPCErrorIsInternal(t *testing.T) {
	e := FromGRPC(errors.New("plain error"))
	assert.Equal(t, CodeInternal, e.Code)
	assert.Equal(t, "plain error", e.Message)
}

func TestNewUsageErrorExitCode(t *testing.T) {
	e := NewUsageError("missing <schedule-name>")
	assert.Equal(t, CodeUsage, e.Code)
	assert.Equal(t, 2, e.ExitCode())
}
