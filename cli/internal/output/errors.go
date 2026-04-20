// Package output owns the CLI's success and error emission contract.
// errors.go centralizes gRPC-status → CLI-code translation so no other
// package imports google.golang.org/grpc/codes.
package output

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrorCode is the fixed vocabulary the CLI emits in JSON and maps to exit codes.
type ErrorCode string

const (
	CodeUsage       ErrorCode = "usage"
	CodeNotFound    ErrorCode = "not_found"
	CodeConflict    ErrorCode = "conflict"
	CodeUnavailable ErrorCode = "unavailable"
	CodeInternal    ErrorCode = "internal"
)

// CLIError is the structured error surfaced to stdout (JSON) and via exit code.
type CLIError struct {
	Code      ErrorCode `json:"code"`
	Message   string    `json:"message"`
	Retryable bool      `json:"retryable"`
}

// ExitCode returns the process exit code for this CLIError.
func (e CLIError) ExitCode() int {
	switch e.Code {
	case CodeUsage:
		return 2
	case CodeNotFound:
		return 3
	case CodeConflict:
		return 4
	case CodeUnavailable:
		return 5
	case CodeInternal:
		return 6
	default:
		return 1
	}
}

// Error satisfies the error interface so CLIError values are returnable from commands.
func (e CLIError) Error() string { return e.Message }

// NewUsageError builds a CLIError for argument/flag-level problems detected before the RPC.
func NewUsageError(msg string) CLIError {
	return CLIError{Code: CodeUsage, Message: msg, Retryable: false}
}

// FromGRPC translates any error (gRPC or not) into a CLIError with the correct code and retryable bit.
func FromGRPC(err error) CLIError {
	if err == nil {
		return CLIError{}
	}
	st, ok := status.FromError(err)
	if !ok {
		return CLIError{Code: CodeInternal, Message: err.Error(), Retryable: false}
	}
	msg := st.Message()
	switch st.Code() {
	case codes.NotFound:
		return CLIError{Code: CodeNotFound, Message: msg, Retryable: false}
	case codes.FailedPrecondition, codes.AlreadyExists, codes.Aborted:
		return CLIError{Code: CodeConflict, Message: msg, Retryable: true}
	case codes.Unavailable, codes.DeadlineExceeded:
		return CLIError{Code: CodeUnavailable, Message: msg, Retryable: true}
	case codes.InvalidArgument:
		return CLIError{Code: CodeUsage, Message: msg, Retryable: false}
	case codes.Internal, codes.Unknown:
		return CLIError{Code: CodeInternal, Message: msg, Retryable: false}
	default:
		return CLIError{Code: CodeInternal, Message: msg, Retryable: false}
	}
}
