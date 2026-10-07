// Package output owns the CLI's success and error emission contract.
// errors.go centralizes gRPC-status → CLI-code translation so no other
// package imports google.golang.org/grpc/codes.
package output

import (
	"errors"

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
	// CodeMaintenance marks a call the server refused because continuo is in
	// maintenance mode. It is not retryable: the refusal lasts until an
	// operator turns maintenance off.
	CodeMaintenance ErrorCode = "maintenance"
)

// ErrMaintenance marks an error the server refused because continuo is in
// maintenance mode. The gRPC clients wrap a refusal with it when the reply
// carries the maintenance trailer.
var ErrMaintenance = errors.New("maintenance mode")

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
	case CodeMaintenance:
		return 7
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
	if errors.Is(err, ErrMaintenance) {
		return CLIError{Code: CodeMaintenance, Message: serverMessage(err), Retryable: false}
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

// serverMessage returns the message of the gRPC status wrapped inside err.
// status.FromError reports the whole wrapped error text for a wrapped status,
// so the status is located with errors.As to recover the server's own message.
func serverMessage(err error) string {
	var carrier interface{ GRPCStatus() *status.Status }
	if errors.As(err, &carrier) {
		if st := carrier.GRPCStatus(); st != nil {
			return st.Message()
		}
	}
	return err.Error()
}
