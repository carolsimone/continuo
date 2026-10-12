package objectstore

import "errors"

// The errors a role reports about the object itself. An adapter translates
// them into the errors of the port it implements. Every other error a Client
// returns wraps the AWS SDK's, so an S3 5xx keeps its HTTP status
// (HTTPStatusCode) and a body cut short keeps io.ErrUnexpectedEOF, both of
// which pkg/redis classifies as infrastructure.
var (
	// ErrNotFound reports a read of a key that holds no object.
	ErrNotFound = errors.New("object not found")
	// ErrTooLarge reports an object larger than the caller accepts.
	ErrTooLarge = errors.New("object too large")
	// ErrInvalidRef reports a reference that names no object: a URI ParseRef
	// refuses, or a Ref whose Validate fails.
	ErrInvalidRef = errors.New("invalid object reference")
)
