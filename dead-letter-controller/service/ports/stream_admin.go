package ports

import "context"

// StreamAdmin creates the Redis streams and consumer groups the system reads
// and writes. Every call takes the contract name of the stream (for example
// streams.TaskDispatchV1); the implementation maps it to the key it stores the
// stream under. Both calls are idempotent: a second call creates nothing.
type StreamAdmin interface {
	// EnsureGroup creates the stream, when it does not exist, and the group on
	// it, reading from the start of the stream. created is false when the group
	// already exists.
	EnsureGroup(ctx context.Context, stream, group string) (created bool, err error)
	// EnsureStream creates the stream, when it does not exist, with no group
	// left on it: the stream a producer adds to without creating it. created is
	// false when the stream already exists.
	EnsureStream(ctx context.Context, stream string) (created bool, err error)
}
