package ports

import "context"

// StreamAdmin creates the Redis streams and consumer groups the system reads
// and writes. Both calls are idempotent: a second call creates nothing.
type StreamAdmin interface {
	// EnsureGroup creates the stream at key, when it does not exist, and the
	// group on it, reading from the start of the stream. created is false when
	// the group already exists.
	EnsureGroup(ctx context.Context, key, group string) (created bool, err error)
	// EnsureStream creates the stream at key, when it does not exist, with no
	// group left on it: the stream a producer adds to without creating it.
	// created is false when the stream already exists.
	EnsureStream(ctx context.Context, key string) (created bool, err error)
}
