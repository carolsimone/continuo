package ports

import (
	"context"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/unprefixed"
)

// StreamAdmin creates the Redis streams and consumer groups the system reads
// and writes, deletes the unprefixed keys of the streams, and checks that the
// Redis logins of the services sign in. Every stream call takes the contract
// name of the stream (for example streams.TaskDispatchV1); the implementation
// maps it to the key it acts on. Every stream call is idempotent: a second
// call creates or deletes nothing.
type StreamAdmin interface {
	// CheckLogin opens a new connection to the same Redis, signs in as
	// username with password and closes it. It returns Redis's answer when the
	// sign-in is refused (the login does not exist, is turned off, or has
	// another password) or when Redis cannot be reached.
	CheckLogin(ctx context.Context, username, password string) error
	// EnsureGroup creates the stream, when it does not exist, and the group on
	// it, reading from the start of the stream. created is false when the group
	// already exists.
	EnsureGroup(ctx context.Context, stream, group string) (created bool, err error)
	// EnsureStream creates the stream, when it does not exist, with no group
	// left on it: the stream a producer adds to without creating it. created is
	// false when the stream already exists.
	EnsureStream(ctx context.Context, stream string) (created bool, err error)
	// StreamExists reports whether the key the stream is stored under exists.
	StreamExists(ctx context.Context, stream string) (bool, error)
	// ReadUnprefixed reads the unprefixed key of stream, the key named by the
	// bare contract name: every consumer group it holds and its last entry.
	// exists is false when the key does not exist.
	ReadUnprefixed(ctx context.Context, stream string) (k unprefixed.Key, exists bool, err error)
	// DeleteUnprefixed deletes the unprefixed key of stream and reports whether
	// it existed. It never touches the stream's prefixed key.
	DeleteUnprefixed(ctx context.Context, stream string) (deleted bool, err error)
}
