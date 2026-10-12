package objectstore

import "context"

// Reader reads one object. A Ref with an empty Bucket reads the default
// bucket. A missing object is ErrNotFound, an object larger than maxBytes
// ErrTooLarge, and a Ref that names no object ErrInvalidRef; maxBytes must be
// positive and below math.MaxInt64. Any other error is a failure to reach the
// store and keeps its cause.
type Reader interface {
	Get(ctx context.Context, ref Ref, maxBytes int64) ([]byte, error)
}

// Writer writes one object, replacing any object at ref. A Ref with an empty
// Bucket writes to the default bucket; a Ref that names no object is
// ErrInvalidRef. Any other error keeps its cause.
type Writer interface {
	Put(ctx context.Context, ref Ref, body []byte, contentType string) error
}

// PrefixDeleter deletes every object of the default bucket whose key begins
// with prefix. Nothing matching is not an error; an empty prefix, which names
// the whole bucket, is refused.
type PrefixDeleter interface {
	DeletePrefix(ctx context.Context, prefix string) error
}

// ReadWriter is the role of an adapter that both reads and writes objects.
type ReadWriter interface {
	Reader
	Writer
}

// Client implements every role.
var (
	_ Reader        = (*Client)(nil)
	_ Writer        = (*Client)(nil)
	_ PrefixDeleter = (*Client)(nil)
	_ ReadWriter    = (*Client)(nil)
)
