package objectstore

import (
	"context"
	"io"
	"time"
)

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

// Object describes one stored object: its full key, its size in bytes and the
// time it was last modified.
type Object struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// ObjectWriter writes one object as a stream. Nothing is visible at the key
// until Close completes the object; Abort discards everything written. After
// either, Write and a second Close return ErrWriterClosed. A Write or Close
// that fails to reach the store keeps the cause, every later Write and Close
// returns the same error, and the writer can still be aborted. Abort is
// idempotent, and after a successful Close it does nothing.
type ObjectWriter interface {
	io.Writer
	Close() error
	Abort() error
}

// StreamWriter writes one object without holding it whole in memory, replacing
// any object at ref. The upload is opened with the call, so a store that
// refuses writes fails OpenWriter, before the caller has written anything. A
// Ref with an empty Bucket writes to the default bucket; a Ref that names no
// object is ErrInvalidRef. Any other error keeps its cause.
//
// The Client uploads the object as an S3 multipart upload of 8 MiB parts and
// holds one part in memory, so an object is limited to 10,000 parts (80 GiB).
type StreamWriter interface {
	OpenWriter(ctx context.Context, ref Ref, contentType string) (ObjectWriter, error)
}

// StreamReader reads one object as a stream, without reading it whole. The
// caller closes the stream. A Ref with an empty Bucket reads the default
// bucket. A missing object is ErrNotFound and a Ref that names no object
// ErrInvalidRef; the size of the object is not limited. Any other error keeps
// its cause, and so does a failure while the stream is read.
type StreamReader interface {
	GetStream(ctx context.Context, ref Ref) (io.ReadCloser, error)
}

// Lister lists the objects of the default bucket whose key begins with prefix,
// in key order. Nothing matching is an empty list, not an error, and an empty
// prefix lists the whole bucket.
type Lister interface {
	List(ctx context.Context, prefix string) ([]Object, error)
}

// Deleter deletes one object. A Ref with an empty Bucket deletes from the
// default bucket and a Ref that names no object is ErrInvalidRef. An object
// that does not exist is not an error. Any other error keeps its cause.
type Deleter interface {
	Delete(ctx context.Context, ref Ref) error
}

// Client implements every role.
var (
	_ Reader        = (*Client)(nil)
	_ Writer        = (*Client)(nil)
	_ PrefixDeleter = (*Client)(nil)
	_ ReadWriter    = (*Client)(nil)
	_ StreamWriter  = (*Client)(nil)
	_ StreamReader  = (*Client)(nil)
	_ Lister        = (*Client)(nil)
	_ Deleter       = (*Client)(nil)
)
