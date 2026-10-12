package objectstore

import (
	"fmt"
	"strings"
)

// s3Scheme prefixes a URI that names its bucket.
const s3Scheme = "s3://"

// Ref names one object: a key in a bucket. An empty Bucket is the default
// bucket of the store that serves the Ref. A Ref received from outside the
// process comes from ParseRef; a Ref built in code is checked by Validate
// before any store is reached.
type Ref struct {
	Bucket string
	Key    string
}

// ParseRef validates uri, a reference received from outside the process (an
// event field, a database row), and returns the object it names. "s3://b/k"
// names key k in bucket b; a value without a scheme is a bare key in the
// default bucket. Every other value is an error wrapping ErrInvalidRef: an
// empty or blank value, "s3://" or an s3:// URI without a bucket, a bucket
// without a key ("s3://b", "s3://b/"), a key that begins with "/", and any
// other scheme.
func ParseRef(uri string) (Ref, error) {
	rest, isS3 := strings.CutPrefix(uri, s3Scheme)
	var ref Ref
	switch {
	case isS3:
		bucket, key, _ := strings.Cut(rest, "/")
		if bucket == "" {
			return Ref{}, fmt.Errorf("%w: %q names no bucket", ErrInvalidRef, uri)
		}
		ref = Ref{Bucket: bucket, Key: key}
	case strings.Contains(uri, "://"):
		return Ref{}, fmt.Errorf("%w: %q is neither an s3:// URI nor a bare key", ErrInvalidRef, uri)
	default:
		ref = Ref{Key: uri}
	}
	if err := ref.Validate(); err != nil {
		return Ref{}, fmt.Errorf("parse %q: %w", uri, err)
	}
	return ref, nil
}

// Validate reports, wrapping ErrInvalidRef, a Ref that names no object: a
// blank key, a key that begins with "/", or a bucket that contains "/".
func (r Ref) Validate() error {
	switch {
	case strings.TrimSpace(r.Key) == "":
		return fmt.Errorf("%w: no object key", ErrInvalidRef)
	case strings.HasPrefix(r.Key, "/"):
		return fmt.Errorf("%w: key %q begins with a slash", ErrInvalidRef, r.Key)
	case strings.Contains(r.Bucket, "/"):
		return fmt.Errorf("%w: bucket %q contains a slash", ErrInvalidRef, r.Bucket)
	}
	return nil
}

// String returns the Ref as a URI: "s3://<bucket>/<key>", or the bare key
// when Bucket is empty. For every Ref ParseRef returns, ParseRef(ref.String())
// returns that Ref again.
func (r Ref) String() string {
	if r.Bucket == "" {
		return r.Key
	}
	return s3Scheme + r.Bucket + "/" + r.Key
}
