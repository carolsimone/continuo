// Package s3 implements remediation's object-storage port, the log reader,
// over the objectstore.Reader role.
package s3

import (
	"context"
	"errors"

	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/remediation/service/ports"
)

// maxLogBytes caps the log object held in memory, the ceiling the code-bundle
// and topology-artifact readers use. A dbt log of one node is far below it.
const maxLogBytes int64 = 64 << 20

// LogReader implements ports.LogReader over an object-store reader: it
// fetches dbt log and run-result objects.
type LogReader struct {
	objects objectstore.Reader
}

var _ ports.LogReader = (*LogReader)(nil)

// NewLogReader builds a LogReader over objects. A URI without a bucket reads
// the store's default bucket.
func NewLogReader(objects objectstore.Reader) *LogReader {
	return &LogReader{objects: objects}
}

// Fetch returns the object at uri as a string. A log that cannot be read for
// good yields the bare ports.ErrLogNotFound, which the classifier compares by
// identity and classifies without the log (unknown:log_unavailable): a uri
// that names no object (an empty one included), a missing object, and an
// object larger than maxLogBytes. Any other error is returned with its cause,
// so the message is retried.
func (r *LogReader) Fetch(ctx context.Context, uri string) (string, error) {
	ref, err := objectstore.ParseRef(uri)
	if err != nil {
		return "", ports.ErrLogNotFound
	}
	body, err := r.objects.Get(ctx, ref, maxLogBytes)
	switch {
	case errors.Is(err, objectstore.ErrNotFound), errors.Is(err, objectstore.ErrTooLarge):
		return "", ports.ErrLogNotFound
	case err != nil:
		return "", err
	}
	return string(body), nil
}
