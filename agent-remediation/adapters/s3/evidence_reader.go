// Package s3 implements agent-remediation's object-storage ports, each in an
// adapter of its own over the narrowest objectstore role it uses: the
// evidence reader and the candidate-source reader over objectstore.Reader,
// the artifact writer over objectstore.Writer.
package s3

import (
	"context"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/pkg/objectstore"
)

// maxEvidenceBytes caps an evidence object (candidate SQL, a dbt or runner
// log, a run result, a proposal's content or diff) held in memory, the
// ceiling the code-bundle reader uses.
const maxEvidenceBytes int64 = 64 << 20

// EvidenceReader implements ports.EvidenceReader over an object-store reader.
type EvidenceReader struct {
	objects objectstore.Reader
}

var _ ports.EvidenceReader = (*EvidenceReader)(nil)

// NewEvidenceReader builds an EvidenceReader over objects. A URI without a
// bucket reads the store's default bucket.
func NewEvidenceReader(objects objectstore.Reader) *EvidenceReader {
	return &EvidenceReader{objects: objects}
}

// Fetch returns the object at uri as a string. Evidence that cannot be read
// for good wraps ports.ErrNotFound, and the caller proceeds without it: a uri
// that names no object (an empty one included), a missing object, and an
// object larger than maxEvidenceBytes. Any other error is returned with its
// cause.
func (r *EvidenceReader) Fetch(ctx context.Context, uri string) (string, error) {
	ref, err := objectstore.ParseRef(uri)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ports.ErrNotFound, err)
	}
	body, err := r.objects.Get(ctx, ref, maxEvidenceBytes)
	switch {
	case errors.Is(err, objectstore.ErrNotFound), errors.Is(err, objectstore.ErrTooLarge):
		return "", fmt.Errorf("%w: %w", ports.ErrNotFound, err)
	case err != nil:
		return "", err
	}
	return string(body), nil
}
