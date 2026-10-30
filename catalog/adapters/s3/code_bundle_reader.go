// Package s3 implements catalog's object-storage ports, the code-bundle
// reader and the topology-artifact reader. Each adapter depends on the
// objectstore.Reader role, never on the concrete client, parses the URI it is
// given once with objectstore.ParseRef, and translates the store's errors
// into its port's.
package s3

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/pkg/codebundle"
	"github.com/carolsimone/continuo/pkg/objectstore"
)

// maxBundleBytes caps the code-bundle document catalog will hold in memory.
// It is far above any realistic estate — a bundle is source text for every node
// in the release — and exists so a pathological object cannot exhaust the
// container's memory limit during the read-then-decode step.
const maxBundleBytes int64 = 64 * 1024 * 1024

// CodeBundleReader implements ports.CodeBundleReader over an object-store
// reader.
type CodeBundleReader struct {
	objects objectstore.Reader
}

// Compile-time assertion that the adapter satisfies the application port.
var _ ports.CodeBundleReader = (*CodeBundleReader)(nil)

// NewCodeBundleReader builds a CodeBundleReader over objects. A bundle URI
// without a bucket reads the store's default bucket.
func NewCodeBundleReader(objects objectstore.Reader) *CodeBundleReader {
	return &CodeBundleReader{objects: objects}
}

// Fetch reads and decodes the bundle at uri, mapping the store's outcomes onto
// the port's errors:
//   - a blank uri or a missing object: ports.ErrBundleNotFound (retried);
//   - a uri that names no object (objectstore.ErrInvalidRef) or an
//     undecodable object: ports.ErrBundleMalformed (permanent);
//   - an object over maxBundleBytes: ports.ErrBundleTooLarge (permanent);
//   - any other failure keeps its cause, so an S3 outage reaches the
//     consumer's classifier intact.
func (r *CodeBundleReader) Fetch(ctx context.Context, uri string) (codebundle.Bundle, error) {
	if strings.TrimSpace(uri) == "" {
		return codebundle.Bundle{}, ports.ErrBundleNotFound
	}
	ref, err := objectstore.ParseRef(uri)
	if err != nil {
		return codebundle.Bundle{}, fmt.Errorf("%w: %w", ports.ErrBundleMalformed, err)
	}
	// The read is bounded before decoding allocates a second representation
	// of the document, so an unbounded object cannot exhaust the container's
	// memory limit before the per-node size guard ever runs.
	body, err := r.objects.Get(ctx, ref, maxBundleBytes)
	switch {
	case errors.Is(err, objectstore.ErrNotFound):
		return codebundle.Bundle{}, fmt.Errorf("%w: %s", ports.ErrBundleNotFound, uri)
	case errors.Is(err, objectstore.ErrTooLarge):
		return codebundle.Bundle{}, fmt.Errorf("%w: %w", ports.ErrBundleTooLarge, err)
	case err != nil:
		return codebundle.Bundle{}, fmt.Errorf("get code bundle %s: %w", uri, err)
	}
	bundle, err := codebundle.Decode(body)
	if err != nil {
		return codebundle.Bundle{}, fmt.Errorf("%w: %s: %v", ports.ErrBundleMalformed, uri, err)
	}
	return bundle, nil
}
