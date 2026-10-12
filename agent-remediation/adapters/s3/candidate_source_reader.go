package s3

import (
	"context"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/pkg/codebundle"
	"github.com/carolsimone/continuo/pkg/objectstore"
)

// CandidateSourceReader implements ports.CandidateSourceReader over an
// object-store reader: it reads one node's source out of a release's code
// bundle. It reuses the bundle contract decoder shared with the orchestrator,
// but exposes only the slice remediation needs.
type CandidateSourceReader struct {
	objects objectstore.Reader
}

var _ ports.CandidateSourceReader = (*CandidateSourceReader)(nil)

// maxBundleBytes caps the bundle document held in memory, mirroring the
// orchestrator's ceiling for the same object.
const maxBundleBytes int64 = 64 * 1024 * 1024

// NewCandidateSourceReader builds a CandidateSourceReader over objects. A
// bundle URI without a bucket reads the store's default bucket.
func NewCandidateSourceReader(objects objectstore.Reader) *CandidateSourceReader {
	return &CandidateSourceReader{objects: objects}
}

// NodeSource returns the node's bundle entry. Every permanently unreadable
// condition maps to ports.ErrNotFound so the caller takes its repo-read
// fallback: a URI that names no object (an empty one included), a missing
// object, an object over maxBundleBytes, a malformed document, a bundle for a
// release other than releaseID, and a missing node. Any other fetch error
// returns with its cause so the trigger redelivers.
func (r *CandidateSourceReader) NodeSource(ctx context.Context, bundleURI, uniqueID, releaseID string) (ports.CandidateSource, error) {
	ref, err := objectstore.ParseRef(bundleURI)
	if err != nil {
		return ports.CandidateSource{}, fmt.Errorf("%w: %w", ports.ErrNotFound, err)
	}
	body, err := r.objects.Get(ctx, ref, maxBundleBytes)
	switch {
	case errors.Is(err, objectstore.ErrNotFound), errors.Is(err, objectstore.ErrTooLarge):
		return ports.CandidateSource{}, fmt.Errorf("%w: %w", ports.ErrNotFound, err)
	case err != nil:
		return ports.CandidateSource{}, fmt.Errorf("get code bundle %s: %w", bundleURI, err)
	}
	bundle, err := codebundle.Decode(body)
	if err != nil {
		return ports.CandidateSource{}, ports.ErrNotFound
	}
	if bundle.ReleaseID != releaseID {
		// The URI resolved to a bundle for a different release than the trigger's
		// own — a stale or misrouted object naming the same node id. Returning its
		// RawCode would stamp another release's source onto this node's fix, the
		// same guard the orchestrator applies to the same bundle document in
		// remediation_requested_rejections_handler.go and
		// release_promoted_versions_handler.go.
		return ports.CandidateSource{}, ports.ErrNotFound
	}
	node, ok := bundle.Nodes[uniqueID]
	if !ok {
		return ports.CandidateSource{}, ports.ErrNotFound
	}
	return ports.CandidateSource{RawCode: node.RawCode, Runtime: node.Runtime}, nil
}
