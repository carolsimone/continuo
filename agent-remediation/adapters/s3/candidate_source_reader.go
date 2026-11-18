package s3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/pkg/codebundle"
	"github.com/carolsimone/continuo/pkg/objectstore"
)

// CandidateSourceReader implements ports.CandidateSourceReader over an
// object-store reader: it reads one node's source out of a release's code
// bundle. It reuses the bundle contract decoder shared with catalog,
// but exposes only the slice remediation needs.
type CandidateSourceReader struct {
	objects objectstore.Reader
}

var _ ports.CandidateSourceReader = (*CandidateSourceReader)(nil)

// maxBundleBytes caps the bundle document held in memory, mirroring the
// catalog's ceiling for the same object.
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
// release other than releaseID (readBundle), and a missing node. Any other
// fetch error returns with its cause so the trigger redelivers.
func (r *CandidateSourceReader) NodeSource(ctx context.Context, bundleURI, uniqueID, releaseID string) (ports.CandidateSource, error) {
	bundle, err := r.readBundle(ctx, bundleURI, releaseID)
	if err != nil {
		return ports.CandidateSource{}, err
	}
	node, ok := bundle.Nodes[uniqueID]
	if !ok {
		return ports.CandidateSource{}, ports.ErrNotFound
	}
	return ports.CandidateSource{RawCode: node.RawCode, Runtime: node.Runtime}, nil
}

// NodeOwners returns the owner the bundle records for each of nodeIDs
// (ownerOf), from one read of the bundle. A node the bundle does not hold, or
// holds with no owner, is absent from the map. The bundle's own permanent
// misses map to ports.ErrNotFound and any other fetch error returns with its
// cause, as for NodeSource (readBundle).
func (r *CandidateSourceReader) NodeOwners(ctx context.Context, bundleURI, releaseID string, nodeIDs []string) (map[string]string, error) {
	bundle, err := r.readBundle(ctx, bundleURI, releaseID)
	if err != nil {
		return nil, err
	}
	owners := make(map[string]string, len(nodeIDs))
	for _, id := range nodeIDs {
		node, ok := bundle.Nodes[id]
		if !ok {
			continue
		}
		if owner := ownerOf(node); owner != "" {
			owners[id] = owner
		}
	}
	return owners, nil
}

// readBundle reads and decodes the bundle at bundleURI and confirms it belongs
// to releaseID. A URI that names no object (an empty one included), a missing
// object, an object over maxBundleBytes, a malformed document and a bundle for
// another release yield ports.ErrNotFound; any other fetch error is returned
// with its cause.
func (r *CandidateSourceReader) readBundle(ctx context.Context, bundleURI, releaseID string) (codebundle.Bundle, error) {
	ref, err := objectstore.ParseRef(bundleURI)
	if err != nil {
		return codebundle.Bundle{}, fmt.Errorf("%w: %w", ports.ErrNotFound, err)
	}
	body, err := r.objects.Get(ctx, ref, maxBundleBytes)
	switch {
	case errors.Is(err, objectstore.ErrNotFound), errors.Is(err, objectstore.ErrTooLarge):
		return codebundle.Bundle{}, fmt.Errorf("%w: %w", ports.ErrNotFound, err)
	case err != nil:
		return codebundle.Bundle{}, fmt.Errorf("get code bundle %s: %w", bundleURI, err)
	}
	bundle, err := codebundle.Decode(body)
	if err != nil {
		return codebundle.Bundle{}, ports.ErrNotFound
	}
	if bundle.ReleaseID != releaseID {
		// The URI resolved to a bundle for a different release than the trigger's
		// own — a stale or misrouted object naming the same node id. Reading it
		// would stamp another release's source or owner onto this node, the
		// same guard catalog applies to the same bundle document in
		// remediation_requested_rejections_handler.go and
		// release_promoted_versions_handler.go.
		return codebundle.Bundle{}, ports.ErrNotFound
	}
	return bundle, nil
}

// ownerOf is the owner the bundle records for node: the `owner` key of a
// python node's raw_code, its contract entry as JSON, and the `meta.owner` of
// any other node's config. A value that is absent or not a string, and a
// raw_code that is not a JSON object, give no owner.
func ownerOf(node codebundle.Node) string {
	if node.Runtime == ports.RuntimePython {
		var entry struct {
			Owner any `json:"owner"`
		}
		if err := json.Unmarshal([]byte(node.RawCode), &entry); err != nil {
			return ""
		}
		owner, _ := entry.Owner.(string)
		return owner
	}
	meta, _ := node.Config["meta"].(map[string]any)
	owner, _ := meta["owner"].(string)
	return owner
}
