// Package s3 implements state's object-storage port, the topology-artifact
// reader, over pkg/objectstore's Reader role.
package s3

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/ports"
)

// topologyArtifactCacheSize bounds the decoded artifacts kept in memory, so a
// promotion redelivered or redriven soon after its first delivery is served
// without a second read.
const topologyArtifactCacheSize = 8

// TopologyArtifactReader reads release topology artifacts from object storage,
// verifies each against the SHA-256 its promotion carried, and keeps the most
// recently read artifacts in memory. An artifact never changes once written, so
// an entry keyed by URI and checksum stays valid for as long as it is held.
type TopologyArtifactReader struct {
	objects objectstore.Reader

	mu      sync.Mutex
	entries map[artifactKey]*list.Element
	lru     *list.List
	maxSize int
}

type artifactKey struct{ uri, sha256 string }

type artifactEntry struct {
	key      artifactKey
	artifact ports.TopologyArtifact
}

// Compile-time assertion that the adapter satisfies the application port.
var _ ports.TopologyArtifactReader = (*TopologyArtifactReader)(nil)

// NewTopologyArtifactReader builds a TopologyArtifactReader that reads through
// objects; state passes the shared object-store client.
func NewTopologyArtifactReader(objects objectstore.Reader) *TopologyArtifactReader {
	return newTopologyArtifactReader(objects, topologyArtifactCacheSize)
}

func newTopologyArtifactReader(objects objectstore.Reader, maxSize int) *TopologyArtifactReader {
	return &TopologyArtifactReader{
		objects: objects,
		entries: make(map[artifactKey]*list.Element),
		lru:     list.New(),
		maxSize: maxSize,
	}
}

// Load returns the artifact at uri once its bytes hash to sha256Hex. A missing
// object wraps ports.ErrTopologyArtifactNotFound; an oversized, mismatching or
// unreadable object, or a URI that is not an object reference, wraps
// ports.ErrTopologyArtifactCorrupt; any other failure keeps its cause in the
// chain, so an outage stays recognisable.
func (r *TopologyArtifactReader) Load(ctx context.Context, uri, sha256Hex string) (ports.TopologyArtifact, error) {
	key := artifactKey{uri: uri, sha256: sha256Hex}
	if a, ok := r.cached(key); ok {
		return a, nil
	}

	ref, err := objectstore.ParseRef(uri)
	if err != nil {
		// ParseRef fails only on a malformed reference (objectstore.ErrInvalidRef),
		// a producer defect no retry repairs.
		return ports.TopologyArtifact{}, fmt.Errorf("%w: %q: %v", ports.ErrTopologyArtifactCorrupt, uri, err)
	}
	body, err := r.objects.Get(ctx, ref, topologyartifact.MaxObjectBytes)
	switch {
	case errors.Is(err, objectstore.ErrNotFound):
		return ports.TopologyArtifact{}, fmt.Errorf("%w: %s: %v", ports.ErrTopologyArtifactNotFound, uri, err)
	case errors.Is(err, objectstore.ErrTooLarge):
		return ports.TopologyArtifact{}, fmt.Errorf("%w: %s: %v", ports.ErrTopologyArtifactCorrupt, uri, err)
	case err != nil:
		return ports.TopologyArtifact{}, fmt.Errorf("read topology artifact %s: %w", uri, err)
	}

	doc, err := topologyartifact.Decode(body, sha256Hex)
	if err != nil {
		if errors.Is(err, topologyartifact.ErrChecksumMismatch) {
			actual := sha256.Sum256(body)
			return ports.TopologyArtifact{}, fmt.Errorf("%w: %s: checksum mismatch: expected sha256 %s, object has %s",
				ports.ErrTopologyArtifactCorrupt, uri, sha256Hex, hex.EncodeToString(actual[:]))
		}
		return ports.TopologyArtifact{}, fmt.Errorf("%w: %s: %v", ports.ErrTopologyArtifactCorrupt, uri, err)
	}
	a := artifactOf(doc)
	r.put(key, a)
	return a, nil
}

// artifactOf maps a decoded document onto the port's node specs. The document
// states every node's test count, so none is unknown; a timeout of 0 means the
// node declares none.
func artifactOf(doc topologyartifact.Document) ports.TopologyArtifact {
	nodes := make([]topology.NodeSpec, 0, len(doc.Nodes))
	for _, n := range doc.Nodes {
		testCount := n.TestCount
		var timeout *int
		if n.TimeoutSeconds > 0 {
			seconds := n.TimeoutSeconds
			timeout = &seconds
		}
		nodes = append(nodes, topology.NodeSpec{
			UniqueID:          n.UniqueID,
			ServiceName:       n.ServiceName,
			SchemaName:        n.SchemaName,
			TableName:         n.TableName,
			NodeType:          n.NodeType,
			TestCount:         &testCount,
			ContentHash:       n.ContentHash,
			ImageTag:          n.ImageTag,
			SecretRef:         n.SecretRef,
			ScheduleName:      n.Schedule,
			TimeoutSeconds:    timeout,
			UpstreamUniqueIDs: append([]string(nil), n.UpstreamUniqueIDs...),
		})
	}
	return ports.TopologyArtifact{TenantID: doc.TenantID, ReleaseID: doc.ReleaseID, Nodes: nodes}
}

func (r *TopologyArtifactReader) cached(key artifactKey) (ports.TopologyArtifact, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	elem, ok := r.entries[key]
	if !ok {
		return ports.TopologyArtifact{}, false
	}
	r.lru.MoveToFront(elem)
	return elem.Value.(*artifactEntry).artifact, true
}

func (r *TopologyArtifactReader) put(key artifactKey, a ports.TopologyArtifact) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if elem, ok := r.entries[key]; ok {
		r.lru.MoveToFront(elem)
		return
	}
	r.entries[key] = r.lru.PushFront(&artifactEntry{key: key, artifact: a})
	for r.lru.Len() > r.maxSize {
		oldest := r.lru.Back()
		r.lru.Remove(oldest)
		delete(r.entries, oldest.Value.(*artifactEntry).key)
	}
}
