package s3

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/carolsimone/continuo/orchestrator/service/ports"
	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
)

// topologyArtifactCacheSize bounds the decoded topology documents kept in
// memory. The topology group and the versions group read the same promotion,
// so a small window serves both reads of every promotion from one fetch.
const topologyArtifactCacheSize = 8

// TopologyArtifactReader reads release topology artifacts through an
// object-store reader, verifies each against the SHA-256 its promotion
// carried, and keeps the most recently read documents in memory. An artifact
// never changes once written, so an entry keyed by URI and checksum stays
// valid for as long as it is held.
type TopologyArtifactReader struct {
	objects objectstore.Reader

	mu      sync.Mutex
	entries map[artifactKey]*list.Element
	lru     *list.List
	maxSize int
}

type artifactKey struct{ uri, sha256 string }

type artifactEntry struct {
	key artifactKey
	doc topologyartifact.Document
}

// Compile-time assertion that the adapter satisfies the application port.
var _ ports.TopologyArtifactReader = (*TopologyArtifactReader)(nil)

// NewTopologyArtifactReader builds a TopologyArtifactReader over objects. An
// artifact URI without a bucket reads the store's default bucket.
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

// Load returns the topology document at uri once its bytes hash to sha256Hex.
// The store's outcomes map onto the port's errors: a missing object is
// ports.ErrTopologyArtifactNotFound (retried); a uri that names no object, an
// object over topologyartifact.MaxObjectBytes, a checksum mismatch and an
// undecodable document are ports.ErrTopologyArtifactCorrupt (permanent); any
// other error keeps its cause and wraps neither, so the consumer's classifier
// recognises an outage.
func (r *TopologyArtifactReader) Load(ctx context.Context, uri, sha256Hex string) (topologyartifact.Document, error) {
	key := artifactKey{uri: uri, sha256: sha256Hex}
	if doc, ok := r.cached(key); ok {
		return doc, nil
	}

	ref, err := objectstore.ParseRef(uri)
	if err != nil {
		return topologyartifact.Document{}, fmt.Errorf("%w: %w", ports.ErrTopologyArtifactCorrupt, err)
	}
	body, err := r.objects.Get(ctx, ref, topologyartifact.MaxObjectBytes)
	switch {
	case errors.Is(err, objectstore.ErrNotFound):
		return topologyartifact.Document{}, fmt.Errorf("%w: %w", ports.ErrTopologyArtifactNotFound, err)
	case errors.Is(err, objectstore.ErrTooLarge):
		return topologyartifact.Document{}, fmt.Errorf("%w: %w", ports.ErrTopologyArtifactCorrupt, err)
	case err != nil:
		return topologyartifact.Document{}, fmt.Errorf("get topology artifact %s: %w", uri, err)
	}

	doc, err := topologyartifact.Decode(body, sha256Hex)
	if err != nil {
		if errors.Is(err, topologyartifact.ErrChecksumMismatch) {
			actual := sha256.Sum256(body)
			return topologyartifact.Document{}, fmt.Errorf("%w: %s: expected sha256 %s, object has %s",
				ports.ErrTopologyArtifactCorrupt, uri, sha256Hex, hex.EncodeToString(actual[:]))
		}
		return topologyartifact.Document{}, fmt.Errorf("%w: %s: %v", ports.ErrTopologyArtifactCorrupt, uri, err)
	}
	r.put(key, doc)
	return doc, nil
}

func (r *TopologyArtifactReader) cached(key artifactKey) (topologyartifact.Document, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	elem, ok := r.entries[key]
	if !ok {
		return topologyartifact.Document{}, false
	}
	r.lru.MoveToFront(elem)
	return elem.Value.(*artifactEntry).doc, true
}

func (r *TopologyArtifactReader) put(key artifactKey, doc topologyartifact.Document) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if elem, ok := r.entries[key]; ok {
		r.lru.MoveToFront(elem)
		return
	}
	r.entries[key] = r.lru.PushFront(&artifactEntry{key: key, doc: doc})
	for r.lru.Len() > r.maxSize {
		oldest := r.lru.Back()
		r.lru.Remove(oldest)
		delete(r.entries, oldest.Value.(*artifactEntry).key)
	}
}
