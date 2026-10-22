// Package s3 implements release-controller's object-storage ports, each over
// the narrowest pkg/objectstore role it uses: the topology artifact store over
// objectstore.ReadWriter and the prune-time candidate-SQL deleter over
// objectstore.PrefixDeleter.
package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/ports"
)

// TopologyArtifactStore implements ports.TopologyArtifactStore over an object
// store, with an LRU of decoded topologies in front of it.
type TopologyArtifactStore struct {
	objects objectstore.ReadWriter
	bucket  string
	cache   *topologyCache
}

var _ ports.TopologyArtifactStore = (*TopologyArtifactStore)(nil)

// NewTopologyArtifactStore reads and writes artifacts in bucket through
// objects, caching up to cacheSize decoded topologies. bucket is named in
// every reference Write returns and is the only bucket Load reads.
func NewTopologyArtifactStore(objects objectstore.ReadWriter, bucket string, cacheSize int) *TopologyArtifactStore {
	return &TopologyArtifactStore{objects: objects, bucket: bucket, cache: newTopologyCache(cacheSize)}
}

// Load returns the topology ref names, verifying the object against
// ref.SHA256. A cached topology is returned only for the same checksum. A URI
// that does not name an object in this store's bucket, an object over
// topologyartifact.MaxObjectBytes, and an object that does not decode or
// match the checksum are ports.ErrTopologyArtifactCorrupt; a missing object is
// ports.ErrTopologyArtifactNotFound; any other error keeps its cause.
func (s *TopologyArtifactStore) Load(ctx context.Context, ref release.TopologyRef) (release.Topology, error) {
	if topo, ok := s.cache.get(ref); ok {
		return topo, nil
	}
	obj, err := s.objectOf(ref.URI)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ports.ErrTopologyArtifactCorrupt, err)
	}
	body, err := s.objects.Get(ctx, obj, topologyartifact.MaxObjectBytes)
	switch {
	case errors.Is(err, objectstore.ErrNotFound):
		return nil, fmt.Errorf("%w: %s", ports.ErrTopologyArtifactNotFound, ref.URI)
	case errors.Is(err, objectstore.ErrTooLarge):
		return nil, fmt.Errorf("%w: %s: %w", ports.ErrTopologyArtifactCorrupt, ref.URI, err)
	case err != nil:
		return nil, fmt.Errorf("read topology artifact %s: %w", ref.URI, err)
	}
	doc, err := topologyartifact.Decode(body, ref.SHA256)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ports.ErrTopologyArtifactCorrupt, ref.URI, err)
	}
	topo := topologyFromDocument(doc)
	s.cache.put(ref, topo)
	return topo, nil
}

// Write stores topo as releaseID's artifact under the default tenant and
// returns its reference. The object is immutable: when one already exists at
// the key, a re-write of the identical bytes is left as it is (so a crash-retry
// stays idempotent) and a write of different bytes is refused with
// ports.ErrTopologyArtifactImmutable, rather than invalidating the checksum an
// earlier announcement still names. The cache is primed with the topology
// decoded from the written bytes, so a later Load returns exactly what a reader
// of the object would.
func (s *TopologyArtifactStore) Write(ctx context.Context, releaseID string, topo release.Topology) (release.TopologyRef, error) {
	gz, sum, err := topologyartifact.Encode(documentFromTopology(releaseID, topo))
	if err != nil {
		return release.TopologyRef{}, fmt.Errorf("encode topology artifact %s: %w", releaseID, err)
	}
	obj := objectstore.Ref{Bucket: s.bucket, Key: topologyartifact.Key(pkgevents.DefaultTenantID, releaseID)}
	switch existing, err := s.objects.Get(ctx, obj, topologyartifact.MaxObjectBytes); {
	case err == nil:
		existingSum := sha256.Sum256(existing)
		if hex.EncodeToString(existingSum[:]) != sum {
			return release.TopologyRef{}, fmt.Errorf("%w: %s", ports.ErrTopologyArtifactImmutable, obj)
		}
		// Identical bytes already stored: nothing to re-put.
	case errors.Is(err, objectstore.ErrNotFound):
		if err := s.objects.Put(ctx, obj, gz, "application/gzip"); err != nil {
			return release.TopologyRef{}, fmt.Errorf("write topology artifact %s: %w", obj, err)
		}
	default:
		return release.TopologyRef{}, fmt.Errorf("read existing topology artifact %s: %w", obj, err)
	}
	ref := release.TopologyRef{URI: obj.String(), SHA256: sum, NodeCount: len(topo)}
	doc, err := topologyartifact.Decode(gz, sum)
	if err != nil {
		return release.TopologyRef{}, fmt.Errorf("decode the topology artifact just written for %s: %w", releaseID, err)
	}
	s.cache.put(ref, topologyFromDocument(doc))
	return ref, nil
}

// objectOf parses uri once and requires it to name an object in this store's
// bucket. A bare key names no bucket and is refused too.
func (s *TopologyArtifactStore) objectOf(uri string) (objectstore.Ref, error) {
	obj, err := objectstore.ParseRef(uri)
	if err != nil {
		return objectstore.Ref{}, err
	}
	if obj.Bucket != s.bucket {
		return objectstore.Ref{}, fmt.Errorf("topology artifact URI %q is not in bucket %q", uri, s.bucket)
	}
	return obj, nil
}

func documentFromTopology(releaseID string, topo release.Topology) topologyartifact.Document {
	nodes := make([]topologyartifact.Node, len(topo))
	for i, n := range topo {
		nodes[i] = topologyartifact.Node{
			UniqueID:           n.UniqueID,
			SchemaName:         n.SchemaName,
			TableName:          n.TableName,
			ResolvedRelationID: n.ResolvedRelationID,
			ServiceName:        n.ServiceName,
			NodeType:           n.NodeType,
			TestCount:          n.TestCount,
			ContentHash:        n.ContentHash,
			ImageTag:           n.ImageTag,
			OriginalFilePath:   n.OriginalFilePath,
			UpstreamUniqueIDs:  n.UpstreamUniqueIDs,
			Schedule:           n.Schedule,
			SecretRef:          n.SecretRef,
			TimeoutSeconds:     n.TimeoutSeconds,
		}
	}
	return topologyartifact.Document{
		SchemaVersion: topologyartifact.SchemaVersion,
		TenantID:      pkgevents.DefaultTenantID,
		ReleaseID:     releaseID,
		Nodes:         nodes,
	}
}

func topologyFromDocument(doc topologyartifact.Document) release.Topology {
	topo := make(release.Topology, len(doc.Nodes))
	for i, n := range doc.Nodes {
		topo[i] = release.Node{
			UniqueID:           n.UniqueID,
			SchemaName:         n.SchemaName,
			TableName:          n.TableName,
			ResolvedRelationID: n.ResolvedRelationID,
			ServiceName:        n.ServiceName,
			NodeType:           n.NodeType,
			ContentHash:        n.ContentHash,
			TestCount:          n.TestCount,
			ImageTag:           n.ImageTag,
			UpstreamUniqueIDs:  n.UpstreamUniqueIDs,
			Schedule:           n.Schedule,
			OriginalFilePath:   n.OriginalFilePath,
			SecretRef:          n.SecretRef,
			TimeoutSeconds:     n.TimeoutSeconds,
		}
	}
	return topo
}
