package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/ports"
)

// TopologyArtifactStore implements ports.TopologyArtifactStore over an object
// store, with an LRU of decoded topologies in front of it.
type TopologyArtifactStore struct {
	objects ObjectStore
	bucket  string
	cache   *topologyCache
}

var _ ports.TopologyArtifactStore = (*TopologyArtifactStore)(nil)

// NewTopologyArtifactStore reads and writes artifacts in bucket through
// objects, caching up to cacheSize decoded topologies.
func NewTopologyArtifactStore(objects ObjectStore, bucket string, cacheSize int) *TopologyArtifactStore {
	return &TopologyArtifactStore{objects: objects, bucket: bucket, cache: newTopologyCache(cacheSize)}
}

// Load returns the topology ref names, verifying the object against
// ref.SHA256. A cached topology is returned only for the same checksum.
func (s *TopologyArtifactStore) Load(ctx context.Context, ref release.TopologyRef) (release.Topology, error) {
	if topo, ok := s.cache.get(ref); ok {
		return topo, nil
	}
	key, err := s.keyOf(ref.URI)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ports.ErrTopologyArtifactCorrupt, err)
	}
	body, err := s.objects.GetObject(ctx, key, topologyartifact.MaxObjectBytes)
	switch {
	case errors.Is(err, ErrObjectNotFound):
		return nil, fmt.Errorf("%w: %s", ports.ErrTopologyArtifactNotFound, ref.URI)
	case errors.Is(err, ErrObjectTooLarge):
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
	key := topologyartifact.Key(pkgevents.DefaultTenantID, releaseID)
	switch existing, err := s.objects.GetObject(ctx, key, topologyartifact.MaxObjectBytes); {
	case err == nil:
		existingSum := sha256.Sum256(existing)
		if hex.EncodeToString(existingSum[:]) != sum {
			return release.TopologyRef{}, fmt.Errorf("%w: %s", ports.ErrTopologyArtifactImmutable, key)
		}
		// Identical bytes already stored: nothing to re-put.
	case errors.Is(err, ErrObjectNotFound):
		if err := s.objects.PutObject(ctx, key, gz, "application/gzip"); err != nil {
			return release.TopologyRef{}, fmt.Errorf("write topology artifact %s: %w", key, err)
		}
	default:
		return release.TopologyRef{}, fmt.Errorf("read existing topology artifact %s: %w", key, err)
	}
	ref := release.TopologyRef{URI: "s3://" + s.bucket + "/" + key, SHA256: sum, NodeCount: len(topo)}
	doc, err := topologyartifact.Decode(gz, sum)
	if err != nil {
		return release.TopologyRef{}, fmt.Errorf("decode the topology artifact just written for %s: %w", releaseID, err)
	}
	s.cache.put(ref, topologyFromDocument(doc))
	return ref, nil
}

// keyOf returns the object key of uri, which must name this store's bucket.
func (s *TopologyArtifactStore) keyOf(uri string) (string, error) {
	rest, ok := strings.CutPrefix(uri, "s3://")
	if !ok {
		return "", fmt.Errorf("topology artifact URI %q is not an s3:// URI", uri)
	}
	bucket, key, ok := strings.Cut(rest, "/")
	if !ok || key == "" {
		return "", fmt.Errorf("topology artifact URI %q names no key", uri)
	}
	if bucket != s.bucket {
		return "", fmt.Errorf("topology artifact URI %q is outside bucket %q", uri, s.bucket)
	}
	return key, nil
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
		}
	}
	return topo
}
