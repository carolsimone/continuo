package ports

import (
	"context"
	"errors"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// ErrTopologyArtifactNotFound reports a reference whose object does not exist.
var ErrTopologyArtifactNotFound = errors.New("topology artifact not found")

// ErrTopologyArtifactCorrupt reports an object that exists but is not the
// artifact the reference names: its bytes do not match the reference's
// SHA-256, it cannot be decoded, it exceeds the size a reader accepts, or the
// URI points outside this install's bucket. Reading it again cannot succeed.
var ErrTopologyArtifactCorrupt = errors.New("topology artifact corrupt")

// ErrTopologyArtifactImmutable reports a Write of a release id whose object
// already exists with different bytes. The object is immutable — an earlier
// announcement still names its checksum — so a changed-bytes overwrite is
// refused rather than silently invalidating that checksum. A re-write of the
// identical bytes is idempotent and allowed.
var ErrTopologyArtifactImmutable = errors.New("topology artifact already exists with different bytes")

// TopologyArtifactStore reads and writes topology artifacts: the immutable,
// checksummed object holding one run's whole candidate topology.
type TopologyArtifactStore interface {
	// Load returns the topology ref names. ErrTopologyArtifactNotFound and
	// ErrTopologyArtifactCorrupt are the only errors that describe the object;
	// any other error means the object store could not be reached and the call
	// may succeed later.
	Load(ctx context.Context, ref release.TopologyRef) (release.Topology, error)
	// Write stores topo as releaseID's artifact and returns its reference.
	Write(ctx context.Context, releaseID string, topo release.Topology) (release.TopologyRef, error)
}
