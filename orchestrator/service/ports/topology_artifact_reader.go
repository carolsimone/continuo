package ports

import (
	"context"
	"errors"

	"github.com/carolsimone/continuo/pkg/topologyartifact"
)

// ErrTopologyArtifactNotFound means the topology artifact a promotion names is
// absent from object storage. The caller retries: an object that is merely late
// becomes readable, and one that never appears exhausts the delivery budget and
// is dead-lettered.
var ErrTopologyArtifactNotFound = errors.New("topology artifact not found")

// ErrTopologyArtifactCorrupt means the object is not the artifact the promotion
// announced: its SHA-256 differs from the event's, it is not a readable topology
// document, or it exceeds the size a reader accepts. Re-reading the same object
// cannot help, so the caller fails the message permanently.
var ErrTopologyArtifactCorrupt = errors.New("topology artifact corrupt")

// TopologyArtifactReader reads a release's topology artifact and verifies it
// against the SHA-256 its promotion carried. The returned document is shared:
// callers read it and never modify it.
type TopologyArtifactReader interface {
	Load(ctx context.Context, uri, sha256 string) (topologyartifact.Document, error)
}
