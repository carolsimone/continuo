package ports

import (
	"context"
	"errors"

	"github.com/carolsimone/continuo/state/domain/topology"
)

// ErrTopologyArtifactNotFound means the topology artifact a promotion names is
// absent from object storage. The caller retries: an object that is merely late
// becomes readable, and one that never appears exhausts the delivery budget and
// is dead-lettered.
var ErrTopologyArtifactNotFound = errors.New("topology artifact not found")

// ErrTopologyArtifactCorrupt means the object is not the artifact the promotion
// announced: its SHA-256 differs from the event's, it is not a readable
// topology document, it exceeds the size a reader accepts, or the URI is not
// an object reference. Re-reading cannot help, so the caller fails the message
// permanently.
var ErrTopologyArtifactCorrupt = errors.New("topology artifact corrupt")

// TopologyArtifact is a release's topology as its artifact holds it: the tenant
// and release the artifact names and every node, dbt tests included.
type TopologyArtifact struct {
	TenantID  string
	ReleaseID string
	Nodes     []topology.NodeSpec
}

// TopologyArtifactReader reads a release's topology artifact and verifies it
// against the SHA-256 its promotion carried. The returned nodes are shared:
// callers read them and never modify them.
type TopologyArtifactReader interface {
	Load(ctx context.Context, uri, sha256 string) (TopologyArtifact, error)
}
