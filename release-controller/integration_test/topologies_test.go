//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/stretchr/testify/require"
)

// memTopologies is an in-memory ports.TopologyArtifactStore for the
// integration tests, which run against Postgres but no object store.
type memTopologies struct {
	mu    sync.Mutex
	byURI map[string]memArtifact
}

type memArtifact struct {
	sha256 string
	topo   release.Topology
}

func newMemTopologies() *memTopologies { return &memTopologies{byURI: map[string]memArtifact{}} }

func (m *memTopologies) Write(_ context.Context, releaseID string, topo release.Topology) (release.TopologyRef, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, err := json.Marshal(topo)
	if err != nil {
		return release.TopologyRef{}, err
	}
	sum := sha256.Sum256(raw)
	ref := release.TopologyRef{
		URI:       "s3://test-bucket/tenants/default/topologies/" + releaseID + "/topology.json.gz",
		SHA256:    hex.EncodeToString(sum[:]),
		NodeCount: len(topo),
	}
	m.byURI[ref.URI] = memArtifact{sha256: ref.SHA256, topo: slices.Clone(topo)}
	return ref, nil
}

func (m *memTopologies) Load(_ context.Context, ref release.TopologyRef) (release.Topology, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.byURI[ref.URI]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ports.ErrTopologyArtifactNotFound, ref.URI)
	}
	if a.sha256 != ref.SHA256 {
		return nil, fmt.Errorf("%w: %s", ports.ErrTopologyArtifactCorrupt, ref.URI)
	}
	return slices.Clone(a.topo), nil
}

var _ ports.TopologyArtifactStore = (*memTopologies)(nil)

// putTopology writes topo as releaseID's artifact and returns its reference.
func putTopology(t *testing.T, d *handlers.Deps, releaseID string, topo release.Topology) release.TopologyRef {
	t.Helper()
	ref, err := d.Topologies.Write(context.Background(), releaseID, topo)
	require.NoError(t, err)
	return ref
}
