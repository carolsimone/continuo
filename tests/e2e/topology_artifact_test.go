package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/stretchr/testify/require"
)

// assertCandidateTopologyArtifact checks the topology artifact topology-controller
// wrote for a release: release_pipeline_runs references it by URI, checksum and
// node count; the object at the deterministic key in MinIO hashes to the recorded
// checksum and decodes to the release's own document; and nodeID carries
// imageTag, the tag the release was submitted with, which topology-controller
// joins onto every node from release.requested.
func assertCandidateTopologyArtifact(t *testing.T, ctx context.Context, clients *testClients, releaseID, nodeID, imageTag string) {
	t.Helper()
	var uri, sha string
	var count int
	require.NoError(t, clients.releaseDB.QueryRowContext(ctx,
		`SELECT candidate_topology_uri, candidate_topology_sha256, candidate_node_count
		   FROM release_pipeline_runs WHERE run_id = $1`, releaseID).Scan(&uri, &sha, &count),
		"read the topology reference of release %s", releaseID)

	key := topologyartifact.Key(events.DefaultTenantID, releaseID)
	require.Equal(t, "s3://"+e2eS3Bucket+"/"+key, uri, "the run must reference the artifact at its deterministic key")

	gz := getS3Object(t, ctx, clients, key)
	sum := sha256.Sum256(gz)
	require.Equal(t, hex.EncodeToString(sum[:]), sha, "the recorded checksum must be the SHA-256 of the stored object")

	doc, err := topologyartifact.Decode(gz, sha)
	require.NoError(t, err, "the artifact of %s must decode", releaseID)
	require.Equal(t, releaseID, doc.ReleaseID)
	require.Equal(t, events.DefaultTenantID, doc.TenantID)
	require.Len(t, doc.Nodes, count, "candidate_node_count must count the artifact's nodes")

	found := false
	for _, n := range doc.Nodes {
		if n.UniqueID != nodeID {
			continue
		}
		found = true
		require.Equal(t, imageTag, n.ImageTag, "topology-controller must join the release's image tag onto %s", nodeID)
	}
	require.True(t, found, "the artifact of %s lacks node %s", releaseID, nodeID)
	t.Logf("✅ topology artifact s3://%s/%s (%d nodes, sha256 %s)", e2eS3Bucket, key, count, sha)
}
