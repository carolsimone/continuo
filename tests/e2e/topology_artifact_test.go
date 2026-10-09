package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
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

// assertCurrentProdByReference checks what a promotion left on current_prod:
// it names releaseID, points at that release's own artifact (the candidate run's
// URI and checksum, no copy), carries the promotion seq the promotion took, and
// the orchestrator's live pointer holds the release under that same seq.
func assertCurrentProdByReference(t *testing.T, ctx context.Context, clients *testClients, releaseID string) {
	t.Helper()
	var cpRelease, cpURI, cpSHA, runURI, runSHA string
	var cpSeq int64
	require.NoError(t, clients.releaseDB.QueryRowContext(ctx,
		`SELECT release_id, topology_uri, topology_sha256, promotion_seq FROM current_prod WHERE id = 1`).
		Scan(&cpRelease, &cpURI, &cpSHA, &cpSeq), "read current_prod")
	require.NoError(t, clients.releaseDB.QueryRowContext(ctx,
		`SELECT candidate_topology_uri, candidate_topology_sha256 FROM release_pipeline_runs WHERE run_id = $1`,
		releaseID).Scan(&runURI, &runSHA), "read the topology reference of release %s", releaseID)

	require.Equal(t, releaseID, cpRelease)
	require.Equal(t, runURI, cpURI, "current_prod must point at the release's own artifact")
	require.Equal(t, runSHA, cpSHA)
	require.Positive(t, cpSeq, "a promotion takes a promotion seq")
	require.Equal(t, livePointer{releaseID: releaseID, promotionSeq: cpSeq}, readLivePointer(ctx, clients),
		"the orchestrator's live pointer must carry the seq the promotion took")
	t.Logf("✅ current_prod → %s at promotion seq %d", cpURI, cpSeq)
}

// currentProdTopology reads the artifact current_prod points at from MinIO and
// decodes it, verifying the recorded checksum.
func currentProdTopology(t *testing.T, ctx context.Context, clients *testClients) topologyartifact.Document {
	t.Helper()
	var uri, sha string
	require.NoError(t, clients.releaseDB.QueryRowContext(ctx,
		`SELECT topology_uri, topology_sha256 FROM current_prod WHERE id = 1`).Scan(&uri, &sha),
		"read current_prod's topology reference")
	prefix := "s3://" + e2eS3Bucket + "/"
	require.True(t, strings.HasPrefix(uri, prefix), "current_prod's artifact %s is not in bucket %s", uri, e2eS3Bucket)
	doc, err := topologyartifact.Decode(getS3Object(t, ctx, clients, strings.TrimPrefix(uri, prefix)), sha)
	require.NoError(t, err, "decode current_prod's artifact %s", uri)
	return doc
}
