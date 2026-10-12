package s3

import (
	"context"
	"net/http"
	"testing"

	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTopologyArtifactStore_WritesWithTheDefaultChain proves the wiring main,
// announce-topology and seed-service-prod use: one *objectstore.Client built
// without static keys, passed to the artifact store as its
// objectstore.ReadWriter role, signs every request with the credentials the
// SDK's default chain resolves.
func TestTopologyArtifactStore_WritesWithTheDefaultChain(t *testing.T) {
	objectstoretest.UseChainCredentials(t)
	srv := objectstoretest.NewServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			objectstoretest.NoSuchKey(w, r) // no artifact yet: Write stores it
			return
		}
		objectstoretest.OK(w, r)
	})
	client, err := objectstore.New(context.Background(), objectstore.Config{
		EndpointURL: srv.URL, Region: "us-east-1", Bucket: bucket,
	})
	require.NoError(t, err)

	ref, err := NewTopologyArtifactStore(client, bucket, 1).Write(context.Background(), "r1", sampleTopology())
	require.NoError(t, err)
	assert.Equal(t, r1Ref.String(), ref.URI)

	reqs := srv.Requests()
	require.Len(t, reqs, 2)
	assert.Equal(t, http.MethodGet, reqs[0].Method)
	assert.Equal(t, http.MethodPut, reqs[1].Method)
	assert.Equal(t, "/continuo/tenants/default/topologies/r1/topology.json.gz", reqs[1].Path)
	assert.Equal(t, "application/gzip", reqs[1].ContentType)
	for _, r := range reqs {
		assert.Contains(t, r.Authorization, "Credential="+objectstoretest.ChainAccessKeyID+"/")
	}
}
