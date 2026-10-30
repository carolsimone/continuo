package s3

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
)

// TestReaders_ReadWithTheDefaultChain proves the wiring main uses: one
// *objectstore.Client built without static keys, passed to both readers as
// their objectstore.Reader role, signs every request with the credentials the
// SDK's default chain resolves, and the client's not-found outcome reaches
// each port as its not-found sentinel.
func TestReaders_ReadWithTheDefaultChain(t *testing.T) {
	objectstoretest.UseChainCredentials(t)
	srv := objectstoretest.NewServer(t, objectstoretest.NoSuchKey)
	client, err := objectstore.New(context.Background(), objectstore.Config{
		EndpointURL: srv.URL, Region: "us-east-1", Bucket: defaultBucket,
	})
	require.NoError(t, err)
	ctx := context.Background()

	_, err = NewCodeBundleReader(client).Fetch(ctx, "s3://continuo/code-bundles/rel-1/bundle.json")
	assert.ErrorIs(t, err, ports.ErrBundleNotFound)
	_, err = NewTopologyArtifactReader(client).Load(ctx, "tenants/default/topologies/rel-1/topology.json.gz", "ab")
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactNotFound)

	reqs := srv.Requests()
	require.Len(t, reqs, 2)
	assert.Equal(t, "/continuo/code-bundles/rel-1/bundle.json", reqs[0].Path)
	assert.Equal(t, "/continuo/tenants/default/topologies/rel-1/topology.json.gz", reqs[1].Path,
		"a bare key reads the client's default bucket")
	for _, r := range reqs {
		assert.Contains(t, r.Authorization, "Credential="+objectstoretest.ChainAccessKeyID+"/")
	}
}
