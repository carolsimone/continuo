package s3

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
	"github.com/carolsimone/continuo/run-controller/service/ports"
)

// TestTopologyArtifactReader_ReadsWithTheDefaultChain proves the wiring main
// uses: one *objectstore.Client built without static keys, passed to the reader
// as its objectstore.Reader role, signs the request with the credentials the
// SDK's default chain resolves, and the client's not-found outcome reaches the
// port as its not-found sentinel.
func TestTopologyArtifactReader_ReadsWithTheDefaultChain(t *testing.T) {
	objectstoretest.UseChainCredentials(t)
	srv := objectstoretest.NewServer(t, objectstoretest.NoSuchKey)
	client, err := objectstore.New(context.Background(), objectstore.Config{
		EndpointURL: srv.URL, Region: "us-east-1", Bucket: defaultBucket,
	})
	require.NoError(t, err)

	_, err = NewTopologyArtifactReader(client).Load(context.Background(), "tenants/default/topologies/rel-1/topology.json.gz", "ab")
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactNotFound)

	reqs := srv.Requests()
	require.Len(t, reqs, 1)
	assert.Equal(t, "/continuo/tenants/default/topologies/rel-1/topology.json.gz", reqs[0].Path,
		"a bare key reads the client's default bucket")
	assert.Contains(t, reqs[0].Authorization, "Credential="+objectstoretest.ChainAccessKeyID+"/")
}
