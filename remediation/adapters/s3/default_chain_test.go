package s3

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
)

// TestLogReader_ReadsWithTheDefaultChain proves the wiring main uses: an
// *objectstore.Client built without static keys, passed to the reader as its
// objectstore.Reader role, signs the read with the credentials the SDK's
// default chain resolves instead of failing on empty static keys.
func TestLogReader_ReadsWithTheDefaultChain(t *testing.T) {
	objectstoretest.UseChainCredentials(t)
	srv := objectstoretest.NewServer(t, objectstoretest.Object([]byte("Database Error in model orders")))
	client, err := objectstore.New(context.Background(), objectstore.Config{
		EndpointURL: srv.URL, Region: "us-east-1", Bucket: defaultBucket,
	})
	require.NoError(t, err)

	got, err := NewLogReader(client).Fetch(context.Background(), "logs/a/b.log")
	require.NoError(t, err)
	assert.Equal(t, "Database Error in model orders", got)

	reqs := srv.Requests()
	require.Len(t, reqs, 1)
	assert.Equal(t, "/continuo/logs/a/b.log", reqs[0].Path)
	assert.Contains(t, reqs[0].Authorization, "Credential="+objectstoretest.ChainAccessKeyID+"/")
}
