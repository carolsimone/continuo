package s3

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
)

// TestAdapters_SignWithTheDefaultChain proves the wiring main uses: one
// *objectstore.Client built without static keys, passed to each adapter as
// the role it uses (the candidate-source and evidence readers as
// objectstore.Reader, the artifact writer as objectstore.Writer), signs every
// request with the credentials the SDK's default chain resolves instead of
// failing on empty static keys.
func TestAdapters_SignWithTheDefaultChain(t *testing.T) {
	objectstoretest.UseChainCredentials(t)
	srv := objectstoretest.NewServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			objectstoretest.OK(w, r)
		case strings.HasSuffix(r.URL.Path, "/bundle.json"):
			objectstoretest.Object([]byte(validBundleJSON))(w, r)
		default:
			objectstoretest.Object([]byte("select 1"))(w, r)
		}
	})
	client, err := objectstore.New(context.Background(), objectstore.Config{
		EndpointURL: srv.URL, Region: "us-east-1", Bucket: testBucket,
	})
	require.NoError(t, err)
	ctx := context.Background()

	src, err := NewCandidateSourceReader(client).NodeSource(ctx, "code-bundles/rel-1/bundle.json", "analytics.orders", "rel-1")
	require.NoError(t, err)
	assert.Equal(t, "select 1 as x", src.RawCode)
	evidence, err := NewEvidenceReader(client).Fetch(ctx, "candidate-sql/rel-1/candidate_model.orders.sql")
	require.NoError(t, err)
	assert.Equal(t, "select 1", evidence)
	uri, err := NewArtifactWriter(client, testBucket).Write(ctx, "proposals/p1/diff.patch", "--- a\n", "text/x-diff")
	require.NoError(t, err)
	assert.Equal(t, "s3://test-bucket/proposals/p1/diff.patch", uri)

	reqs := srv.Requests()
	require.Len(t, reqs, 3)
	assert.Equal(t, "/test-bucket/code-bundles/rel-1/bundle.json", reqs[0].Path, "a bare key reads the client's default bucket")
	assert.Equal(t, "/test-bucket/candidate-sql/rel-1/candidate_model.orders.sql", reqs[1].Path)
	assert.Equal(t, http.MethodPut, reqs[2].Method)
	assert.Equal(t, "/test-bucket/proposals/p1/diff.patch", reqs[2].Path)
	for _, r := range reqs {
		assert.Contains(t, r.Authorization, "Credential="+objectstoretest.ChainAccessKeyID+"/")
	}
}
