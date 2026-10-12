package s3

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
)

// The writer writes into its own bucket, named explicitly, so the URI it
// returns always names the object it wrote, whatever the store's default.
func TestArtifactWriter_WritesIntoItsBucketAndReturnsTheURI(t *testing.T) {
	mem := objectstoretest.NewMemory("default-bucket")

	uri, err := NewArtifactWriter(mem, testBucket).Write(context.Background(), "proposals/p1/diff.patch", "--- a\n+++ b\n", "text/x-diff")
	require.NoError(t, err)
	assert.Equal(t, "s3://test-bucket/proposals/p1/diff.patch", uri)
	assert.Equal(t, []objectstoretest.Write{{
		Ref:         objectstore.Ref{Bucket: testBucket, Key: "proposals/p1/diff.patch"},
		Body:        []byte("--- a\n+++ b\n"),
		ContentType: "text/x-diff",
	}}, mem.Puts())
}

func TestArtifactWriter_ReturnsARefusedWrite(t *testing.T) {
	mem := objectstoretest.NewMemory(testBucket)
	refused := statusError{code: http.StatusForbidden}
	mem.FailPut(objectstore.Ref{Bucket: testBucket, Key: "proposals/p1/diff.patch"}, refused)

	uri, err := NewArtifactWriter(mem, testBucket).Write(context.Background(), "proposals/p1/diff.patch", "diff", "text/x-diff")
	require.ErrorIs(t, err, refused)
	assert.Empty(t, uri)
}

// The proposal service builds every key; an empty one is refused by the store
// before any write.
func TestArtifactWriter_RefusesAKeyThatNamesNoObjectWithoutAWrite(t *testing.T) {
	mem := objectstoretest.NewMemory(testBucket)

	_, err := NewArtifactWriter(mem, testBucket).Write(context.Background(), "", "body", "text/plain")
	require.ErrorIs(t, err, objectstore.ErrInvalidRef)
	assert.Empty(t, mem.Puts())
}
