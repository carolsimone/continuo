package objectstore_test

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
)

// The role contract: every assertion below holds for the production Client
// and for objectstoretest.Memory alike, which is what lets an adapter's unit
// tests substitute the Memory for the Client.

// store is every role at once.
type store interface {
	objectstore.Reader
	objectstore.Writer
	objectstore.PrefixDeleter
}

// subject is one implementation under contract, and a count of the calls
// that reached its backing store: requests for the Client, reads and stored
// writes for the Memory.
type subject struct {
	store store
	calls func() int
}

// subjects returns a fresh Client, backed by an emulated S3, and a fresh
// Memory, both with "continuo" as the default bucket.
func subjects(t *testing.T) map[string]subject {
	t.Helper()
	srv := objectstoretest.NewServer(t, newFakeS3(0).handle)
	mem := objectstoretest.NewMemory("continuo")
	return map[string]subject{
		"Client": {store: newTestClient(t, srv.URL), calls: func() int { return len(srv.Requests()) }},
		"Memory": {store: mem, calls: func() int { return len(mem.Reads()) + len(mem.Puts()) }},
	}
}

func TestRoles_PutThenGetReturnsACopyOfTheBytes(t *testing.T) {
	for name, s := range subjects(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			ref := objectstore.Ref{Key: "a/b.json"}
			require.NoError(t, s.store.Put(ctx, ref, []byte("body"), "application/json"))

			got, err := s.store.Get(ctx, ref, 1024)
			require.NoError(t, err)
			assert.Equal(t, []byte("body"), got)
			got[0] = 'X'
			again, err := s.store.Get(ctx, ref, 1024)
			require.NoError(t, err)
			assert.Equal(t, []byte("body"), again, "editing what Get returned does not change the stored object")
		})
	}
}

func TestRoles_MissingObjectIsNotFound(t *testing.T) {
	for name, s := range subjects(t) {
		t.Run(name, func(t *testing.T) {
			_, err := s.store.Get(context.Background(), objectstore.Ref{Key: "absent"}, 1024)
			require.ErrorIs(t, err, objectstore.ErrNotFound)
			assert.NotErrorIs(t, err, objectstore.ErrTooLarge)
			assert.Contains(t, err.Error(), "s3://continuo/absent", "the error names the resolved object")
		})
	}
}

func TestRoles_ObjectOverTheLimitIsTooLargeAndOneAtTheLimitIsRead(t *testing.T) {
	for name, s := range subjects(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			ref := objectstore.Ref{Key: "k"}
			require.NoError(t, s.store.Put(ctx, ref, []byte("0123456789"), "text/plain"))

			_, err := s.store.Get(ctx, ref, 4)
			require.ErrorIs(t, err, objectstore.ErrTooLarge)
			assert.NotErrorIs(t, err, objectstore.ErrNotFound)
			got, err := s.store.Get(ctx, ref, 10)
			require.NoError(t, err)
			assert.Equal(t, []byte("0123456789"), got)
		})
	}
}

func TestRoles_AnEmptyBucketIsTheDefaultBucket(t *testing.T) {
	for name, s := range subjects(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			require.NoError(t, s.store.Put(ctx, objectstore.Ref{Key: "a/b"}, []byte("default"), "text/plain"))
			require.NoError(t, s.store.Put(ctx, objectstore.Ref{Bucket: "other", Key: "a/b"}, []byte("other"), "text/plain"))

			got, err := s.store.Get(ctx, objectstore.Ref{Bucket: "continuo", Key: "a/b"}, 1024)
			require.NoError(t, err)
			assert.Equal(t, []byte("default"), got)
			got, err = s.store.Get(ctx, objectstore.Ref{Bucket: "other", Key: "a/b"}, 1024)
			require.NoError(t, err)
			assert.Equal(t, []byte("other"), got)
			_, err = s.store.Get(ctx, objectstore.Ref{Bucket: "third", Key: "a/b"}, 1024)
			assert.ErrorIs(t, err, objectstore.ErrNotFound)
		})
	}
}

func TestRoles_ARefThatNamesNoObjectIsRefusedBeforeTheStore(t *testing.T) {
	for name, s := range subjects(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			for _, ref := range []objectstore.Ref{{}, {Bucket: "continuo"}, {Key: " "}, {Key: "/k"}, {Bucket: "a/b", Key: "k"}} {
				_, err := s.store.Get(ctx, ref, 1024)
				assert.ErrorIs(t, err, objectstore.ErrInvalidRef, "get %+v", ref)
				assert.ErrorIs(t, s.store.Put(ctx, ref, []byte("x"), "text/plain"), objectstore.ErrInvalidRef, "put %+v", ref)
			}
			assert.Error(t, s.store.DeletePrefix(ctx, ""), "an empty prefix names the whole bucket")
			assert.Zero(t, s.calls())
		})
	}
}

func TestRoles_AnUnusableLimitIsRefusedBeforeTheStore(t *testing.T) {
	for name, s := range subjects(t) {
		t.Run(name, func(t *testing.T) {
			for _, limit := range []int64{0, -1, math.MaxInt64} {
				_, err := s.store.Get(context.Background(), objectstore.Ref{Key: "k"}, limit)
				assert.Error(t, err, "limit %d", limit)
				assert.NotErrorIs(t, err, objectstore.ErrNotFound)
			}
			assert.Zero(t, s.calls())
		})
	}
}

func TestRoles_DeletePrefixRemovesMatchingKeysOfTheDefaultBucketOnly(t *testing.T) {
	for name, s := range subjects(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			for _, ref := range []objectstore.Ref{
				{Key: "a/1"}, {Key: "a/2"}, {Key: "b/1"}, {Bucket: "other", Key: "a/1"},
			} {
				require.NoError(t, s.store.Put(ctx, ref, []byte("x"), "text/plain"))
			}

			require.NoError(t, s.store.DeletePrefix(ctx, "a/"))
			require.NoError(t, s.store.DeletePrefix(ctx, "nothing/"), "nothing matching is not an error")

			for ref, gone := range map[objectstore.Ref]bool{
				{Key: "a/1"}:                  true,
				{Key: "a/2"}:                  true,
				{Key: "b/1"}:                  false,
				{Bucket: "other", Key: "a/1"}: false,
			} {
				_, err := s.store.Get(ctx, ref, 1024)
				if gone {
					assert.ErrorIs(t, err, objectstore.ErrNotFound, "%+v", ref)
				} else {
					assert.NoError(t, err, "%+v", ref)
				}
			}
		})
	}
}
