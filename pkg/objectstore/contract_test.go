package objectstore_test

import (
	"bytes"
	"context"
	"io"
	"math"
	"math/rand"
	"testing"
	"time"

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
	objectstore.StreamWriter
	objectstore.StreamReader
	objectstore.Lister
	objectstore.Deleter
}

// subject is one implementation under contract, and a count of the calls
// that reached its backing store: HTTP requests for the Client, role calls
// for the Memory.
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
		"Memory": {store: mem, calls: mem.Requests},
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

const mib = 1 << 20

// content returns n bytes that differ from one mebibyte to the next, so a
// part written out of order or twice changes what a reader gets back.
func content(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.New(rand.NewSource(1)).Read(b) //nolint:gosec // deterministic test data
	return b
}

// writeAll writes body to w in chunks of chunk bytes.
func writeAll(t *testing.T, w objectstore.ObjectWriter, body []byte, chunk int) {
	t.Helper()
	for len(body) > 0 {
		n := min(chunk, len(body))
		written, err := w.Write(body[:n])
		require.NoError(t, err)
		require.Equal(t, n, written)
		body = body[n:]
	}
}

// readStream returns everything GetStream yields for ref.
func readStream(t *testing.T, s store, ref objectstore.Ref) []byte {
	t.Helper()
	rc, err := s.GetStream(context.Background(), ref)
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	return got
}

func TestRoles_OpenWriterCloseStoresTheObject(t *testing.T) {
	for name, s := range subjects(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			ref := objectstore.Ref{Key: "exports/graph.ndjson"}
			body := content(20 * mib)

			w, err := s.store.OpenWriter(ctx, ref, "application/x-ndjson")
			require.NoError(t, err)
			writeAll(t, w, body, mib)
			_, err = s.store.GetStream(ctx, ref)
			require.ErrorIs(t, err, objectstore.ErrNotFound, "nothing is visible before Close")
			require.NoError(t, w.Close())

			assert.True(t, bytes.Equal(body, readStream(t, s.store, ref)), "the 20 MiB written come back equal")
			got, err := s.store.Get(ctx, ref, 32*mib)
			require.NoError(t, err)
			assert.True(t, bytes.Equal(body, got), "Get reads the streamed object too")
		})
	}
}

// Close replaces an object already stored at the key, as Put does.
func TestRoles_OpenWriterCloseReplacesAnExistingObject(t *testing.T) {
	for name, s := range subjects(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			ref := objectstore.Ref{Key: "k"}
			require.NoError(t, s.store.Put(ctx, ref, []byte("old"), "text/plain"))

			w, err := s.store.OpenWriter(ctx, ref, "text/plain")
			require.NoError(t, err)
			writeAll(t, w, []byte("new"), 2)
			require.NoError(t, w.Close())

			assert.Equal(t, []byte("new"), readStream(t, s.store, ref))
		})
	}
}

func TestRoles_OpenWriterAbortLeavesNoObject(t *testing.T) {
	for name, s := range subjects(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fresh := objectstore.Ref{Key: "exports/fresh"}
			existing := objectstore.Ref{Key: "exports/existing"}
			require.NoError(t, s.store.Put(ctx, existing, []byte("kept"), "text/plain"))

			for _, ref := range []objectstore.Ref{fresh, existing} {
				w, err := s.store.OpenWriter(ctx, ref, "text/plain")
				require.NoError(t, err)
				writeAll(t, w, content(9*mib), mib)
				require.NoError(t, w.Abort())
				require.NoError(t, w.Abort(), "Abort is idempotent")
				_, err = w.Write([]byte("x"))
				assert.ErrorIs(t, err, objectstore.ErrWriterClosed, "a write after Abort is refused")
				assert.ErrorIs(t, w.Close(), objectstore.ErrWriterClosed, "an aborted writer cannot complete")
			}

			_, err := s.store.GetStream(ctx, fresh)
			assert.ErrorIs(t, err, objectstore.ErrNotFound, "an aborted write leaves no object")
			assert.Equal(t, []byte("kept"), readStream(t, s.store, existing), "an aborted write leaves the object it would have replaced")
			listed, err := s.store.List(ctx, "exports/")
			require.NoError(t, err)
			require.Len(t, listed, 1)
			assert.Equal(t, "exports/existing", listed[0].Key)
		})
	}
}

// A writer that is closed refuses more bytes and a second Close, and an Abort
// after Close discards nothing.
func TestRoles_OpenWriterRefusesUseAfterClose(t *testing.T) {
	for name, s := range subjects(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			ref := objectstore.Ref{Key: "k"}
			w, err := s.store.OpenWriter(ctx, ref, "text/plain")
			require.NoError(t, err)
			writeAll(t, w, []byte("body"), 4)
			require.NoError(t, w.Close())

			_, err = w.Write([]byte("more"))
			assert.ErrorIs(t, err, objectstore.ErrWriterClosed)
			assert.ErrorIs(t, w.Close(), objectstore.ErrWriterClosed)
			assert.NoError(t, w.Abort(), "Abort after Close is a no-op")
			assert.Equal(t, []byte("body"), readStream(t, s.store, ref), "the closed object stays")
		})
	}
}

// An object with no bytes is an object: S3 completes a multipart upload only
// from at least one part, and the empty object must still be stored.
func TestRoles_OpenWriterStoresAZeroByteObject(t *testing.T) {
	for name, s := range subjects(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			ref := objectstore.Ref{Key: "exports/empty"}
			w, err := s.store.OpenWriter(ctx, ref, "text/plain")
			require.NoError(t, err)
			require.NoError(t, w.Close())

			assert.Empty(t, readStream(t, s.store, ref))
			listed, err := s.store.List(ctx, "exports/")
			require.NoError(t, err)
			require.Len(t, listed, 1)
			assert.Zero(t, listed[0].Size)
		})
	}
}

// One Write of 20 MiB spans three 8 MiB parts: the object comes back whole.
func TestRoles_OneWriteLargerThanAPartIsStoredWhole(t *testing.T) {
	for name, s := range subjects(t) {
		t.Run(name, func(t *testing.T) {
			ref := objectstore.Ref{Key: "exports/one-write"}
			body := content(20 * mib)
			w, err := s.store.OpenWriter(context.Background(), ref, "text/plain")
			require.NoError(t, err)
			n, err := w.Write(body)
			require.NoError(t, err)
			assert.Equal(t, len(body), n, "the one Write takes every byte")
			require.NoError(t, w.Close())

			assert.True(t, bytes.Equal(body, readStream(t, s.store, ref)), "the 20 MiB written in one call come back equal")
		})
	}
}

// The count the refused-Ref assertions read rises with every streaming call
// that reaches the store, so those assertions catch an OpenWriter, a
// GetStream or a Delete that reaches it.
func TestRoles_StreamingCallsAreCountedWhenTheyReachTheStore(t *testing.T) {
	for name, s := range subjects(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			ref := objectstore.Ref{Key: "exports/counted"}

			before := s.calls()
			w, err := s.store.OpenWriter(ctx, ref, "text/plain")
			require.NoError(t, err)
			assert.Greater(t, s.calls(), before, "OpenWriter")
			require.NoError(t, w.Abort())

			before = s.calls()
			_, err = s.store.GetStream(ctx, ref)
			require.ErrorIs(t, err, objectstore.ErrNotFound)
			assert.Greater(t, s.calls(), before, "GetStream")

			before = s.calls()
			require.NoError(t, s.store.Delete(ctx, ref))
			assert.Greater(t, s.calls(), before, "Delete")
		})
	}
}

// 16 MiB is exactly two parts: the object comes back whole.
func TestRoles_OpenWriterStoresAnObjectOfExactlyWholeParts(t *testing.T) {
	for name, s := range subjects(t) {
		t.Run(name, func(t *testing.T) {
			ref := objectstore.Ref{Key: "exports/two-parts"}
			body := content(16 * mib)
			w, err := s.store.OpenWriter(context.Background(), ref, "text/plain")
			require.NoError(t, err)
			writeAll(t, w, body, 4*mib)
			require.NoError(t, w.Close())

			assert.True(t, bytes.Equal(body, readStream(t, s.store, ref)))
		})
	}
}

func TestRoles_GetStreamReadsAndReportsNotFound(t *testing.T) {
	for name, s := range subjects(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			ref := objectstore.Ref{Key: "a/b.json"}
			require.NoError(t, s.store.Put(ctx, ref, []byte("body"), "application/json"))
			require.NoError(t, s.store.Put(ctx, objectstore.Ref{Bucket: "other", Key: "a/b.json"}, []byte("other"), "application/json"))

			assert.Equal(t, []byte("body"), readStream(t, s.store, ref))
			assert.Equal(t, []byte("other"), readStream(t, s.store, objectstore.Ref{Bucket: "other", Key: "a/b.json"}))

			_, err := s.store.GetStream(ctx, objectstore.Ref{Key: "absent"})
			require.ErrorIs(t, err, objectstore.ErrNotFound)
			assert.NotErrorIs(t, err, objectstore.ErrTooLarge)
			assert.Contains(t, err.Error(), "s3://continuo/absent", "the error names the resolved object")
		})
	}
}

func TestRoles_ListReturnsKeysUnderPrefixInKeyOrder(t *testing.T) {
	for name, s := range subjects(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			for _, put := range []struct {
				ref  objectstore.Ref
				body string
			}{
				{objectstore.Ref{Key: "p/b"}, "22"},
				{objectstore.Ref{Key: "p/a"}, "1"},
				{objectstore.Ref{Key: "p/c/1"}, "333"},
				{objectstore.Ref{Key: "q/a"}, "x"},
				{objectstore.Ref{Bucket: "other", Key: "p/z"}, "x"},
			} {
				require.NoError(t, s.store.Put(ctx, put.ref, []byte(put.body), "text/plain"))
			}
			w, err := s.store.OpenWriter(ctx, objectstore.Ref{Key: "p/streamed"}, "text/plain")
			require.NoError(t, err)
			writeAll(t, w, []byte("4444"), 4)
			require.NoError(t, w.Close())

			before := time.Now().Add(-time.Minute)
			listed, err := s.store.List(ctx, "p/")
			require.NoError(t, err)

			require.Len(t, listed, 4)
			wantKeys := []string{"p/a", "p/b", "p/c/1", "p/streamed"}
			wantSizes := []int64{1, 2, 3, 4}
			for i, o := range listed {
				assert.Equal(t, wantKeys[i], o.Key, "key order, with the full key")
				assert.Equal(t, wantSizes[i], o.Size, o.Key)
				assert.True(t, o.LastModified.After(before) && !o.LastModified.After(time.Now().Add(time.Minute)),
					"%s was modified just now, got %s", o.Key, o.LastModified)
			}

			none, err := s.store.List(ctx, "nothing/")
			require.NoError(t, err)
			assert.Empty(t, none)
		})
	}
}

func TestRoles_DeleteRemovesOnlyThatObject(t *testing.T) {
	for name, s := range subjects(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			for _, ref := range []objectstore.Ref{{Key: "a/1"}, {Key: "a/2"}, {Bucket: "other", Key: "a/1"}} {
				require.NoError(t, s.store.Put(ctx, ref, []byte("x"), "text/plain"))
			}

			require.NoError(t, s.store.Delete(ctx, objectstore.Ref{Key: "a/1"}))
			require.NoError(t, s.store.Delete(ctx, objectstore.Ref{Key: "a/1"}), "a missing object is not an error")
			require.NoError(t, s.store.Delete(ctx, objectstore.Ref{Key: "never-there"}))

			_, err := s.store.GetStream(ctx, objectstore.Ref{Key: "a/1"})
			assert.ErrorIs(t, err, objectstore.ErrNotFound)
			assert.Equal(t, []byte("x"), readStream(t, s.store, objectstore.Ref{Key: "a/2"}), "a sibling key stays")
			assert.Equal(t, []byte("x"), readStream(t, s.store, objectstore.Ref{Bucket: "other", Key: "a/1"}), "the same key in another bucket stays")
		})
	}
}

// A writer's Write and Close obey the context it was opened with: once that
// context is cancelled they fail with context.Canceled and store nothing,
// every later Write and Close fails the same way, and Abort still discards
// what was written. OpenWriter on a cancelled context opens nothing.
func TestRoles_OpenWriterHonoursItsContext(t *testing.T) {
	for name, s := range subjects(t) {
		t.Run(name, func(t *testing.T) {
			ref := objectstore.Ref{Key: "exports/cancelled"}
			ctx, cancel := context.WithCancel(context.Background())
			w, err := s.store.OpenWriter(ctx, ref, "text/plain")
			require.NoError(t, err)
			writeAll(t, w, content(mib), mib)
			cancel()

			_, err = w.Write([]byte("x"))
			require.ErrorIs(t, err, context.Canceled)
			require.ErrorIs(t, w.Close(), context.Canceled)
			_, err = w.Write([]byte("x"))
			require.ErrorIs(t, err, context.Canceled, "the failure sticks")
			require.NoError(t, w.Abort(), "a writer whose context is cancelled can still be aborted")

			_, err = s.store.GetStream(context.Background(), ref)
			assert.ErrorIs(t, err, objectstore.ErrNotFound, "a cancelled writer stores nothing")

			opened, err := s.store.OpenWriter(ctx, objectstore.Ref{Key: "exports/never"}, "text/plain")
			assert.ErrorIs(t, err, context.Canceled, "OpenWriter on a cancelled context")
			assert.Nil(t, opened)
		})
	}
}

func TestRoles_OpenWriterRefusesAnInvalidRef(t *testing.T) {
	for name, s := range subjects(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			for _, ref := range []objectstore.Ref{{}, {Bucket: "continuo"}, {Key: " "}, {Key: "/k"}, {Bucket: "a/b", Key: "k"}} {
				w, err := s.store.OpenWriter(ctx, ref, "text/plain")
				assert.ErrorIs(t, err, objectstore.ErrInvalidRef, "open %+v", ref)
				assert.Nil(t, w, "open %+v", ref)
				_, err = s.store.GetStream(ctx, ref)
				assert.ErrorIs(t, err, objectstore.ErrInvalidRef, "stream %+v", ref)
				assert.ErrorIs(t, s.store.Delete(ctx, ref), objectstore.ErrInvalidRef, "delete %+v", ref)
			}
			assert.Zero(t, s.calls(), "no request reaches the store")
		})
	}
}
