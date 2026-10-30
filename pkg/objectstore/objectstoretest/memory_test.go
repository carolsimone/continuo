package objectstoretest_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
)

// Reads and Puts record each call with its Ref resolved, so a test can assert
// which bucket an adapter reached and with which limit.
func TestMemory_RecordsReadsAndWritesWithTheirRefResolved(t *testing.T) {
	ctx := context.Background()
	mem := objectstoretest.NewMemory("continuo")

	require.NoError(t, mem.Put(ctx, objectstore.Ref{Key: "a/b"}, []byte("body"), "text/plain"))
	_, err := mem.Get(ctx, objectstore.Ref{Key: "a/b"}, 64)
	require.NoError(t, err)

	resolved := objectstore.Ref{Bucket: "continuo", Key: "a/b"}
	assert.Equal(t, []objectstoretest.Write{{Ref: resolved, Body: []byte("body"), ContentType: "text/plain"}}, mem.Puts())
	assert.Equal(t, []objectstoretest.Read{{Ref: resolved, MaxBytes: 64}}, mem.Reads())
}

// FailGet and FailPut inject an error for one object, named with or without
// its default bucket. Other objects are unaffected, and a failed write stores
// nothing and is not recorded.
func TestMemory_FailGetAndFailPutInjectAnErrorForOneObject(t *testing.T) {
	ctx := context.Background()
	mem := objectstoretest.NewMemory("continuo")
	boom := errors.New("boom")
	require.NoError(t, mem.Put(ctx, objectstore.Ref{Key: "a"}, []byte("x"), ""))
	require.NoError(t, mem.Put(ctx, objectstore.Ref{Key: "b"}, []byte("y"), ""))

	mem.FailGet(objectstore.Ref{Key: "a"}, boom)
	_, err := mem.Get(ctx, objectstore.Ref{Bucket: "continuo", Key: "a"}, 64)
	assert.ErrorIs(t, err, boom)
	got, err := mem.Get(ctx, objectstore.Ref{Key: "b"}, 64)
	require.NoError(t, err)
	assert.Equal(t, []byte("y"), got)

	mem.FailPut(objectstore.Ref{Bucket: "continuo", Key: "c"}, boom)
	assert.ErrorIs(t, mem.Put(ctx, objectstore.Ref{Key: "c"}, []byte("z"), ""), boom)
	_, err = mem.Get(ctx, objectstore.Ref{Key: "c"}, 64)
	assert.ErrorIs(t, err, objectstore.ErrNotFound, "a failed write stores nothing")
	assert.Len(t, mem.Puts(), 2, "a failed write is not recorded")
}

func TestMemory_FailDeletePrefixInjectsAnErrorAndKeepsTheObjects(t *testing.T) {
	ctx := context.Background()
	mem := objectstoretest.NewMemory("continuo")
	boom := errors.New("boom")
	require.NoError(t, mem.Put(ctx, objectstore.Ref{Key: "p/a"}, []byte("x"), ""))
	mem.FailDeletePrefix("p/", boom)

	assert.ErrorIs(t, mem.DeletePrefix(ctx, "p/"), boom)
	_, err := mem.Get(ctx, objectstore.Ref{Key: "p/a"}, 64)
	assert.NoError(t, err)
}

// A caller that edits the slice it wrote, or one it read, changes neither the
// stored object nor what Puts recorded.
func TestMemory_CopiesBodiesInAndOut(t *testing.T) {
	ctx := context.Background()
	mem := objectstoretest.NewMemory("continuo")
	body := []byte("body")
	require.NoError(t, mem.Put(ctx, objectstore.Ref{Key: "k"}, body, ""))
	body[0] = 'X'
	got, err := mem.Get(ctx, objectstore.Ref{Key: "k"}, 64)
	require.NoError(t, err)
	got[1] = 'Y'
	mem.Puts()[0].Body[2] = 'Z'

	again, err := mem.Get(ctx, objectstore.Ref{Key: "k"}, 64)
	require.NoError(t, err)
	assert.Equal(t, []byte("body"), again)
	assert.Equal(t, []byte("body"), mem.Puts()[0].Body)
}

// A writer that completes is recorded in Puts like a Put, with its content
// type; one that is aborted records nothing.
func TestMemory_CompletedWritersAreRecordedInPutsAndAbortedOnesAreNot(t *testing.T) {
	ctx := context.Background()
	mem := objectstoretest.NewMemory("continuo")

	w, err := mem.OpenWriter(ctx, objectstore.Ref{Key: "streamed"}, "application/x-ndjson")
	require.NoError(t, err)
	chunk := []byte("ab")
	_, err = w.Write(chunk)
	require.NoError(t, err)
	chunk[0] = 'X' // the writer copied what it was given
	_, err = w.Write([]byte("cd"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	aborted, err := mem.OpenWriter(ctx, objectstore.Ref{Key: "aborted"}, "text/plain")
	require.NoError(t, err)
	_, err = aborted.Write([]byte("zz"))
	require.NoError(t, err)
	require.NoError(t, aborted.Abort())

	assert.Equal(t, []objectstoretest.Write{{
		Ref:         objectstore.Ref{Bucket: "continuo", Key: "streamed"},
		Body:        []byte("abcd"),
		ContentType: "application/x-ndjson",
	}}, mem.Puts())
	assert.Empty(t, mem.Reads(), "a stream read is not a Get")
}

// FailPut refuses a writer where a store refuses a multipart upload, at open,
// and refuses a write that was already open when the failure was injected, at
// Close; in both cases nothing is stored.
func TestMemory_FailPutRefusesAWriterAtOpenAndAtClose(t *testing.T) {
	ctx := context.Background()
	mem := objectstoretest.NewMemory("continuo")
	boom := errors.New("boom")

	open, err := mem.OpenWriter(ctx, objectstore.Ref{Key: "late"}, "text/plain")
	require.NoError(t, err)
	_, err = open.Write([]byte("x"))
	require.NoError(t, err)
	mem.FailPut(objectstore.Ref{Key: "late"}, boom)
	mem.FailPut(objectstore.Ref{Key: "early"}, boom)

	w, err := mem.OpenWriter(ctx, objectstore.Ref{Bucket: "continuo", Key: "early"}, "text/plain")
	assert.ErrorIs(t, err, boom)
	assert.Nil(t, w)
	assert.ErrorIs(t, open.Close(), boom)
	require.NoError(t, open.Abort(), "a writer whose Close failed can still be aborted")

	for _, key := range []string{"early", "late"} {
		_, err := mem.GetStream(ctx, objectstore.Ref{Key: key})
		assert.ErrorIs(t, err, objectstore.ErrNotFound, key)
	}
	assert.Empty(t, mem.Puts())
}

// FailList refuses a List of exactly one prefix; a List of another prefix,
// even one the failing prefix begins with, still answers.
func TestMemory_FailListInjectsAnErrorForOnePrefix(t *testing.T) {
	ctx := context.Background()
	mem := objectstoretest.NewMemory("continuo")
	boom := errors.New("boom")
	require.NoError(t, mem.Put(ctx, objectstore.Ref{Key: "p/q/a"}, []byte("x"), ""))
	mem.FailList("p/q/", boom)

	_, err := mem.List(ctx, "p/q/")
	assert.ErrorIs(t, err, boom)
	listed, err := mem.List(ctx, "p/")
	require.NoError(t, err)
	require.Len(t, listed, 1)
	assert.Equal(t, "p/q/a", listed[0].Key)
}

// FailDelete refuses a Delete of one object, named with or without its
// default bucket, and keeps it; other objects are deleted as before, and
// DeletePrefix is not affected.
func TestMemory_FailDeleteInjectsAnErrorForOneObject(t *testing.T) {
	ctx := context.Background()
	mem := objectstoretest.NewMemory("continuo")
	boom := errors.New("boom")
	for _, key := range []string{"p/a", "p/b"} {
		require.NoError(t, mem.Put(ctx, objectstore.Ref{Key: key}, []byte("x"), ""))
	}
	mem.FailDelete(objectstore.Ref{Key: "p/a"}, boom)

	assert.ErrorIs(t, mem.Delete(ctx, objectstore.Ref{Bucket: "continuo", Key: "p/a"}), boom)
	_, err := mem.Get(ctx, objectstore.Ref{Key: "p/a"}, 64)
	require.NoError(t, err, "a refused Delete keeps the object")
	require.NoError(t, mem.Delete(ctx, objectstore.Ref{Key: "p/b"}))
	_, err = mem.Get(ctx, objectstore.Ref{Key: "p/b"}, 64)
	assert.ErrorIs(t, err, objectstore.ErrNotFound)

	require.NoError(t, mem.DeletePrefix(ctx, "p/"), "FailDelete does not refuse DeletePrefix")
	_, err = mem.Get(ctx, objectstore.Ref{Key: "p/a"}, 64)
	assert.ErrorIs(t, err, objectstore.ErrNotFound)
}

func TestMemory_FailGetAppliesToGetStream(t *testing.T) {
	ctx := context.Background()
	mem := objectstoretest.NewMemory("continuo")
	boom := errors.New("boom")
	require.NoError(t, mem.Put(ctx, objectstore.Ref{Key: "a"}, []byte("x"), ""))
	require.NoError(t, mem.Put(ctx, objectstore.Ref{Key: "b"}, []byte("y"), ""))
	mem.FailGet(objectstore.Ref{Key: "a"}, boom)

	_, err := mem.GetStream(ctx, objectstore.Ref{Key: "a"})
	assert.ErrorIs(t, err, boom)
	rc, err := mem.GetStream(ctx, objectstore.Ref{Key: "b"})
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, []byte("y"), got)
}
