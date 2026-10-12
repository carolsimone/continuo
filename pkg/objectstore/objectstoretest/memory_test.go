package objectstoretest_test

import (
	"context"
	"errors"
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
