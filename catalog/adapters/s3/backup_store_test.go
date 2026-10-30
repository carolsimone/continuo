package s3

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/catalog/domain/export"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
)

var backupAt = time.Date(2026, 10, 30, 2, 0, 0, 0, time.UTC)

// storeExport writes body as tenantID's export taken at at, through the store.
func storeExport(t *testing.T, store *BackupStore, tenantID string, at time.Time, body string) string {
	t.Helper()
	w, key, err := store.Open(context.Background(), tenantID, at)
	require.NoError(t, err)
	_, err = io.WriteString(w, body)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return key
}

// Open streams the export to the tenant's backup key in the default bucket,
// visible only once the writer closes.
func TestBackupStore_OpenWritesTheExportAtItsKey(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	store := NewBackupStore(mem)
	ctx := context.Background()

	w, key, err := store.Open(ctx, "default", backupAt.Add(1500*time.Millisecond))
	require.NoError(t, err)
	assert.Equal(t, "tenants/default/backups/catalog/20261030T020001Z.jsonl.gz", key)
	_, err = io.WriteString(w, "gzipped lines")
	require.NoError(t, err)
	_, err = mem.GetStream(ctx, objectstore.Ref{Key: key})
	require.ErrorIs(t, err, objectstore.ErrNotFound, "nothing is visible before Close")
	require.NoError(t, w.Close())

	puts := mem.Puts()
	require.Len(t, puts, 1)
	assert.Equal(t, objectstore.Ref{Bucket: defaultBucket, Key: key}, puts[0].Ref)
	assert.Equal(t, "gzipped lines", string(puts[0].Body))
	assert.Equal(t, "application/gzip", puts[0].ContentType)
}

// A store that refuses the upload fails Open, before anything is written.
func TestBackupStore_OpenFailsWhenTheStoreRefusesTheUpload(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	key, _ := export.ObjectKey("default", backupAt)
	boom := errors.New("AccessDenied")
	mem.FailPut(objectstore.Ref{Key: key}, boom)

	w, _, err := NewBackupStore(mem).Open(context.Background(), "default", backupAt)
	require.ErrorIs(t, err, boom)
	assert.Nil(t, w)
}

func TestBackupStore_OpenRefusesATenantThatNamesNoPrefix(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	for _, tenantID := range []string{"", "a/b"} {
		_, _, err := NewBackupStore(mem).Open(context.Background(), tenantID, backupAt)
		require.ErrorIs(t, err, export.ErrNotAnExportKey, tenantID)
	}
	assert.Empty(t, mem.Puts())
}

// An aborted export leaves nothing.
func TestBackupStore_AbortLeavesNoExport(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	store := NewBackupStore(mem)
	w, _, err := store.Open(context.Background(), "default", backupAt)
	require.NoError(t, err)
	_, err = io.WriteString(w, "half")
	require.NoError(t, err)
	require.NoError(t, w.Abort())

	listed, err := store.List(context.Background(), "default")
	require.NoError(t, err)
	assert.Empty(t, listed)
}

// List returns the tenant's exports newest first, with their size and time,
// and leaves out every other object: another tenant's exports (also one whose
// id begins with this one's) and objects under the backup prefix that are
// not exports.
func TestBackupStore_ListIsTheTenantsExportsNewestFirst(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	store := NewBackupStore(mem)
	ctx := context.Background()
	oldest := storeExport(t, store, "t", backupAt.Add(-48*time.Hour), "1")
	newest := storeExport(t, store, "t", backupAt, "333")
	middle := storeExport(t, store, "t", backupAt.Add(-24*time.Hour), "22")
	storeExport(t, store, "t2", backupAt.Add(time.Hour), "x")
	for _, key := range []string{
		"tenants/t/backups/catalog/notes.txt",
		"tenants/t/backups/catalog/20261030T020000Z.jsonl.gz.part",
		"tenants/t/backups/catalog/old/20261001T020000Z.jsonl.gz",
		"tenants/t/topologies/r1/topology.json.gz",
	} {
		require.NoError(t, mem.Put(ctx, objectstore.Ref{Key: key}, []byte("x"), "text/plain"))
	}

	listed, err := store.List(ctx, "t")
	require.NoError(t, err)
	var keys []string
	for _, o := range listed {
		keys = append(keys, o.Key)
		assert.False(t, o.LastModified.IsZero(), o.Key)
	}
	assert.Equal(t, []string{newest, middle, oldest}, keys)
	assert.Equal(t, []int64{3, 2, 1}, []int64{listed[0].Size, listed[1].Size, listed[2].Size})

	none, err := store.List(ctx, "nobody")
	require.NoError(t, err)
	assert.Empty(t, none)
}

func TestBackupStore_ListFailureKeepsItsCause(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	prefix, _ := export.ObjectPrefix("t")
	boom := errors.New("SlowDown")
	mem.FailList(prefix, boom)

	_, err := NewBackupStore(mem).List(context.Background(), "t")
	require.ErrorIs(t, err, boom)
}

// Read streams an export back; a missing one is ErrBackupNotFound; a key that
// is not an export's is refused before the store is reached.
func TestBackupStore_ReadStreamsAnExport(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	store := NewBackupStore(mem)
	ctx := context.Background()
	key := storeExport(t, store, "t", backupAt, "the export")

	rc, err := store.Read(ctx, key)
	require.NoError(t, err)
	body, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	assert.Equal(t, "the export", string(body))

	missing, _ := export.ObjectKey("t", backupAt.Add(time.Hour))
	_, err = store.Read(ctx, missing)
	require.ErrorIs(t, err, ports.ErrBackupNotFound)

	require.NoError(t, mem.Put(ctx, objectstore.Ref{Key: "tenants/t/topologies/r1/topology.json.gz"}, []byte("x"), "application/gzip"))
	for _, other := range []string{"tenants/t/topologies/r1/topology.json.gz", "", "s3://continuo/" + key} {
		_, err := store.Read(ctx, other)
		require.ErrorIs(t, err, export.ErrNotAnExportKey, other)
	}
}

func TestBackupStore_ReadFailureKeepsItsCause(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	key, _ := export.ObjectKey("t", backupAt)
	boom := errors.New("InternalError")
	mem.FailGet(objectstore.Ref{Key: key}, boom)

	_, err := NewBackupStore(mem).Read(context.Background(), key)
	require.ErrorIs(t, err, boom)
	assert.NotErrorIs(t, err, ports.ErrBackupNotFound)
}

// Delete removes one export, and only an export: a key that is not an
// export's is refused and its object stays.
func TestBackupStore_DeleteRemovesOnlyAnExport(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	store := NewBackupStore(mem)
	ctx := context.Background()
	gone := storeExport(t, store, "t", backupAt.Add(-time.Hour), "old")
	kept := storeExport(t, store, "t", backupAt, "new")
	artifact := objectstore.Ref{Key: "tenants/t/topologies/r1/topology.json.gz"}
	require.NoError(t, mem.Put(ctx, artifact, []byte("x"), "application/gzip"))

	require.NoError(t, store.Delete(ctx, gone))
	require.NoError(t, store.Delete(ctx, gone), "a missing export is not an error")
	require.ErrorIs(t, store.Delete(ctx, artifact.Key), export.ErrNotAnExportKey)

	listed, err := store.List(ctx, "t")
	require.NoError(t, err)
	require.Len(t, listed, 1)
	assert.Equal(t, kept, listed[0].Key)
	_, err = mem.Get(ctx, artifact, 64)
	assert.NoError(t, err, "an object that is not an export is never deleted")
}

func TestBackupStore_DeleteFailureKeepsItsCause(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	store := NewBackupStore(mem)
	key := storeExport(t, store, "t", backupAt, "x")
	boom := errors.New("AccessDenied")
	mem.FailDelete(objectstore.Ref{Key: key}, boom)

	require.ErrorIs(t, store.Delete(context.Background(), key), boom)
}
