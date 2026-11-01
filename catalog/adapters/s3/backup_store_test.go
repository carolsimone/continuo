package s3

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/catalog/serialization/exportfile"
	"github.com/carolsimone/continuo/catalog/service/export"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
)

var backupAt = time.Date(2026, 10, 30, 2, 0, 0, 0, time.UTC)

// exportOf is an export of tenantID holding n :Node nodes and a chain of n-1
// relationships, as its header and its records.
func exportOf(tenantID string, n int) (export.Header, []any) {
	h := export.Header{
		FormatVersion: export.FormatVersion, TenantID: tenantID, ExportedAt: backupAt, CatalogVersion: "test",
		Nodes: map[string]int64{}, Relationships: map[string]int64{},
	}
	if n > 0 {
		h.Nodes["Node"] = int64(n)
	}
	if n > 1 {
		h.Relationships["DEPENDS_ON"] = int64(n - 1)
	}
	var recs []any
	for i := range n {
		recs = append(recs, export.NodeRecord{Labels: []string{"Node"}, Props: map[string]any{
			"tenant_id": tenantID, "unique_id": fmt.Sprintf("svc.t%d", i),
		}})
	}
	for i := 1; i < n; i++ {
		recs = append(recs, export.RelRecord{
			Type:  "DEPENDS_ON",
			From:  export.NodeRef{Label: "Node", Key: []any{fmt.Sprintf("svc.t%d", i)}},
			To:    export.NodeRef{Label: "Node", Key: []any{fmt.Sprintf("svc.t%d", i-1)}},
			Props: map[string]any{},
		})
	}
	return h, recs
}

// writeRecords writes h and recs to w.
func writeRecords(t *testing.T, w ports.RecordWriter, h export.Header, recs []any) {
	t.Helper()
	require.NoError(t, w.WriteHeader(h))
	for _, rec := range recs {
		switch x := rec.(type) {
		case export.NodeRecord:
			require.NoError(t, w.WriteNode(x))
		case export.RelRecord:
			require.NoError(t, w.WriteRel(x))
		}
	}
}

// storeExport stores an export of tenantID taken at at holding n nodes,
// through the store, and returns its key.
func storeExport(t *testing.T, store *BackupStore, tenantID string, at time.Time, n int) string {
	t.Helper()
	w, key, err := store.Open(context.Background(), tenantID, at, nil)
	require.NoError(t, err)
	h, recs := exportOf(tenantID, n)
	writeRecords(t, w, h, recs)
	_, err = w.Close()
	require.NoError(t, err)
	return key
}

// readAll reads every record of src.
func readAll(t *testing.T, src ports.ExportSource) []any {
	t.Helper()
	var out []any
	for {
		rec, err := src.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		require.NoError(t, err)
		out = append(out, rec)
	}
}

// gzipped compresses body.
func gzipped(t *testing.T, body []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(body)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

// exportFile is the export file of tenantID holding n nodes, gzipped, as the
// store keeps it.
func exportFile(t *testing.T, tenantID string, n int) []byte {
	t.Helper()
	var buf bytes.Buffer
	h, recs := exportOf(tenantID, n)
	writeRecords(t, exportfile.NewWriter(&buf), h, recs)
	return gzipped(t, buf.Bytes())
}

// Open streams the export's records, as the gzipped export file, to the
// tenant's backup key in the default bucket, visible only once the sink
// closes; Close returns the size of the stored object, and the export reads
// back record for record.
func TestBackupStore_OpenStoresTheRecordsAsAGzippedExportFile(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	store := NewBackupStore(mem)
	ctx := context.Background()

	w, key, err := store.Open(ctx, "default", backupAt.Add(1500*time.Millisecond), nil)
	require.NoError(t, err)
	assert.Equal(t, "tenants/default/backups/catalog/20261030T020001Z.jsonl.gz", key)
	h, recs := exportOf("default", 3)
	writeRecords(t, w, h, recs)
	_, err = mem.GetStream(ctx, objectstore.Ref{Key: key})
	require.ErrorIs(t, err, objectstore.ErrNotFound, "nothing is visible before Close")
	size, err := w.Close()
	require.NoError(t, err)

	puts := mem.Puts()
	require.Len(t, puts, 1)
	assert.Equal(t, objectstore.Ref{Bucket: defaultBucket, Key: key}, puts[0].Ref)
	assert.Equal(t, "application/gzip", puts[0].ContentType)
	assert.Equal(t, int64(len(puts[0].Body)), size, "Close returns the stored object's size")
	zr, err := gzip.NewReader(bytes.NewReader(puts[0].Body))
	require.NoError(t, err)
	got, err := exportfile.Validate(zr, "default")
	require.NoError(t, err, "the object is the export file of the records")
	assert.Equal(t, h, got)

	src, err := store.OpenRead(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, h, src.Header())
	assert.Equal(t, recs, readAll(t, src))
	require.NoError(t, src.Close())
}

// A store that refuses the upload fails Open, before anything is written.
func TestBackupStore_OpenFailsWhenTheStoreRefusesTheUpload(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	key, _ := exportKey("default", backupAt)
	boom := errors.New("AccessDenied")
	mem.FailPut(objectstore.Ref{Key: key}, boom)

	w, _, err := NewBackupStore(mem).Open(context.Background(), "default", backupAt, nil)
	require.ErrorIs(t, err, boom)
	assert.Nil(t, w)
}

func TestBackupStore_RefusesATenantThatNamesNoPrefix(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	store := NewBackupStore(mem)
	for _, tenantID := range []string{"", ".", "..", "a/b"} {
		_, _, err := store.Open(context.Background(), tenantID, backupAt, nil)
		require.ErrorIs(t, err, ports.ErrInvalidTenant, tenantID)
		_, err = store.List(context.Background(), tenantID)
		require.ErrorIs(t, err, ports.ErrInvalidTenant, tenantID)
		require.ErrorIs(t, store.CheckTenant(tenantID), ports.ErrInvalidTenant, tenantID)
	}
	assert.Empty(t, mem.Puts())
}

// An aborted export leaves nothing, and a sink that was completed or aborted
// takes no record and no Close, with the port's error.
func TestBackupStore_AbortLeavesNoExportAndAClosedSinkRefusesUse(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	store := NewBackupStore(mem)
	h, recs := exportOf("default", 1)

	aborted, _, err := store.Open(context.Background(), "default", backupAt, nil)
	require.NoError(t, err)
	writeRecords(t, aborted, h, recs)
	require.NoError(t, aborted.Abort())
	require.NoError(t, aborted.Abort(), "Abort is idempotent")
	require.ErrorIs(t, aborted.WriteNode(recs[0].(export.NodeRecord)), ports.ErrExportClosed)
	_, err = aborted.Close()
	require.ErrorIs(t, err, ports.ErrExportClosed)
	listed, err := store.List(context.Background(), "default")
	require.NoError(t, err)
	assert.Empty(t, listed)

	closed, _, err := store.Open(context.Background(), "default", backupAt.Add(time.Hour), nil)
	require.NoError(t, err)
	writeRecords(t, closed, h, recs)
	_, err = closed.Close()
	require.NoError(t, err)
	require.NoError(t, closed.Abort(), "Abort after a successful Close does nothing")
	require.ErrorIs(t, closed.WriteHeader(h), ports.ErrExportClosed)
	_, err = closed.Close()
	require.ErrorIs(t, err, ports.ErrExportClosed)
	listed, err = store.List(context.Background(), "default")
	require.NoError(t, err)
	assert.Len(t, listed, 1, "the completed export stays")
}

// closedWriters is an object store whose writers are already closed, so every
// write reports objectstore.ErrWriterClosed.
type closedWriters struct{ *objectstoretest.Memory }

func (c closedWriters) OpenWriter(ctx context.Context, ref objectstore.Ref, contentType string) (objectstore.ObjectWriter, error) {
	w, err := c.Memory.OpenWriter(ctx, ref, contentType)
	if err != nil {
		return nil, err
	}
	if err := w.Abort(); err != nil {
		return nil, err
	}
	return w, nil
}

// The object store's own writer error does not cross the port: it is the
// port's ErrExportClosed.
func TestBackupStore_TranslatesAClosedObjectWriter(t *testing.T) {
	store := NewBackupStore(closedWriters{objectstoretest.NewMemory(defaultBucket)})
	w, _, err := store.Open(context.Background(), "default", backupAt, nil)
	require.NoError(t, err)
	h, _ := exportOf("default", 1)
	err = w.WriteHeader(h) // the compressor writes its own header with the first line
	require.ErrorIs(t, err, ports.ErrExportClosed)
	assert.NotErrorIs(t, err, objectstore.ErrWriterClosed)
	_, err = w.Close()
	require.ErrorIs(t, err, ports.ErrExportClosed)
	assert.NotErrorIs(t, err, objectstore.ErrWriterClosed)
}

// A sink obeys the context it was opened with: once it is cancelled, the
// export fails with context.Canceled and stores nothing.
func TestBackupStore_ASinkFailsOnceItsContextIsCancelled(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	store := NewBackupStore(mem)
	ctx, cancel := context.WithCancel(context.Background())
	w, _, err := store.Open(ctx, "default", backupAt, nil)
	require.NoError(t, err)
	h, recs := exportOf("default", 2)
	writeRecords(t, w, h, recs)
	cancel()

	_, err = w.Close()
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, w.Abort())
	assert.Empty(t, mem.Puts())
}

// List returns the tenant's exports newest first, with their tenant, the time
// their key names, their size and their modification time, and leaves out
// every other object: another tenant's exports (also one whose id begins with
// this one's) and objects under the backup prefix that are not exports.
func TestBackupStore_ListIsTheTenantsExportsNewestFirst(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	store := NewBackupStore(mem)
	ctx := context.Background()
	oldest := storeExport(t, store, "t", backupAt.Add(-48*time.Hour), 1)
	newest := storeExport(t, store, "t", backupAt, 3)
	middle := storeExport(t, store, "t", backupAt.Add(-24*time.Hour), 2)
	storeExport(t, store, "t2", backupAt.Add(time.Hour), 1)
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
	var taken []time.Time
	for _, o := range listed {
		keys = append(keys, o.Key)
		taken = append(taken, o.TakenAt)
		assert.Equal(t, "t", o.TenantID, o.Key)
		assert.False(t, o.LastModified.IsZero(), o.Key)
		obj, err := mem.Get(ctx, objectstore.Ref{Key: o.Key}, 1<<20)
		require.NoError(t, err)
		assert.Equal(t, int64(len(obj)), o.Size, o.Key)
	}
	assert.Equal(t, []string{newest, middle, oldest}, keys)
	assert.Equal(t, []time.Time{backupAt, backupAt.Add(-24 * time.Hour), backupAt.Add(-48 * time.Hour)}, taken)

	none, err := store.List(ctx, "nobody")
	require.NoError(t, err)
	assert.Empty(t, none)
}

func TestBackupStore_ListFailureKeepsItsCause(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	prefix, _ := exportPrefix("t")
	boom := errors.New("SlowDown")
	mem.FailList(prefix, boom)

	_, err := NewBackupStore(mem).List(context.Background(), "t")
	require.ErrorIs(t, err, boom)
}

// OpenRead and Validate read an export back; a missing one is
// ErrBackupNotFound; a key that is not an export's is refused before the
// store is reached.
func TestBackupStore_ReadsAnExportBack(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	store := NewBackupStore(mem)
	ctx := context.Background()
	key := storeExport(t, store, "t", backupAt, 2)
	want, _ := exportOf("t", 2)

	h, err := store.Validate(ctx, key, "t")
	require.NoError(t, err)
	assert.Equal(t, want, h)

	missing, _ := exportKey("t", backupAt.Add(time.Hour))
	_, err = store.OpenRead(ctx, missing)
	require.ErrorIs(t, err, ports.ErrBackupNotFound)
	_, err = store.Validate(ctx, missing, "t")
	require.ErrorIs(t, err, ports.ErrBackupNotFound)

	require.NoError(t, mem.Put(ctx, objectstore.Ref{Key: "tenants/t/topologies/r1/topology.json.gz"}, []byte("x"), "application/gzip"))
	for _, other := range []string{"tenants/t/topologies/r1/topology.json.gz", "", "s3://continuo/" + key} {
		_, err := store.OpenRead(ctx, other)
		require.ErrorIs(t, err, ports.ErrNotAnExportKey, other)
		_, err = store.Validate(ctx, other, "t")
		require.ErrorIs(t, err, ports.ErrNotAnExportKey, other)
	}
}

func TestBackupStore_ReadFailureKeepsItsCause(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	key, _ := exportKey("t", backupAt)
	boom := errors.New("InternalError")
	mem.FailGet(objectstore.Ref{Key: key}, boom)

	_, err := NewBackupStore(mem).OpenRead(context.Background(), key)
	require.ErrorIs(t, err, boom)
	assert.NotErrorIs(t, err, ports.ErrBackupNotFound)
}

// An object that does not decompress, is cut short, has a damaged gzip
// trailer or is another tenant's export fails Validate; OpenRead refuses one
// whose header it cannot read.
func TestBackupStore_ValidateRefusesAnObjectARestoreCannotLoad(t *testing.T) {
	good := exportFile(t, "t", 2)
	cases := map[string][]byte{
		"not gzip":                []byte("{\"format_version\":1}\n"),
		"cut short":               good[:len(good)-6],
		"another tenant's export": exportFile(t, "t2", 2),
		"a corrupt trailer": func() []byte {
			b := bytes.Clone(good)
			b[len(b)-8] ^= 0xff // the CRC-32 of the gzip trailer
			return b
		}(),
		"not an export file": gzipped(t, []byte("hello\n")),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			mem := objectstoretest.NewMemory(defaultBucket)
			key, _ := exportKey("t", backupAt)
			require.NoError(t, mem.Put(context.Background(), objectstore.Ref{Key: key}, body, backupContentType))

			_, err := NewBackupStore(mem).Validate(context.Background(), key, "t")
			require.Error(t, err)
			assert.NotErrorIs(t, err, ports.ErrBackupNotFound)
		})
	}
	for _, name := range []string{"not gzip", "not an export file"} {
		mem := objectstoretest.NewMemory(defaultBucket)
		key, _ := exportKey("t", backupAt)
		require.NoError(t, mem.Put(context.Background(), objectstore.Ref{Key: key}, cases[name], backupContentType))
		_, err := NewBackupStore(mem).OpenRead(context.Background(), key)
		require.Error(t, err, name)
	}
}

// Delete removes one export, and only an export: a key that is not an
// export's is refused and its object stays.
func TestBackupStore_DeleteRemovesOnlyAnExport(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	store := NewBackupStore(mem)
	ctx := context.Background()
	gone := storeExport(t, store, "t", backupAt.Add(-time.Hour), 1)
	kept := storeExport(t, store, "t", backupAt, 1)
	artifact := objectstore.Ref{Key: "tenants/t/topologies/r1/topology.json.gz"}
	require.NoError(t, mem.Put(ctx, artifact, []byte("x"), "application/gzip"))

	require.NoError(t, store.Delete(ctx, gone))
	require.NoError(t, store.Delete(ctx, gone), "a missing export is not an error")
	require.ErrorIs(t, store.Delete(ctx, artifact.Key), ports.ErrNotAnExportKey)

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
	key := storeExport(t, store, "t", backupAt, 1)
	boom := errors.New("AccessDenied")
	mem.FailDelete(objectstore.Ref{Key: key}, boom)

	require.ErrorIs(t, store.Delete(context.Background(), key), boom)
}

// A record that fills several parts of the object counts as progress once per
// write to the store, so a stall watch sees a slow upload move.
func TestBackupStore_ProgressFiresForEachWriteToTheStore(t *testing.T) {
	mem := objectstoretest.NewMemory("continuo")
	progressed := 0
	w, _, err := NewBackupStore(mem).Open(context.Background(), "default", backupAt, func() { progressed++ })
	require.NoError(t, err)
	h, _ := exportOf("default", 1)
	require.NoError(t, w.WriteHeader(h))

	// Random hex does not compress, so the one record becomes many writes.
	raw := make([]byte, 10<<20)
	_, err = rand.Read(raw)
	require.NoError(t, err)
	require.NoError(t, w.WriteNode(export.NodeRecord{Labels: []string{"Node"}, Props: map[string]any{
		"tenant_id": "default", "unique_id": "svc.big", "blob": hex.EncodeToString(raw),
	}}))
	before := progressed
	_, err = w.Close()
	require.NoError(t, err)

	assert.GreaterOrEqual(t, progressed, 3, "each write to the object store is progress")
	assert.Positive(t, progressed-before, "completing the export writes the compressor's tail")
}
