package s3

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
)

func liveIndex() ports.LiveIndex {
	return ports.LiveIndex{
		SchemaVersion:  ports.LiveIndexSchemaVersion,
		TenantID:       "default",
		PromotionSeq:   52,
		ReleaseID:      "rel-77aa0bc-230",
		WrittenAt:      time.Date(2026, 11, 27, 16, 40, 14, 0, time.UTC),
		SettingsSHA256: "1c9e",
		Files: []ports.LiveIndexFile{
			{UniqueID: "analytics.py_daily_kpis", Version: 3, ContractHash: "sha256:3f1c"},
		},
	}
}

// Each file is stored under the tenant's live prefix as <unique_id>.odcs.yaml,
// as YAML, and the index as _index.json in the design's shape, naming each
// file's key within the folder.
func TestLiveContractsStore_Keys(t *testing.T) {
	ctx := context.Background()
	mem := objectstoretest.NewMemory(defaultBucket)
	store := NewLiveContractsStore(mem)

	require.NoError(t, store.Put(ctx, "default", "analytics.py_daily_kpis", []byte("id: analytics.py_daily_kpis\n")))
	require.NoError(t, store.PutIndex(ctx, "default", liveIndex()))

	puts := mem.Puts()
	require.Len(t, puts, 2)
	assert.Equal(t, objectstore.Ref{Bucket: defaultBucket, Key: "tenants/default/contracts/live/analytics.py_daily_kpis.odcs.yaml"}, puts[0].Ref)
	assert.Equal(t, "application/yaml", puts[0].ContentType)
	assert.Equal(t, "id: analytics.py_daily_kpis\n", string(puts[0].Body))
	assert.Equal(t, objectstore.Ref{Bucket: defaultBucket, Key: "tenants/default/contracts/live/_index.json"}, puts[1].Ref)
	assert.Equal(t, "application/json", puts[1].ContentType)
	assert.JSONEq(t, `{"schema_version": 1, "tenant_id": "default", "promotion_seq": 52, "release_id": "rel-77aa0bc-230",
 "written_at": "2026-11-27T16:40:14Z", "settings_sha256": "1c9e",
 "files": [{"unique_id": "analytics.py_daily_kpis", "key": "analytics.py_daily_kpis.odcs.yaml",
            "version": 3, "contract_hash": "sha256:3f1c"}]}`, string(puts[1].Body))

	require.NoError(t, store.Delete(ctx, "default", "analytics.py_daily_kpis"))
	_, err := mem.Get(ctx, puts[0].Ref, 1<<20)
	assert.ErrorIs(t, err, objectstore.ErrNotFound, "Delete removes the file")
	require.NoError(t, store.Delete(ctx, "default", "analytics.py_daily_kpis"), "a file already gone is no error")
}

func TestLiveContractsStore_ReadsBackTheIndexItWrote(t *testing.T) {
	ctx := context.Background()
	store := NewLiveContractsStore(objectstoretest.NewMemory(defaultBucket))

	_, found, err := store.ReadIndex(ctx, "default")
	require.NoError(t, err)
	assert.False(t, found, "a folder never written has no index")

	require.NoError(t, store.PutIndex(ctx, "default", liveIndex()))
	got, found, err := store.ReadIndex(ctx, "default")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, liveIndex(), got)
}

// An index this build cannot read is an error, never an empty folder: a
// rewrite that took it for one would leave every listed file in place.
func TestLiveContractsStore_RefusesAnUnreadableIndex(t *testing.T) {
	ctx := context.Background()
	mem := objectstoretest.NewMemory(defaultBucket)
	store := NewLiveContractsStore(mem)
	ref := objectstore.Ref{Key: "tenants/default/contracts/live/_index.json"}

	require.NoError(t, mem.Put(ctx, ref, []byte("not json"), "application/json"))
	_, _, err := store.ReadIndex(ctx, "default")
	assert.ErrorIs(t, err, ports.ErrLiveIndexUnreadable)
	assert.ErrorContains(t, err, ref.Key, "the error names the index to delete")

	require.NoError(t, mem.Put(ctx, ref, []byte(`{"schema_version": 2, "files": []}`), "application/json"))
	_, _, err = store.ReadIndex(ctx, "default")
	assert.ErrorContains(t, err, "schema_version 2")
	assert.ErrorIs(t, err, ports.ErrLiveIndexUnreadable)

	require.NoError(t, mem.Put(ctx, ref, []byte(`{"schema_version": 1, "written_at": "yesterday", "files": []}`), "application/json"))
	_, _, err = store.ReadIndex(ctx, "default")
	assert.ErrorIs(t, err, ports.ErrLiveIndexUnreadable)
}

func TestLiveContractsStore_RefusesAnIdThatNamesNoFile(t *testing.T) {
	ctx := context.Background()
	mem := objectstoretest.NewMemory(defaultBucket)
	store := NewLiveContractsStore(mem)

	for _, tc := range []struct{ tenant, uid string }{
		{"", "analytics.t"}, {"..", "analytics.t"}, {"a/b", "analytics.t"},
		{"default", ""}, {"default", "."}, {"default", "a/b"},
	} {
		assert.ErrorIs(t, store.Put(ctx, tc.tenant, tc.uid, []byte("x")), ports.ErrInvalidLiveFile, "%q %q", tc.tenant, tc.uid)
		assert.ErrorIs(t, store.Delete(ctx, tc.tenant, tc.uid), ports.ErrInvalidLiveFile, "%q %q", tc.tenant, tc.uid)
	}
	assert.ErrorIs(t, store.PutIndex(ctx, "a/b", liveIndex()), ports.ErrInvalidLiveFile)
	_, _, err := store.ReadIndex(ctx, "")
	assert.ErrorIs(t, err, ports.ErrInvalidLiveFile)
	assert.Zero(t, mem.Requests(), "an invalid id reaches no object")
}

func TestLiveContractsStore_AFailedWriteKeepsItsCause(t *testing.T) {
	ctx := context.Background()
	mem := objectstoretest.NewMemory(defaultBucket)
	outage := errors.New("503 slow down")
	mem.FailPut(objectstore.Ref{Key: "tenants/default/contracts/live/analytics.t.odcs.yaml"}, outage)

	err := NewLiveContractsStore(mem).Put(ctx, "default", "analytics.t", []byte("x"))

	assert.ErrorIs(t, err, outage)
}
