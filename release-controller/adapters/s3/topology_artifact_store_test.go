package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"reflect"
	"sort"
	"testing"

	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// bucket is the artifact store's bucket and the in-memory store's default one.
const bucket = "continuo"

// r1Ref is where the store writes release r1's artifact.
var r1Ref = objectstore.Ref{Bucket: bucket, Key: "tenants/default/topologies/r1/topology.json.gz"}

// outage is an object-store error that reports an HTTP 503, as the AWS SDK's
// response errors do.
type outage struct{}

func (outage) Error() string       { return "s3: service unavailable" }
func (outage) HTTPStatusCode() int { return 503 }

func sampleTopology() release.Topology {
	return release.Topology{
		{UniqueID: "svc.orders", SchemaName: "svc", TableName: "orders", ResolvedRelationID: "svc.orders", ServiceName: "svc",
			NodeType: "dbt-model", TestCount: 2, ContentHash: "sha256:o", ImageTag: "img:1", OriginalFilePath: "models/orders.sql",
			UpstreamUniqueIDs: []string{"svc.customers"}, Schedule: "daily"},
		{UniqueID: "svc.customers", SchemaName: "svc", TableName: "customers", ServiceName: "svc", NodeType: "dbt-seed",
			ContentHash: "sha256:c", ImageTag: "img:1", UpstreamUniqueIDs: []string{}, Schedule: "daily"},
		{UniqueID: "test.svc.not_null_orders_id", ServiceName: "svc", NodeType: "dbt-test",
			UpstreamUniqueIDs: []string{"svc.orders"}},
		//nolint:gosec // test fixture secret_ref value, not a credential
		{UniqueID: "api.rates", SchemaName: "api", TableName: "rates", ServiceName: "api", NodeType: "python-api",
			ImageTag: "img:2", UpstreamUniqueIDs: []string{}, SecretRef: "continuo-api-rates"},
	}
}

func byUniqueID(topo release.Topology) map[string]release.Node {
	out := make(map[string]release.Node, len(topo))
	for _, n := range topo {
		out[n.UniqueID] = n
	}
	return out
}

// stored returns the bytes mem holds at ref; the read it makes is recorded,
// so call it only where read counts are not asserted afterwards.
func stored(t *testing.T, mem *objectstoretest.Memory, ref objectstore.Ref) []byte {
	t.Helper()
	body, err := mem.Get(context.Background(), ref, topologyartifact.MaxObjectBytes)
	require.NoError(t, err)
	return body
}

func TestTopologyArtifactStore_WriteThenLoadRoundTrips(t *testing.T) {
	mem := objectstoretest.NewMemory(bucket)
	ref, err := NewTopologyArtifactStore(mem, bucket, 4).Write(context.Background(), "r1", sampleTopology())
	require.NoError(t, err)
	assert.Equal(t, "s3://continuo/tenants/default/topologies/r1/topology.json.gz", ref.URI)
	assert.Len(t, ref.SHA256, 64)
	assert.Equal(t, 4, ref.NodeCount)
	puts := mem.Puts()
	require.Len(t, puts, 1)
	assert.Equal(t, r1Ref, puts[0].Ref)
	assert.Equal(t, "application/gzip", puts[0].ContentType)

	// A second store has an empty cache, so this reads the object back.
	got, err := NewTopologyArtifactStore(mem, bucket, 4).Load(context.Background(), ref)
	require.NoError(t, err)
	assert.Equal(t, byUniqueID(sampleTopology()), byUniqueID(got))
	ids := make([]string, len(got))
	for i, n := range got {
		ids[i] = n.UniqueID
	}
	assert.True(t, sort.StringsAreSorted(ids), "an artifact's nodes come back in unique_id order")
}

// A write repeated for the same topology produces the same object and
// reference, so a retry after a crash between writing an artifact and
// recording its reference leaves the stored bytes and checksum unchanged.
func TestTopologyArtifactStore_RepeatedWriteIsByteIdentical(t *testing.T) {
	mem := objectstoretest.NewMemory(bucket)

	first, err := NewTopologyArtifactStore(mem, bucket, 4).Write(context.Background(), "r1", sampleTopology())
	require.NoError(t, err)
	firstBytes := stored(t, mem, r1Ref)

	second, err := NewTopologyArtifactStore(mem, bucket, 4).Write(context.Background(), "r1", sampleTopology())
	require.NoError(t, err)

	assert.Equal(t, first, second)
	assert.Equal(t, firstBytes, stored(t, mem, r1Ref))
	assert.Len(t, mem.Puts(), 1, "identical bytes already stored are not written again")
}

// The object is immutable: a repeated write of the same id carrying a changed
// topology would leave an earlier announcement's checksum naming bytes that no
// longer exist, so a different-bytes overwrite is refused and the stored object
// is left untouched. An identical-bytes re-write stays idempotent, so a crash
// between writing the object and recording its reference is repaired by the
// retry.
func TestTopologyArtifactStore_WriteRefusesAChangedOverwrite(t *testing.T) {
	mem := objectstoretest.NewMemory(bucket)

	first, err := NewTopologyArtifactStore(mem, bucket, 4).Write(context.Background(), "r1", sampleTopology())
	require.NoError(t, err)
	firstBytes := stored(t, mem, r1Ref)

	changed := sampleTopology()
	changed[0].ContentHash = "sha256:changed"
	_, err = NewTopologyArtifactStore(mem, bucket, 4).Write(context.Background(), "r1", changed)
	require.ErrorIs(t, err, ports.ErrTopologyArtifactImmutable)
	assert.Equal(t, firstBytes, stored(t, mem, r1Ref), "a changed-bytes overwrite leaves the stored object untouched")

	again, err := NewTopologyArtifactStore(mem, bucket, 4).Write(context.Background(), "r1", sampleTopology())
	require.NoError(t, err)
	assert.Equal(t, first, again, "an identical-bytes re-write returns the same reference")
	assert.Equal(t, firstBytes, stored(t, mem, r1Ref))
}

func TestTopologyArtifactStore_LoadOfAMissingObjectIsNotFound(t *testing.T) {
	_, err := NewTopologyArtifactStore(objectstoretest.NewMemory(bucket), bucket, 4).Load(context.Background(),
		release.TopologyRef{URI: "s3://continuo/tenants/default/topologies/gone/topology.json.gz", SHA256: "x"})
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactNotFound)
}

func TestTopologyArtifactStore_LoadWithAnotherChecksumIsCorrupt(t *testing.T) {
	mem := objectstoretest.NewMemory(bucket)
	ref, err := NewTopologyArtifactStore(mem, bucket, 4).Write(context.Background(), "r1", sampleTopology())
	require.NoError(t, err)

	ref.SHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
	_, err = NewTopologyArtifactStore(mem, bucket, 4).Load(context.Background(), ref)
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
	assert.ErrorIs(t, err, topologyartifact.ErrChecksumMismatch)
}

// An object over the size limit will not shrink on a later read: it is
// corrupt, a permanent failure.
func TestTopologyArtifactStore_LoadOfAnOversizedObjectIsCorrupt(t *testing.T) {
	mem := objectstoretest.NewMemory(bucket)
	mem.FailGet(r1Ref, objectstore.ErrTooLarge)

	_, err := NewTopologyArtifactStore(mem, bucket, 4).Load(context.Background(),
		release.TopologyRef{URI: r1Ref.String(), SHA256: "x"})
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
	assert.NotErrorIs(t, err, ports.ErrTopologyArtifactNotFound)
}

// A stored object that matches its checksum but cannot be decoded (a gzip
// stream cut short, bytes that are not gzip) is a corrupt artifact: permanent
// by nature, never an infrastructure outage, even though a cut-short gzip
// stream reports io.ErrUnexpectedEOF while it is read.
func TestTopologyArtifactStore_UndecodableObjectIsCorruptNotInfrastructure(t *testing.T) {
	for _, tc := range []struct {
		name   string
		damage func(gz []byte) []byte
	}{
		{"truncated gzip", func(gz []byte) []byte { return gz[:len(gz)/2] }},
		{"not gzip", func([]byte) []byte { return []byte("not a gzip stream") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := objectstoretest.NewMemory(bucket)
			ref, err := NewTopologyArtifactStore(mem, bucket, 4).Write(context.Background(), "r1", sampleTopology())
			require.NoError(t, err)
			damaged := tc.damage(stored(t, mem, r1Ref))
			require.NoError(t, mem.Put(context.Background(), r1Ref, damaged, "application/gzip"))
			sum := sha256.Sum256(damaged)
			ref.SHA256 = hex.EncodeToString(sum[:])

			_, err = NewTopologyArtifactStore(mem, bucket, 4).Load(context.Background(), ref)
			require.Error(t, err)
			assert.ErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
			assert.False(t, errors.Is(err, io.ErrUnexpectedEOF), "a decode failure is not a transport truncation")
			assert.NotEqual(t, pkgredis.ClassInfrastructure, pkgredis.Classify(err))
		})
	}
}

// A stored reference that does not name an object in this install's bucket is
// a defect no retry repairs: corrupt, and refused before any read.
func TestTopologyArtifactStore_LoadOfAURINamingNoArtifactInTheBucketIsCorrupt(t *testing.T) {
	for _, tc := range []struct {
		name       string
		uri        string
		invalidRef bool
	}{
		{"another bucket", "s3://elsewhere/tenants/default/topologies/r1/topology.json.gz", false},
		{"bare key", "tenants/default/topologies/r1/topology.json.gz", false},
		{"empty", "", true},
		{"bucket without a key", "s3://continuo", true},
		{"bucket with an empty key", "s3://continuo/", true},
		{"another scheme", "https://minio:9000/continuo/tenants/default/topologies/r1/topology.json.gz", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := objectstoretest.NewMemory(bucket)

			_, err := NewTopologyArtifactStore(mem, bucket, 4).Load(context.Background(),
				release.TopologyRef{URI: tc.uri, SHA256: "x"})
			assert.ErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
			assert.Equal(t, tc.invalidRef, errors.Is(err, objectstore.ErrInvalidRef))
			assert.Empty(t, mem.Reads())
		})
	}
}

func TestTopologyArtifactStore_UnreachableStoreIsInfrastructure(t *testing.T) {
	mem := objectstoretest.NewMemory(bucket)
	mem.FailGet(r1Ref, outage{})
	_, err := NewTopologyArtifactStore(mem, bucket, 4).Load(context.Background(),
		release.TopologyRef{URI: r1Ref.String(), SHA256: "x"})
	require.Error(t, err)
	assert.False(t, errors.Is(err, ports.ErrTopologyArtifactNotFound) || errors.Is(err, ports.ErrTopologyArtifactCorrupt))
	assert.Equal(t, pkgredis.ClassInfrastructure, pkgredis.Classify(err))
}

func TestTopologyArtifactStore_CacheServesRepeatLoadsAndKeysOnTheChecksum(t *testing.T) {
	mem := objectstoretest.NewMemory(bucket)
	writer := NewTopologyArtifactStore(mem, bucket, 4)
	ref, err := writer.Write(context.Background(), "r1", sampleTopology())
	require.NoError(t, err)

	// Write reads the key to enforce immutability; count only the reads the
	// Loads below drive.
	before := len(mem.Reads())

	store := NewTopologyArtifactStore(mem, bucket, 4)
	_, err = store.Load(context.Background(), ref)
	require.NoError(t, err)
	_, err = store.Load(context.Background(), ref)
	require.NoError(t, err)
	assert.Equal(t, 1, len(mem.Reads())-before, "the second load is served from the cache")

	other := ref
	other.SHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
	_, err = store.Load(context.Background(), other)
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactCorrupt, "a cached entry never answers for another checksum")
	assert.Equal(t, 2, len(mem.Reads())-before)
}

func TestTopologyArtifactStore_WritePrimesTheCache(t *testing.T) {
	mem := objectstoretest.NewMemory(bucket)
	store := NewTopologyArtifactStore(mem, bucket, 4)
	ref, err := store.Write(context.Background(), "r1", sampleTopology())
	require.NoError(t, err)
	// Write reads the key to enforce immutability; count only the reads the
	// Load below drives, which the primed cache serves.
	before := len(mem.Reads())
	_, err = store.Load(context.Background(), ref)
	require.NoError(t, err)
	assert.Equal(t, before, len(mem.Reads()))
}

// A returned topology shares no memory with the cached one: a caller that
// edits a node's slice (appending to or overwriting an upstream id) must not
// change what the next Load returns, whether the first Load read the object or
// was served from the cache.
func TestTopologyArtifactStore_LoadReturnsACopy(t *testing.T) {
	store := NewTopologyArtifactStore(objectstoretest.NewMemory(bucket), bucket, 4)
	ref, err := store.Write(context.Background(), "r1", sampleTopology())
	require.NoError(t, err)

	first, err := store.Load(context.Background(), ref)
	require.NoError(t, err)
	orders := indexOf(t, first, "svc.orders")
	first[orders].ContentHash = "mutated"
	first[orders].UpstreamUniqueIDs[0] = "mutated"
	first[orders].UpstreamUniqueIDs = append(first[orders].UpstreamUniqueIDs[:1], "extra")

	second, err := store.Load(context.Background(), ref)
	require.NoError(t, err)
	assert.Equal(t, byUniqueID(sampleTopology()), byUniqueID(second), "a caller editing its topology must not change the cached one")

	second[orders].UpstreamUniqueIDs[0] = "mutated again"
	third, err := store.Load(context.Background(), ref)
	require.NoError(t, err)
	assert.Equal(t, []string{"svc.customers"}, third[orders].UpstreamUniqueIDs, "a topology served from the cache is a copy too")
}

// The first Load after a cold start decodes the object and returns that
// topology while caching it; editing the returned slices must not reach the
// cached entry.
func TestTopologyArtifactStore_FirstLoadReturnsACopyOfWhatItCaches(t *testing.T) {
	mem := objectstoretest.NewMemory(bucket)
	ref, err := NewTopologyArtifactStore(mem, bucket, 4).Write(context.Background(), "r1", sampleTopology())
	require.NoError(t, err)

	// Write reads the key to enforce immutability; count only the reads the
	// Loads below drive.
	before := len(mem.Reads())

	store := NewTopologyArtifactStore(mem, bucket, 4)
	first, err := store.Load(context.Background(), ref)
	require.NoError(t, err)
	orders := indexOf(t, first, "svc.orders")
	first[orders].UpstreamUniqueIDs[0] = "mutated"

	second, err := store.Load(context.Background(), ref)
	require.NoError(t, err)
	assert.Equal(t, []string{"svc.customers"}, second[orders].UpstreamUniqueIDs)
	assert.Equal(t, 1, len(mem.Reads())-before)
}

// Two nodes may claim the same unique_id; release-controller's duplicate gate
// needs to see both, so the store hands back every node of the artifact.
func TestTopologyArtifactStore_LoadKeepsNodesSharingAUniqueID(t *testing.T) {
	topo := release.Topology{
		{UniqueID: "svc.orders", SchemaName: "svc", TableName: "orders", ServiceName: "a", NodeType: "dbt-model", UpstreamUniqueIDs: []string{}},
		{UniqueID: "svc.orders", SchemaName: "svc", TableName: "orders", ServiceName: "b", NodeType: "dbt-model", UpstreamUniqueIDs: []string{}},
	}
	mem := objectstoretest.NewMemory(bucket)
	writer := NewTopologyArtifactStore(mem, bucket, 4)
	ref, err := writer.Write(context.Background(), "r1", topo)
	require.NoError(t, err)

	fromObject, err := NewTopologyArtifactStore(mem, bucket, 4).Load(context.Background(), ref)
	require.NoError(t, err)
	require.Len(t, fromObject, 2)
	assert.ElementsMatch(t, []string{"a", "b"}, []string{fromObject[0].ServiceName, fromObject[1].ServiceName})

	fromCache, err := writer.Load(context.Background(), ref)
	require.NoError(t, err)
	assert.Len(t, fromCache, 2)
}

func indexOf(t *testing.T, topo release.Topology, uniqueID string) int {
	t.Helper()
	for i, n := range topo {
		if n.UniqueID == uniqueID {
			return i
		}
	}
	require.FailNow(t, "node not in topology", uniqueID)
	return -1
}

func TestTopologyArtifactStore_CacheEvictsTheLeastRecentlyUsed(t *testing.T) {
	mem := objectstoretest.NewMemory(bucket)
	writer := NewTopologyArtifactStore(mem, bucket, 0)
	refA, err := writer.Write(context.Background(), "rA", sampleTopology())
	require.NoError(t, err)
	refB, err := writer.Write(context.Background(), "rB", sampleTopology())
	require.NoError(t, err)

	// Write reads the key to enforce immutability; count only the reads the
	// Loads below drive.
	before := len(mem.Reads())

	store := NewTopologyArtifactStore(mem, bucket, 1)
	for _, ref := range []release.TopologyRef{refA, refB, refA} {
		_, err := store.Load(context.Background(), ref)
		require.NoError(t, err)
	}
	assert.Equal(t, 3, len(mem.Reads())-before, "with room for one topology, loading B evicts A")
}

// cloneTopology must copy every reference-typed field of release.Node. A new
// slice, map or pointer field on the node fails here until the clone (and the
// expectation below) is extended, so the cache can never hand out a topology
// that aliases its entry.
func TestCloneTopology_CoversEveryReferenceField(t *testing.T) {
	var referenceFields []string
	nodeType := reflect.TypeOf(release.Node{})
	for i := 0; i < nodeType.NumField(); i++ {
		switch f := nodeType.Field(i); f.Type.Kind() {
		case reflect.Slice, reflect.Map, reflect.Pointer, reflect.Interface, reflect.Chan, reflect.Func:
			referenceFields = append(referenceFields, f.Name)
		}
	}
	assert.Equal(t, []string{"UpstreamUniqueIDs"}, referenceFields,
		"cloneTopology copies only UpstreamUniqueIDs; extend it for any other reference-typed field")
}
