package s3

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeObjects is an in-memory ObjectStore counting reads.
type fakeObjects struct {
	mu      sync.Mutex
	objects map[string][]byte
	types   map[string]string
	gets    int
	getErr  error
}

func newFakeObjects() *fakeObjects {
	return &fakeObjects{objects: map[string][]byte{}, types: map[string]string{}}
}

func (f *fakeObjects) GetObject(_ context.Context, key string, maxBytes int64) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	if f.getErr != nil {
		return nil, f.getErr
	}
	body, ok := f.objects[key]
	if !ok {
		return nil, ErrObjectNotFound
	}
	if int64(len(body)) > maxBytes {
		return nil, ErrObjectTooLarge
	}
	return append([]byte(nil), body...), nil
}

func (f *fakeObjects) PutObject(_ context.Context, key string, body []byte, contentType string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = append([]byte(nil), body...)
	f.types[key] = contentType
	return nil
}

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

func TestTopologyArtifactStore_WriteThenLoadRoundTrips(t *testing.T) {
	objects := newFakeObjects()
	ref, err := NewTopologyArtifactStore(objects, "continuo", 4).Write(context.Background(), "r1", sampleTopology())
	require.NoError(t, err)
	assert.Equal(t, "s3://continuo/tenants/default/topologies/r1/topology.json.gz", ref.URI)
	assert.Len(t, ref.SHA256, 64)
	assert.Equal(t, 4, ref.NodeCount)
	assert.Equal(t, "application/gzip", objects.types["tenants/default/topologies/r1/topology.json.gz"])

	// A second store has an empty cache, so this reads the object back.
	got, err := NewTopologyArtifactStore(objects, "continuo", 4).Load(context.Background(), ref)
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
	objects := newFakeObjects()
	store := NewTopologyArtifactStore(objects, "continuo", 4)
	const key = "tenants/default/topologies/r1/topology.json.gz"

	first, err := store.Write(context.Background(), "r1", sampleTopology())
	require.NoError(t, err)
	firstBytes := append([]byte(nil), objects.objects[key]...)

	second, err := NewTopologyArtifactStore(objects, "continuo", 4).Write(context.Background(), "r1", sampleTopology())
	require.NoError(t, err)

	assert.Equal(t, first, second)
	assert.Equal(t, firstBytes, objects.objects[key])
}

func TestTopologyArtifactStore_LoadOfAMissingObjectIsNotFound(t *testing.T) {
	_, err := NewTopologyArtifactStore(newFakeObjects(), "continuo", 4).Load(context.Background(),
		release.TopologyRef{URI: "s3://continuo/tenants/default/topologies/gone/topology.json.gz", SHA256: "x"})
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactNotFound)
}

func TestTopologyArtifactStore_LoadWithAnotherChecksumIsCorrupt(t *testing.T) {
	objects := newFakeObjects()
	ref, err := NewTopologyArtifactStore(objects, "continuo", 4).Write(context.Background(), "r1", sampleTopology())
	require.NoError(t, err)

	ref.SHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
	_, err = NewTopologyArtifactStore(objects, "continuo", 4).Load(context.Background(), ref)
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
	assert.ErrorIs(t, err, topologyartifact.ErrChecksumMismatch)
}

func TestTopologyArtifactStore_LoadOutsideTheBucketIsCorrupt(t *testing.T) {
	_, err := NewTopologyArtifactStore(newFakeObjects(), "continuo", 4).Load(context.Background(),
		release.TopologyRef{URI: "s3://elsewhere/tenants/default/topologies/r1/topology.json.gz", SHA256: "x"})
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
}

func TestTopologyArtifactStore_UnreachableStoreIsInfrastructure(t *testing.T) {
	objects := newFakeObjects()
	objects.getErr = outage{}
	_, err := NewTopologyArtifactStore(objects, "continuo", 4).Load(context.Background(),
		release.TopologyRef{URI: "s3://continuo/tenants/default/topologies/r1/topology.json.gz", SHA256: "x"})
	require.Error(t, err)
	assert.False(t, errors.Is(err, ports.ErrTopologyArtifactNotFound) || errors.Is(err, ports.ErrTopologyArtifactCorrupt))
	assert.Equal(t, pkgredis.ClassInfrastructure, pkgredis.Classify(err))
}

func TestTopologyArtifactStore_CacheServesRepeatLoadsAndKeysOnTheChecksum(t *testing.T) {
	objects := newFakeObjects()
	writer := NewTopologyArtifactStore(objects, "continuo", 4)
	ref, err := writer.Write(context.Background(), "r1", sampleTopology())
	require.NoError(t, err)

	store := NewTopologyArtifactStore(objects, "continuo", 4)
	_, err = store.Load(context.Background(), ref)
	require.NoError(t, err)
	_, err = store.Load(context.Background(), ref)
	require.NoError(t, err)
	assert.Equal(t, 1, objects.gets, "the second load is served from the cache")

	other := ref
	other.SHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
	_, err = store.Load(context.Background(), other)
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactCorrupt, "a cached entry never answers for another checksum")
	assert.Equal(t, 2, objects.gets)
}

func TestTopologyArtifactStore_WritePrimesTheCache(t *testing.T) {
	objects := newFakeObjects()
	store := NewTopologyArtifactStore(objects, "continuo", 4)
	ref, err := store.Write(context.Background(), "r1", sampleTopology())
	require.NoError(t, err)
	_, err = store.Load(context.Background(), ref)
	require.NoError(t, err)
	assert.Equal(t, 0, objects.gets)
}

// A returned topology shares no memory with the cached one: a caller that
// edits a node's slice (appending to or overwriting an upstream id) must not
// change what the next Load returns, whether the first Load read the object or
// was served from the cache.
func TestTopologyArtifactStore_LoadReturnsACopy(t *testing.T) {
	store := NewTopologyArtifactStore(newFakeObjects(), "continuo", 4)
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
	objects := newFakeObjects()
	ref, err := NewTopologyArtifactStore(objects, "continuo", 4).Write(context.Background(), "r1", sampleTopology())
	require.NoError(t, err)

	store := NewTopologyArtifactStore(objects, "continuo", 4)
	first, err := store.Load(context.Background(), ref)
	require.NoError(t, err)
	orders := indexOf(t, first, "svc.orders")
	first[orders].UpstreamUniqueIDs[0] = "mutated"

	second, err := store.Load(context.Background(), ref)
	require.NoError(t, err)
	assert.Equal(t, []string{"svc.customers"}, second[orders].UpstreamUniqueIDs)
	assert.Equal(t, 1, objects.gets)
}

// Two nodes may claim the same unique_id; release-controller's duplicate gate
// needs to see both, so the store hands back every node of the artifact.
func TestTopologyArtifactStore_LoadKeepsNodesSharingAUniqueID(t *testing.T) {
	topo := release.Topology{
		{UniqueID: "svc.orders", SchemaName: "svc", TableName: "orders", ServiceName: "a", NodeType: "dbt-model", UpstreamUniqueIDs: []string{}},
		{UniqueID: "svc.orders", SchemaName: "svc", TableName: "orders", ServiceName: "b", NodeType: "dbt-model", UpstreamUniqueIDs: []string{}},
	}
	objects := newFakeObjects()
	writer := NewTopologyArtifactStore(objects, "continuo", 4)
	ref, err := writer.Write(context.Background(), "r1", topo)
	require.NoError(t, err)

	fromObject, err := NewTopologyArtifactStore(objects, "continuo", 4).Load(context.Background(), ref)
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
	objects := newFakeObjects()
	writer := NewTopologyArtifactStore(objects, "continuo", 0)
	refA, err := writer.Write(context.Background(), "rA", sampleTopology())
	require.NoError(t, err)
	refB, err := writer.Write(context.Background(), "rB", sampleTopology())
	require.NoError(t, err)

	store := NewTopologyArtifactStore(objects, "continuo", 1)
	for _, ref := range []release.TopologyRef{refA, refB, refA} {
		_, err := store.Load(context.Background(), ref)
		require.NoError(t, err)
	}
	assert.Equal(t, 3, objects.gets, "with room for one topology, loading B evicts A")
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
		case reflect.Slice, reflect.Map, reflect.Ptr, reflect.Interface, reflect.Chan, reflect.Func:
			referenceFields = append(referenceFields, f.Name)
		}
	}
	assert.Equal(t, []string{"UpstreamUniqueIDs"}, referenceFields,
		"cloneTopology copies only UpstreamUniqueIDs; extend it for any other reference-typed field")
}
