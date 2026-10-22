package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// defaultBucket is the in-memory store's configured bucket: the one a
// reference without a bucket names, as the shared client reads its own.
const defaultBucket = "continuo"

// countingReader passes every read through to an objectstore.Reader and
// records how many reached it, the last reference read and the size cap that
// read carried.
type countingReader struct {
	next     objectstore.Reader
	reads    int
	lastRef  objectstore.Ref
	maxBytes int64
}

var _ objectstore.Reader = (*countingReader)(nil)

func (c *countingReader) Get(ctx context.Context, ref objectstore.Ref, maxBytes int64) ([]byte, error) {
	c.reads++
	c.lastRef = ref
	c.maxBytes = maxBytes
	return c.next.Get(ctx, ref, maxBytes)
}

// newObjects returns an empty in-memory store and the counting reader over it
// that the reader under test reads through.
func newObjects() (*objectstoretest.Memory, *countingReader) {
	mem := objectstoretest.NewMemory(defaultBucket)
	return mem, &countingReader{next: mem}
}

// putObject stores body under key in the default bucket.
func putObject(t *testing.T, mem *objectstoretest.Memory, key string, body []byte) {
	t.Helper()
	ref := objectstore.Ref{Bucket: defaultBucket, Key: key}
	require.NoError(t, mem.Put(context.Background(), ref, body, "application/gzip"))
}

// statusError is a response error carrying an HTTP status, as the SDK's are.
type statusError struct{ code int }

func (e statusError) Error() string       { return fmt.Sprintf("s3 answered HTTP %d", e.code) }
func (e statusError) HTTPStatusCode() int { return e.code }

func artifactDoc(releaseID string) topologyartifact.Document {
	return topologyartifact.Document{
		SchemaVersion: topologyartifact.SchemaVersion,
		TenantID:      "default",
		ReleaseID:     releaseID,
		Nodes: []topologyartifact.Node{
			{UniqueID: "raw.orders", SchemaName: "raw", TableName: "orders", ServiceName: "svc", NodeType: "dbt-seed",
				ContentHash: "sha256:seed", ImageTag: "svc:1", Schedule: "daily", UpstreamUniqueIDs: []string{}},
			{UniqueID: "stg.orders", SchemaName: "stg", TableName: "orders", ServiceName: "svc", NodeType: "dbt-model",
				TestCount: 2, ContentHash: "sha256:model", ImageTag: "svc:1", Schedule: "hourly",
				TimeoutSeconds: 7200, UpstreamUniqueIDs: []string{"raw.orders"}},
		},
	}
}

// storeArtifact encodes doc, stores it under its canonical key in the default
// bucket, and returns the URI and checksum a promotion would carry.
func storeArtifact(t *testing.T, mem *objectstoretest.Memory, doc topologyartifact.Document) (string, string) {
	t.Helper()
	gz, sum, err := topologyartifact.Encode(doc)
	require.NoError(t, err)
	key := topologyartifact.Key(doc.TenantID, doc.ReleaseID)
	putObject(t, mem, key, gz)
	return "s3://" + defaultBucket + "/" + key, sum
}

func TestTopologyArtifactReader_ReturnsTheArtifactAsNodeSpecs(t *testing.T) {
	mem, objects := newObjects()
	uri, sum := storeArtifact(t, mem, artifactDoc("rel-1"))
	r := newTopologyArtifactReader(objects, 8)

	art, err := r.Load(context.Background(), uri, sum)
	require.NoError(t, err)
	assert.Equal(t, "default", art.TenantID)
	assert.Equal(t, "rel-1", art.ReleaseID)
	require.Len(t, art.Nodes, 2)
	two, timeout := 2, 7200
	assert.Equal(t, topology.NodeSpec{
		UniqueID: "stg.orders", ServiceName: "svc", SchemaName: "stg", TableName: "orders", NodeType: "dbt-model",
		TestCount: &two, ContentHash: "sha256:model", ImageTag: "svc:1", ScheduleName: "hourly",
		TimeoutSeconds: &timeout, UpstreamUniqueIDs: []string{"raw.orders"},
	}, art.Nodes[1])
	assert.Nil(t, art.Nodes[0].TimeoutSeconds, "a node that declares no timeout has none")
	require.NotNil(t, art.Nodes[0].TestCount, "the artifact always states a test count")
	assert.Equal(t, int64(topologyartifact.MaxObjectBytes), objects.maxBytes, "the read is capped at the artifact size limit")
}

// An artifact never changes once written, so a repeat read of the same URI and
// checksum is served from memory.
func TestTopologyArtifactReader_ServesARepeatReadFromMemory(t *testing.T) {
	mem, objects := newObjects()
	uri, sum := storeArtifact(t, mem, artifactDoc("rel-1"))
	r := newTopologyArtifactReader(objects, 8)

	_, err := r.Load(context.Background(), uri, sum)
	require.NoError(t, err)
	_, err = r.Load(context.Background(), uri, sum)
	require.NoError(t, err)
	assert.Equal(t, 1, objects.reads)
}

func TestTopologyArtifactReader_ChecksumMismatchIsCorruptAndNamesBothHashes(t *testing.T) {
	mem, objects := newObjects()
	uri, sum := storeArtifact(t, mem, artifactDoc("rel-1"))
	r := newTopologyArtifactReader(objects, 8)

	const wrong = "0000000000000000000000000000000000000000000000000000000000000000"
	_, err := r.Load(context.Background(), uri, wrong)
	require.Error(t, err)
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
	assert.Contains(t, err.Error(), "checksum mismatch")
	assert.Contains(t, err.Error(), wrong, "the expected hash is named")
	assert.Contains(t, err.Error(), sum, "the object's actual hash is named")
}

func TestTopologyArtifactReader_UnreadableObjectIsCorrupt(t *testing.T) {
	body := []byte("not gzip")
	mem, objects := newObjects()
	putObject(t, mem, "k", body)
	r := newTopologyArtifactReader(objects, 8)

	sum := sha256.Sum256(body)
	_, err := r.Load(context.Background(), "s3://"+defaultBucket+"/k", hex.EncodeToString(sum[:]))
	require.Error(t, err)
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
	assert.NotContains(t, err.Error(), "checksum mismatch")
}

// The store refuses an object over the cap the reader passes
// (topologyartifact.MaxObjectBytes) with objectstore.ErrTooLarge; an
// oversized artifact is a producer defect no retry repairs.
func TestTopologyArtifactReader_OversizedObjectIsCorrupt(t *testing.T) {
	mem, objects := newObjects()
	mem.FailGet(objectstore.Ref{Bucket: defaultBucket, Key: "k"},
		fmt.Errorf("%w: s3://continuo/k is over the limit", objectstore.ErrTooLarge))
	r := newTopologyArtifactReader(objects, 8)

	_, err := r.Load(context.Background(), "s3://continuo/k", "ab")
	require.Error(t, err)
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
	assert.NotErrorIs(t, err, ports.ErrTopologyArtifactNotFound)
}

// A missing object is retried: it may only be late.
func TestTopologyArtifactReader_MissingObjectIsNotFound(t *testing.T) {
	_, objects := newObjects()
	r := newTopologyArtifactReader(objects, 8)

	_, err := r.Load(context.Background(), "s3://continuo/tenants/default/topologies/x/topology.json.gz", "ab")
	require.Error(t, err)
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactNotFound)
	assert.NotErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
	assert.Equal(t, pkgredis.ClassTransient, pkgredis.Classify(err))
}

// An outage reaches the stream consumer with its cause intact, so pkg/redis
// pauses the consumer instead of spending a delivery.
func TestTopologyArtifactReader_OutageKeepsItsCause(t *testing.T) {
	cases := map[string]error{
		"connection refused": &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")},
		"HTTP 503":           statusError{code: 503},
	}
	for name, cause := range cases {
		t.Run(name, func(t *testing.T) {
			mem, objects := newObjects()
			mem.FailGet(objectstore.Ref{Bucket: defaultBucket, Key: "k"}, fmt.Errorf("get s3://continuo/k: %w", cause))
			r := newTopologyArtifactReader(objects, 8)

			_, err := r.Load(context.Background(), "s3://continuo/k", "ab")
			require.Error(t, err)
			assert.ErrorIs(t, err, cause)
			assert.NotErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
			assert.NotErrorIs(t, err, ports.ErrTopologyArtifactNotFound)
			assert.Equal(t, pkgredis.ClassInfrastructure, pkgredis.Classify(err))
		})
	}
}

func TestTopologyArtifactReader_EvictsTheLeastRecentlyUsedArtifact(t *testing.T) {
	mem, objects := newObjects()
	uriA, sumA := storeArtifact(t, mem, artifactDoc("rel-a"))
	uriB, sumB := storeArtifact(t, mem, artifactDoc("rel-b"))
	uriC, sumC := storeArtifact(t, mem, artifactDoc("rel-c"))
	r := newTopologyArtifactReader(objects, 2)
	ctx := context.Background()

	for _, l := range []struct{ uri, sum string }{{uriA, sumA}, {uriB, sumB}, {uriC, sumC}} {
		_, err := r.Load(ctx, l.uri, l.sum)
		require.NoError(t, err)
	}
	_, err := r.Load(ctx, uriA, sumA)
	require.NoError(t, err)
	assert.Equal(t, 4, objects.reads, "rel-a was evicted by rel-c and is read again")
}

// A URI that is not an object reference is a producer defect no retry
// repairs, and the reader refuses it before reading anything.
func TestTopologyArtifactReader_InvalidReferenceIsCorrupt(t *testing.T) {
	cases := map[string]string{
		"empty":                "",
		"bucket without a key": "s3://continuo",
		"another scheme":       "gs://continuo/k",
	}
	for name, uri := range cases {
		t.Run(name, func(t *testing.T) {
			_, objects := newObjects()
			r := newTopologyArtifactReader(objects, 8)

			_, err := r.Load(context.Background(), uri, "ab")
			require.Error(t, err)
			assert.ErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
			assert.NotErrorIs(t, err, ports.ErrTopologyArtifactNotFound)
			assert.Zero(t, objects.reads)
		})
	}
}

// A bare key leaves the bucket to the store, which reads its default bucket.
func TestTopologyArtifactReader_BareKeyUsesTheDefaultBucket(t *testing.T) {
	mem, objects := newObjects()
	_, sum := storeArtifact(t, mem, artifactDoc("rel-1"))
	r := newTopologyArtifactReader(objects, 8)
	key := topologyartifact.Key("default", "rel-1")

	_, err := r.Load(context.Background(), key, sum)
	require.NoError(t, err)
	assert.Equal(t, objectstore.Ref{Key: key}, objects.lastRef)
}

// The reader reports the release the artifact names, not the one its key
// suggests, so the consumer can refuse an artifact of another release.
func TestTopologyArtifactReader_ReportsTheReleaseTheArtifactNames(t *testing.T) {
	mem, objects := newObjects()
	gz, sum, err := topologyartifact.Encode(artifactDoc("rel-b"))
	require.NoError(t, err)
	key := topologyartifact.Key("default", "rel-a")
	putObject(t, mem, key, gz)
	r := newTopologyArtifactReader(objects, 8)

	art, err := r.Load(context.Background(), "s3://"+defaultBucket+"/"+key, sum)
	require.NoError(t, err)
	assert.Equal(t, "default", art.TenantID)
	assert.Equal(t, "rel-b", art.ReleaseID)
}
