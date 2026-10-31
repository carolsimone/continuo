package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
)

func artifactDoc(releaseID string) topologyartifact.Document {
	return topologyartifact.Document{
		SchemaVersion: topologyartifact.SchemaVersion,
		TenantID:      "default",
		ReleaseID:     releaseID,
		Nodes: []topologyartifact.Node{{
			UniqueID: "analytics.orders", SchemaName: "analytics", TableName: "orders",
			ServiceName: "svc", NodeType: "dbt-model", ContentHash: "sha256:o",
			ImageTag: "img:1", Schedule: "daily", UpstreamUniqueIDs: []string{},
		}},
	}
}

// storeArtifact encodes doc, stores it under its canonical key in the default
// bucket, and returns the URI and checksum a promotion would carry.
func storeArtifact(t *testing.T, mem *objectstoretest.Memory, doc topologyartifact.Document) (string, string) {
	t.Helper()
	gz, sum, err := topologyartifact.Encode(doc)
	require.NoError(t, err)
	ref := objectstore.Ref{Bucket: defaultBucket, Key: topologyartifact.Key(doc.TenantID, doc.ReleaseID)}
	require.NoError(t, mem.Put(context.Background(), ref, gz, "application/gzip"))
	return ref.String(), sum
}

func TestTopologyArtifactReader_LoadsAndVerifiesTheArtifact(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	uri, sum := storeArtifact(t, mem, artifactDoc("rel-1"))
	r := newTopologyArtifactReader(mem, 8)

	doc, err := r.Load(context.Background(), uri, sum)
	require.NoError(t, err)
	assert.Equal(t, "rel-1", doc.ReleaseID)
	require.Len(t, doc.Nodes, 1)
	assert.Equal(t, "analytics.orders", doc.Nodes[0].UniqueID)
	reads := mem.Reads()
	require.Len(t, reads, 1)
	assert.Equal(t, int64(topologyartifact.MaxObjectBytes), reads[0].MaxBytes, "the read is capped at the artifact size limit")
}

// An artifact never changes once written, so the second read of the same URI
// and checksum is served from memory.
func TestTopologyArtifactReader_ServesARepeatReadFromMemory(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	uri, sum := storeArtifact(t, mem, artifactDoc("rel-1"))
	r := newTopologyArtifactReader(mem, 8)

	_, err := r.Load(context.Background(), uri, sum)
	require.NoError(t, err)
	_, err = r.Load(context.Background(), uri, sum)
	require.NoError(t, err)
	assert.Len(t, mem.Reads(), 1)
}

// A promotion naming the right URI with the wrong checksum is not served from
// the cache: the object is read again and the mismatch is reported with both
// hashes, as a permanent error.
func TestTopologyArtifactReader_ChecksumMismatchIsCorruptAndNamesBothHashes(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	uri, sum := storeArtifact(t, mem, artifactDoc("rel-1"))
	r := newTopologyArtifactReader(mem, 8)
	_, err := r.Load(context.Background(), uri, sum)
	require.NoError(t, err)

	const wrong = "0000000000000000000000000000000000000000000000000000000000000000"
	_, err = r.Load(context.Background(), uri, wrong)
	require.Error(t, err)
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
	assert.Contains(t, err.Error(), wrong, "the expected hash is named")
	assert.Contains(t, err.Error(), sum, "the object's actual hash is named")
	assert.Len(t, mem.Reads(), 2)
}

// An object whose checksum matches but that is not a gzipped topology document
// is a producer defect no retry repairs.
func TestTopologyArtifactReader_UnreadableObjectIsCorrupt(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	body := []byte("not gzip")
	require.NoError(t, mem.Put(context.Background(), objectstore.Ref{Bucket: defaultBucket, Key: "k"}, body, "application/gzip"))
	r := newTopologyArtifactReader(mem, 8)

	sum := sha256.Sum256(body)
	_, err := r.Load(context.Background(), "s3://continuo/k", hex.EncodeToString(sum[:]))
	require.Error(t, err)
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
	assert.NotContains(t, err.Error(), "expected sha256", "the checksum matched; the document is what is unreadable")
}

func TestTopologyArtifactReader_MissingObjectIsNotFound(t *testing.T) {
	r := newTopologyArtifactReader(objectstoretest.NewMemory(defaultBucket), 8)

	_, err := r.Load(context.Background(), "s3://continuo/tenants/default/topologies/x/topology.json.gz", "ab")
	require.Error(t, err)
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactNotFound)
	assert.NotErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
}

// An object over the size limit will not shrink on redelivery: it is corrupt,
// a permanent failure.
func TestTopologyArtifactReader_OversizedObjectIsCorrupt(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	ref := objectstore.Ref{Bucket: defaultBucket, Key: "k"}
	mem.FailGet(ref, fmt.Errorf("%w: %s is over the limit", objectstore.ErrTooLarge, ref))

	_, err := newTopologyArtifactReader(mem, 8).Load(context.Background(), ref.String(), "ab")
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
	assert.NotErrorIs(t, err, ports.ErrTopologyArtifactNotFound)
}

// An outage must reach the stream consumer with its cause intact, so pkg/redis
// pauses the consumer instead of spending a delivery.
func TestTopologyArtifactReader_InfrastructureErrorKeepsItsCause(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	outage := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	mem.FailGet(objectstore.Ref{Bucket: defaultBucket, Key: "k"}, fmt.Errorf("get s3://continuo/k: %w", outage))

	_, err := newTopologyArtifactReader(mem, 8).Load(context.Background(), "s3://continuo/k", "ab")
	require.Error(t, err)
	var opErr *net.OpError
	assert.True(t, errors.As(err, &opErr))
	assert.NotErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
	assert.NotErrorIs(t, err, ports.ErrTopologyArtifactNotFound)
	assert.Equal(t, pkgredis.ClassInfrastructure, pkgredis.Classify(err))
}

func TestTopologyArtifactReader_EvictsTheLeastRecentlyUsedDocument(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	uriA, sumA := storeArtifact(t, mem, artifactDoc("rel-a"))
	uriB, sumB := storeArtifact(t, mem, artifactDoc("rel-b"))
	uriC, sumC := storeArtifact(t, mem, artifactDoc("rel-c"))
	r := newTopologyArtifactReader(mem, 2)
	ctx := context.Background()

	for _, l := range []struct{ uri, sum string }{{uriA, sumA}, {uriB, sumB}, {uriC, sumC}} {
		_, err := r.Load(ctx, l.uri, l.sum)
		require.NoError(t, err)
	}
	_, err := r.Load(ctx, uriA, sumA)
	require.NoError(t, err)
	assert.Len(t, mem.Reads(), 4, "rel-a was evicted by rel-c and is read again")
}

// A URI that names no object is a producer defect no retry repairs: it is
// corrupt, and refused before any read.
func TestTopologyArtifactReader_URIThatNamesNoObjectIsCorruptWithoutARead(t *testing.T) {
	for _, tc := range []struct{ name, uri string }{
		{"empty", ""},
		{"bucket without a key", "s3://continuo"},
		{"bucket with an empty key", "s3://continuo/"},
		{"another scheme", "https://minio:9000/continuo/k"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := objectstoretest.NewMemory(defaultBucket)

			_, err := newTopologyArtifactReader(mem, 8).Load(context.Background(), tc.uri, "ab")
			assert.ErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
			assert.ErrorIs(t, err, objectstore.ErrInvalidRef)
			assert.Empty(t, mem.Reads())
		})
	}
}

func TestTopologyArtifactReader_BareKeyUsesTheDefaultBucket(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	_, sum := storeArtifact(t, mem, artifactDoc("rel-1"))
	r := newTopologyArtifactReader(mem, 8)

	_, err := r.Load(context.Background(), topologyartifact.Key("default", "rel-1"), sum)
	require.NoError(t, err)
	assert.Equal(t, defaultBucket, mem.Reads()[0].Ref.Bucket)
}
