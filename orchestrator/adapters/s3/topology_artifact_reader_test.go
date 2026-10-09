package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/service/ports"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeObjects serves object bodies from memory, keyed by "<bucket>/<key>", and
// counts the reads that reach it.
type fakeObjects struct {
	bodies map[string][]byte
	err    error
	reads  int
}

func (f *fakeObjects) get(_ context.Context, bucket, key string) ([]byte, error) {
	f.reads++
	if f.err != nil {
		return nil, f.err
	}
	body, ok := f.bodies[bucket+"/"+key]
	if !ok {
		return nil, fmt.Errorf("%w: s3://%s/%s", ports.ErrTopologyArtifactNotFound, bucket, key)
	}
	return body, nil
}

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

// storeArtifact encodes doc, stores it under its canonical key in bucket
// "continuo", and returns the URI and checksum a promotion would carry.
func storeArtifact(t *testing.T, objects *fakeObjects, doc topologyartifact.Document) (string, string) {
	t.Helper()
	gz, sum, err := topologyartifact.Encode(doc)
	require.NoError(t, err)
	key := topologyartifact.Key(doc.TenantID, doc.ReleaseID)
	if objects.bodies == nil {
		objects.bodies = map[string][]byte{}
	}
	objects.bodies["continuo/"+key] = gz
	return "s3://continuo/" + key, sum
}

func TestTopologyArtifactReader_LoadsAndVerifiesTheArtifact(t *testing.T) {
	objects := &fakeObjects{}
	uri, sum := storeArtifact(t, objects, artifactDoc("rel-1"))
	r := newTopologyArtifactReader(objects.get, "continuo", 8)

	doc, err := r.Load(context.Background(), uri, sum)
	require.NoError(t, err)
	assert.Equal(t, "rel-1", doc.ReleaseID)
	require.Len(t, doc.Nodes, 1)
	assert.Equal(t, "analytics.orders", doc.Nodes[0].UniqueID)
}

// An artifact never changes once written, so the second read of the same URI
// and checksum is served from memory.
func TestTopologyArtifactReader_ServesARepeatReadFromMemory(t *testing.T) {
	objects := &fakeObjects{}
	uri, sum := storeArtifact(t, objects, artifactDoc("rel-1"))
	r := newTopologyArtifactReader(objects.get, "continuo", 8)

	_, err := r.Load(context.Background(), uri, sum)
	require.NoError(t, err)
	_, err = r.Load(context.Background(), uri, sum)
	require.NoError(t, err)
	assert.Equal(t, 1, objects.reads)
}

// A promotion naming the right URI with the wrong checksum is not served from
// the cache: the object is read again and the mismatch is reported with both
// hashes, as a permanent error.
func TestTopologyArtifactReader_ChecksumMismatchIsCorruptAndNamesBothHashes(t *testing.T) {
	objects := &fakeObjects{}
	uri, sum := storeArtifact(t, objects, artifactDoc("rel-1"))
	r := newTopologyArtifactReader(objects.get, "continuo", 8)
	_, err := r.Load(context.Background(), uri, sum)
	require.NoError(t, err)

	const wrong = "0000000000000000000000000000000000000000000000000000000000000000"
	_, err = r.Load(context.Background(), uri, wrong)
	require.Error(t, err)
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
	assert.Contains(t, err.Error(), wrong, "the expected hash is named")
	assert.Contains(t, err.Error(), sum, "the object's actual hash is named")
	assert.Equal(t, 2, objects.reads)
}

// An object whose checksum matches but that is not a gzipped topology document
// is a producer defect no retry repairs.
func TestTopologyArtifactReader_UnreadableObjectIsCorrupt(t *testing.T) {
	body := []byte("not gzip")
	objects := &fakeObjects{bodies: map[string][]byte{"continuo/k": body}}
	r := newTopologyArtifactReader(objects.get, "continuo", 8)

	sum := sha256.Sum256(body)
	_, err := r.Load(context.Background(), "s3://continuo/k", hex.EncodeToString(sum[:]))
	require.Error(t, err)
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
	assert.NotContains(t, err.Error(), "expected sha256", "the checksum matched; the document is what is unreadable")
}

func TestTopologyArtifactReader_MissingObjectIsNotFound(t *testing.T) {
	r := newTopologyArtifactReader((&fakeObjects{}).get, "continuo", 8)

	_, err := r.Load(context.Background(), "s3://continuo/tenants/default/topologies/x/topology.json.gz", "ab")
	require.Error(t, err)
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactNotFound)
	assert.NotErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
}

// An outage must reach the stream consumer with its cause intact, so pkg/redis
// pauses the consumer instead of spending a delivery.
func TestTopologyArtifactReader_InfrastructureErrorKeepsItsCause(t *testing.T) {
	outage := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	r := newTopologyArtifactReader((&fakeObjects{err: fmt.Errorf("get topology artifact: %w", outage)}).get, "continuo", 8)

	_, err := r.Load(context.Background(), "s3://continuo/k", "ab")
	require.Error(t, err)
	var opErr *net.OpError
	assert.True(t, errors.As(err, &opErr))
	assert.NotErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
	assert.NotErrorIs(t, err, ports.ErrTopologyArtifactNotFound)
	assert.Equal(t, pkgredis.ClassInfrastructure, pkgredis.Classify(err))
}

func TestTopologyArtifactReader_EvictsTheLeastRecentlyUsedDocument(t *testing.T) {
	objects := &fakeObjects{}
	uriA, sumA := storeArtifact(t, objects, artifactDoc("rel-a"))
	uriB, sumB := storeArtifact(t, objects, artifactDoc("rel-b"))
	uriC, sumC := storeArtifact(t, objects, artifactDoc("rel-c"))
	r := newTopologyArtifactReader(objects.get, "continuo", 2)
	ctx := context.Background()

	for _, l := range []struct{ uri, sum string }{{uriA, sumA}, {uriB, sumB}, {uriC, sumC}} {
		_, err := r.Load(ctx, l.uri, l.sum)
		require.NoError(t, err)
	}
	_, err := r.Load(ctx, uriA, sumA)
	require.NoError(t, err)
	assert.Equal(t, 4, objects.reads, "rel-a was evicted by rel-c and is read again")
}

// "s3://bucket" names no object. That is a producer defect no retry repairs.
func TestTopologyArtifactReader_URIWithoutKeyIsCorrupt(t *testing.T) {
	objects := &fakeObjects{}
	r := newTopologyArtifactReader(objects.get, "continuo", 8)

	_, err := r.Load(context.Background(), "s3://continuo", "ab")
	require.Error(t, err)
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
	assert.Zero(t, objects.reads)
}

func TestTopologyArtifactReader_BareKeyUsesTheDefaultBucket(t *testing.T) {
	objects := &fakeObjects{}
	_, sum := storeArtifact(t, objects, artifactDoc("rel-1"))
	r := newTopologyArtifactReader(objects.get, "continuo", 8)

	_, err := r.Load(context.Background(), topologyartifact.Key("default", "rel-1"), sum)
	require.NoError(t, err)
}
