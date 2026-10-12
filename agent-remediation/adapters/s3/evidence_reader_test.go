package s3

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
)

// evidenceRef is the evidence object the unreadable-evidence cases read.
var evidenceRef = objectstore.Ref{Bucket: testBucket, Key: "logs/a.log"}

func TestEvidenceReader_ReadsTheObjectAnS3URINames(t *testing.T) {
	ctx := context.Background()
	mem := objectstoretest.NewMemory(testBucket)
	ref := objectstore.Ref{Bucket: "evidence-bucket", Key: "candidate-sql/rel-1/candidate_model.orders.sql"}
	require.NoError(t, mem.Put(ctx, ref, []byte("select 1"), "text/plain"))

	got, err := NewEvidenceReader(mem).Fetch(ctx, ref.String())
	require.NoError(t, err)
	assert.Equal(t, "select 1", got)
	assert.Equal(t, []objectstoretest.Read{{Ref: ref, MaxBytes: maxEvidenceBytes}}, mem.Reads(),
		"the URI's bucket is read, capped at the evidence ceiling")
}

func TestEvidenceReader_BareKeyReadsTheDefaultBucket(t *testing.T) {
	ctx := context.Background()
	mem := objectstoretest.NewMemory(testBucket)
	require.NoError(t, mem.Put(ctx, evidenceRef, []byte("log"), "text/plain"))

	got, err := NewEvidenceReader(mem).Fetch(ctx, evidenceRef.Key)
	require.NoError(t, err)
	assert.Equal(t, "log", got)
}

// Evidence that cannot be read for good is ports.ErrNotFound, and the fixer
// works without it: a URI that names no object (refused before any read), a
// missing object, and an object over the read ceiling.
func TestEvidenceReader_UnreadableEvidenceIsNotFound(t *testing.T) {
	for _, tc := range []struct {
		name  string
		uri   string
		setup func(*objectstoretest.Memory)
		reads int
	}{
		{"empty uri", "", func(*objectstoretest.Memory) {}, 0},
		{"bucket without a key", "s3://test-bucket", func(*objectstoretest.Memory) {}, 0},
		{"another scheme", "https://minio:9000/test-bucket/logs/a.log", func(*objectstoretest.Memory) {}, 0},
		{"missing object", evidenceRef.String(), func(*objectstoretest.Memory) {}, 1},
		{"object over the ceiling", evidenceRef.String(), func(m *objectstoretest.Memory) {
			m.FailGet(evidenceRef, fmt.Errorf("%w: %s is over the limit", objectstore.ErrTooLarge, evidenceRef))
		}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := objectstoretest.NewMemory(testBucket)
			tc.setup(mem)

			_, err := NewEvidenceReader(mem).Fetch(context.Background(), tc.uri)
			assert.ErrorIs(t, err, ports.ErrNotFound)
			assert.Len(t, mem.Reads(), tc.reads)
		})
	}
}

// A failure other than a missing or oversized object is not flattened into
// ErrNotFound, so the caller does not proceed without evidence that exists.
func TestEvidenceReader_OtherFailuresAreNotFlattened(t *testing.T) {
	mem := objectstoretest.NewMemory(testBucket)
	mem.FailGet(evidenceRef, statusError{code: http.StatusServiceUnavailable})

	_, err := NewEvidenceReader(mem).Fetch(context.Background(), evidenceRef.String())
	require.Error(t, err)
	assert.False(t, errors.Is(err, ports.ErrNotFound))
	var status interface{ HTTPStatusCode() int }
	require.True(t, errors.As(err, &status))
	assert.Equal(t, http.StatusServiceUnavailable, status.HTTPStatusCode())
}
