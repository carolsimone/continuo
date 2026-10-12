package s3

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
	"github.com/carolsimone/continuo/remediation/service/ports"
)

// defaultBucket is the default bucket of every store these tests build.
const defaultBucket = "continuo"

// logRef is the log object the unavailable-log cases read.
var logRef = objectstore.Ref{Bucket: defaultBucket, Key: "logs/a/b.log"}

// statusError is a store error carrying an HTTP status, as the AWS SDK's
// response errors do.
type statusError struct{ code int }

func (e statusError) Error() string       { return fmt.Sprintf("s3 answered HTTP %d", e.code) }
func (e statusError) HTTPStatusCode() int { return e.code }

func TestLogReader_ReadsTheLogAnS3URINames(t *testing.T) {
	ctx := context.Background()
	mem := objectstoretest.NewMemory(defaultBucket)
	ref := objectstore.Ref{Bucket: "logs-bucket", Key: "logs/a/b.log"}
	require.NoError(t, mem.Put(ctx, ref, []byte("Database Error in model orders"), "text/plain"))

	got, err := NewLogReader(mem).Fetch(ctx, "s3://logs-bucket/logs/a/b.log")
	require.NoError(t, err)
	assert.Equal(t, "Database Error in model orders", got)
	assert.Equal(t, []objectstoretest.Read{{Ref: ref, MaxBytes: maxLogBytes}}, mem.Reads(),
		"the URI's bucket is read, capped at the log ceiling")
}

func TestLogReader_BareKeyReadsTheDefaultBucket(t *testing.T) {
	ctx := context.Background()
	mem := objectstoretest.NewMemory(defaultBucket)
	require.NoError(t, mem.Put(ctx, logRef, []byte("log"), "text/plain"))

	got, err := NewLogReader(mem).Fetch(ctx, logRef.Key)
	require.NoError(t, err)
	assert.Equal(t, "log", got)
}

// The classifier compares with ==, so every log that cannot be read for good
// is the bare sentinel: a URI that names no object (refused before any read),
// a missing object, and an object over the read ceiling.
func TestLogReader_UnavailableLogIsTheBareNotFoundSentinel(t *testing.T) {
	for _, tc := range []struct {
		name  string
		uri   string
		setup func(*objectstoretest.Memory)
		reads int
	}{
		{"empty uri", "", func(*objectstoretest.Memory) {}, 0},
		{"blank uri", "  ", func(*objectstoretest.Memory) {}, 0},
		{"bucket without a key", "s3://continuo", func(*objectstoretest.Memory) {}, 0},
		{"another scheme", "https://minio:9000/continuo/logs/a/b.log", func(*objectstoretest.Memory) {}, 0},
		{"missing object", logRef.String(), func(*objectstoretest.Memory) {}, 1},
		{"log over the ceiling", logRef.String(), func(m *objectstoretest.Memory) {
			m.FailGet(logRef, fmt.Errorf("%w: %s is over the limit", objectstore.ErrTooLarge, logRef))
		}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := objectstoretest.NewMemory(defaultBucket)
			tc.setup(mem)

			_, err := NewLogReader(mem).Fetch(context.Background(), tc.uri)
			assert.Same(t, ports.ErrLogNotFound, err)
			assert.Len(t, mem.Reads(), tc.reads)
		})
	}
}

// Any other failure is not flattened into not-found: it reaches the handler
// with its cause, and the message is retried.
func TestLogReader_OtherFailuresKeepTheirCause(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	mem.FailGet(logRef, statusError{code: http.StatusServiceUnavailable})

	_, err := NewLogReader(mem).Fetch(context.Background(), logRef.String())
	require.Error(t, err)
	assert.NotErrorIs(t, err, ports.ErrLogNotFound)
	var status interface{ HTTPStatusCode() int }
	require.True(t, errors.As(err, &status))
	assert.Equal(t, http.StatusServiceUnavailable, status.HTTPStatusCode())
}
