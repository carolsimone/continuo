package s3

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCandidateSQLDeleter_DeletesThePrefixAndLogsNothing(t *testing.T) {
	ctx := context.Background()
	mem := objectstoretest.NewMemory(bucket)
	for _, key := range []string{"candidate-sql/r1/a.sql", "candidate-sql/r1/b.sql", "candidate-sql/r2/a.sql"} {
		require.NoError(t, mem.Put(ctx, objectstore.Ref{Key: key}, []byte("select 1"), "text/plain"))
	}
	var logs bytes.Buffer
	d := NewCandidateSQLDeleter(mem, slog.New(slog.NewTextHandler(&logs, nil)))

	require.NoError(t, d.DeletePrefix(ctx, "candidate-sql/r1/"))

	for _, key := range []string{"candidate-sql/r1/a.sql", "candidate-sql/r1/b.sql"} {
		_, err := mem.Get(ctx, objectstore.Ref{Key: key}, 1024)
		assert.ErrorIs(t, err, objectstore.ErrNotFound, key)
	}
	_, err := mem.Get(ctx, objectstore.Ref{Key: "candidate-sql/r2/a.sql"}, 1024)
	assert.NoError(t, err, "an object outside the prefix stays")
	assert.Empty(t, logs.String(), "a successful delete logs nothing")
}

// The prune that calls the deleter discards its error, so a failed delete is
// logged as a warning naming the prefix, and still returned.
func TestCandidateSQLDeleter_LogsAndReturnsAFailedDelete(t *testing.T) {
	mem := objectstoretest.NewMemory(bucket)
	mem.FailDeletePrefix("code-bundles/r1/", outage{})
	var logs bytes.Buffer
	d := NewCandidateSQLDeleter(mem, slog.New(slog.NewTextHandler(&logs, nil)))

	err := d.DeletePrefix(context.Background(), "code-bundles/r1/")
	require.ErrorIs(t, err, outage{})
	assert.Contains(t, logs.String(), "level=WARN")
	assert.Contains(t, logs.String(), "prefix=code-bundles/r1/")
}
