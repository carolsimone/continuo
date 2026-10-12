package s3

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
)

// defaultBucket is the default bucket of every store these tests build.
const defaultBucket = "continuo"

func TestLogUploader_PutsTheLogAsTextInTheDefaultBucket(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)

	require.NoError(t, NewLogUploader(mem).UploadLog(context.Background(),
		"logs/task-executions/svc/analytics/orders/e1.log", "line one\nline two\n"))

	assert.Equal(t, []objectstoretest.Write{{
		Ref:         objectstore.Ref{Bucket: defaultBucket, Key: "logs/task-executions/svc/analytics/orders/e1.log"},
		Body:        []byte("line one\nline two\n"),
		ContentType: "text/plain",
	}}, mem.Puts())
}

// A refused upload is returned with its cause and the key; the handler logs
// it and continues without the log.
func TestLogUploader_ReturnsARefusedUpload(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	refused := errors.New("access denied")
	key := "run-results/task-executions/svc/analytics/orders/e1.json"
	mem.FailPut(objectstore.Ref{Key: key}, refused)

	err := NewLogUploader(mem).UploadLog(context.Background(), key, `{"status":"success"}`)
	require.ErrorIs(t, err, refused)
	assert.Contains(t, err.Error(), key)
	assert.Empty(t, mem.Puts())
}

// The handler builds every key; an empty one is refused by the store before
// any write and reaches the handler as a failed upload.
func TestLogUploader_RefusesAKeyThatNamesNoObjectWithoutAWrite(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)

	err := NewLogUploader(mem).UploadLog(context.Background(), "", "log")
	require.ErrorIs(t, err, objectstore.ErrInvalidRef)
	assert.Empty(t, mem.Puts())
}
