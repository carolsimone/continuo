package s3

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
)

// TestLogUploader_UploadsWithTheDefaultChain proves the wiring main uses: an
// *objectstore.Client built without static keys, passed to the uploader as
// its objectstore.Writer role, signs the upload with the credentials the
// SDK's default chain resolves instead of failing on empty static keys.
func TestLogUploader_UploadsWithTheDefaultChain(t *testing.T) {
	objectstoretest.UseChainCredentials(t)
	srv := objectstoretest.NewServer(t, objectstoretest.OK)
	client, err := objectstore.New(context.Background(), objectstore.Config{
		EndpointURL: srv.URL, Region: "us-east-1", Bucket: defaultBucket,
	})
	require.NoError(t, err)

	require.NoError(t, NewLogUploader(client).UploadLog(context.Background(),
		"logs/task-executions/svc/analytics/orders/e1.log", "line one\n"))

	reqs := srv.Requests()
	require.Len(t, reqs, 1)
	assert.Equal(t, http.MethodPut, reqs[0].Method)
	assert.Equal(t, "/continuo/logs/task-executions/svc/analytics/orders/e1.log", reqs[0].Path)
	assert.Equal(t, "text/plain", reqs[0].ContentType)
	assert.Contains(t, reqs[0].Authorization, "Credential="+objectstoretest.ChainAccessKeyID+"/")
}
