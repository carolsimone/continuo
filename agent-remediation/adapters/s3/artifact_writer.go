package s3

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/pkg/objectstore"
)

// ArtifactWriter implements ports.ArtifactWriter over an object-store writer:
// it stores proposal artifacts in one bucket and returns their s3:// URIs.
type ArtifactWriter struct {
	objects objectstore.Writer
	bucket  string
}

var _ ports.ArtifactWriter = (*ArtifactWriter)(nil)

// NewArtifactWriter builds an ArtifactWriter that writes into bucket through
// objects. Every URI Write returns names bucket.
func NewArtifactWriter(objects objectstore.Writer, bucket string) *ArtifactWriter {
	return &ArtifactWriter{objects: objects, bucket: bucket}
}

// Write stores body at key in the writer's bucket with the given contentType
// and returns the object URI, "s3://<bucket>/<key>". Every failure is
// returned with its cause; a key that names no object is refused by the store
// with objectstore.ErrInvalidRef before any write.
func (w *ArtifactWriter) Write(ctx context.Context, key, body, contentType string) (string, error) {
	ref := objectstore.Ref{Bucket: w.bucket, Key: key}
	if err := w.objects.Put(ctx, ref, []byte(body), contentType); err != nil {
		return "", fmt.Errorf("write artifact %s: %w", ref, err)
	}
	return ref.String(), nil
}
