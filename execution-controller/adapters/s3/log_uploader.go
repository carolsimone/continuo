// Package s3 implements execution-controller's object-storage port, the log
// uploader, over the objectstore.Writer role.
package s3

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/execution-controller/service/ports"
	"github.com/carolsimone/continuo/pkg/objectstore"
)

// LogUploader implements ports.LogUploader: it writes a Job's text log and its
// structured result block to the store's default bucket.
type LogUploader struct {
	objects objectstore.Writer
}

var _ ports.LogUploader = (*LogUploader)(nil)

// NewLogUploader builds a LogUploader over objects.
func NewLogUploader(objects objectstore.Writer) *LogUploader {
	return &LogUploader{objects: objects}
}

// UploadLog writes content at key in the default bucket as text/plain. Every
// failure is returned with its cause and the key; a key that names no object
// is refused by the store with objectstore.ErrInvalidRef before any write.
func (u *LogUploader) UploadLog(ctx context.Context, key, content string) error {
	if err := u.objects.Put(ctx, objectstore.Ref{Key: key}, []byte(content), "text/plain"); err != nil {
		return fmt.Errorf("upload log %q: %w", key, err)
	}
	return nil
}
