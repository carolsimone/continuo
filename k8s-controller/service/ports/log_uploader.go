// Package ports declares k8s-controller's application-layer ports for
// technical collaborators that are not domain concepts. Implementations
// live in k8s-controller/adapters.
package ports

import "context"

// LogUploader uploads log content to object storage.
type LogUploader interface {
	UploadLog(ctx context.Context, key, content string) error
}
