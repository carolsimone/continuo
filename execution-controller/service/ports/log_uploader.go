package ports

import "context"

// LogUploader uploads log content to object storage.
type LogUploader interface {
	UploadLog(ctx context.Context, key, content string) error
}
