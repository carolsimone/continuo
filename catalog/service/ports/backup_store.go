package ports

import (
	"context"
	"errors"
	"io"
	"time"
)

// ErrBackupNotFound means no export is stored at the key.
var ErrBackupNotFound = errors.New("no export is stored at that key")

// BackupWriter writes one export object as a stream. Nothing is stored until
// Close completes the object; Abort discards everything written. Every writer
// ends in Close or Abort; Abort after a successful Close does nothing. A
// writer is not safe for concurrent use.
type BackupWriter interface {
	io.Writer
	Close() error
	Abort() error
}

// BackupObject is one stored export: its key, its size in bytes and the time
// the store last modified it.
type BackupObject struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// BackupStore keeps each tenant's exports, one object per export, at the key
// export.ObjectKey names: tenants/<tenant>/backups/catalog/<20060102T150405Z>.jsonl.gz.
// It reads, lists and deletes only keys of that form.
type BackupStore interface {
	// Open starts the export of tenantID taken at at and returns its writer
	// and its key. The upload is opened with the call, so a store that
	// refuses writes fails Open before anything is written.
	Open(ctx context.Context, tenantID string, at time.Time) (BackupWriter, string, error)
	// List returns the tenant's stored exports, newest first. An object under
	// the tenant's backup prefix whose key is not an export's is not listed.
	List(ctx context.Context, tenantID string) ([]BackupObject, error)
	// Read streams the export stored at key; the caller closes it. A missing
	// export is ErrBackupNotFound, and a key that is not an export's is
	// export.ErrNotAnExportKey.
	Read(ctx context.Context, key string) (io.ReadCloser, error)
	// Delete deletes the export stored at key. A missing export is not an
	// error; a key that is not an export's is export.ErrNotAnExportKey.
	Delete(ctx context.Context, key string) error
}
