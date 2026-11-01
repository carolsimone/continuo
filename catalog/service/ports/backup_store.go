package ports

import (
	"context"
	"errors"
	"time"

	"github.com/carolsimone/continuo/catalog/service/export"
)

var (
	// ErrBackupNotFound means no export is stored at the key.
	ErrBackupNotFound = errors.New("no export is stored at that key")
	// ErrNotAnExportKey means a key does not name a stored export.
	ErrNotAnExportKey = errors.New("not the object key of a catalog export")
	// ErrInvalidTenant means a tenant id cannot own stored exports: it is
	// empty, "." or "..", or holds a "/".
	ErrInvalidTenant = errors.New("the tenant id cannot own stored exports")
	// ErrExportClosed means a record or a Close reached an export that was
	// already completed or aborted.
	ErrExportClosed = errors.New("the export is already completed or aborted")
)

// ExportSink stores one export, record by record. Nothing is stored until
// Close completes it; Abort discards everything written. Every sink ends in
// Close or Abort; Abort after a successful Close does nothing. A sink is not
// safe for concurrent use.
type ExportSink interface {
	RecordWriter
	// Close completes the export and returns the size in bytes of what the
	// store keeps.
	Close() (int64, error)
	// Abort discards the export.
	Abort() error
}

// ExportSource reads one stored export back, record by record. The caller
// closes it.
type ExportSource interface {
	RecordReader
	Close() error
}

// BackupObject is one stored export: its key, the tenant it belongs to, the
// time it was taken (to the second), its size in bytes and the time the store
// last modified it.
type BackupObject struct {
	Key          string
	TenantID     string
	TakenAt      time.Time
	Size         int64
	LastModified time.Time
}

// BackupKeys names where exports are kept, without reaching the store, so a
// command can check its arguments before it connects to anything.
type BackupKeys interface {
	// Locate returns the tenant and the time of the export a key names. A key
	// that is not an export's is ErrNotAnExportKey.
	Locate(key string) (tenantID string, takenAt time.Time, err error)
	// CheckTenant returns ErrInvalidTenant for a tenant id that cannot own
	// stored exports.
	CheckTenant(tenantID string) error
}

// BackupStore keeps each tenant's exports, one per key. It reads, lists and
// deletes only exports, never another object, and refuses a key that is not
// an export's with ErrNotAnExportKey and a tenant id that cannot own exports
// with ErrInvalidTenant.
type BackupStore interface {
	BackupKeys
	// Open starts the export of tenantID taken at at and returns its sink and
	// its key. The store is reached with the call, so a store that refuses
	// writes fails Open before anything is written. Two exports of one tenant
	// taken in the same second share a key, and the later replaces the
	// earlier. When progress is not nil, the sink calls it each time a write to
	// the object store returns, so each part a large record fills counts as
	// progress for the caller's stall watch.
	Open(ctx context.Context, tenantID string, at time.Time, progress func()) (ExportSink, string, error)
	// OpenRead opens the export stored at key and reads its header. A missing
	// export is ErrBackupNotFound.
	OpenRead(ctx context.Context, key string) (ExportSource, error)
	// Validate reads the whole export stored at key, and touches nothing else.
	// It returns the header when the export is one a restore of tenantID can
	// load exactly: it names tenantID, every record is one the catalog graph
	// holds, its counts match its header, no two nodes share an identity and
	// every relationship names a node of the export. A missing export is
	// ErrBackupNotFound. Its memory grows with the number of nodes.
	Validate(ctx context.Context, key, tenantID string) (export.Header, error)
	// List returns the tenant's stored exports, newest first.
	List(ctx context.Context, tenantID string) ([]BackupObject, error)
	// Delete deletes the export stored at key. A missing export is not an
	// error.
	Delete(ctx context.Context, key string) error
}
