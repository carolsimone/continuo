package ports

import (
	"context"
	"errors"
	"time"

	"github.com/carolsimone/continuo/catalog/service/graphbackup"
)

var (
	// ErrBackupNotFound means no backup is stored at the key.
	ErrBackupNotFound = errors.New("no backup is stored at that key")
	// ErrNotABackupKey means a key does not name a stored backup.
	ErrNotABackupKey = errors.New("not the object key of a catalog backup")
	// ErrInvalidTenant means a tenant id cannot own stored backups: it is
	// empty, "." or "..", or holds a "/".
	ErrInvalidTenant = errors.New("the tenant id cannot own stored backups")
	// ErrBackupClosed means a record or a Close reached a backup that was
	// already completed or aborted.
	ErrBackupClosed = errors.New("the backup is already completed or aborted")
)

// BackupSink stores one backup, record by record. Nothing is stored until
// Close completes it; Abort discards everything written. Every sink ends in
// Close or Abort; Abort after a successful Close does nothing. A sink is not
// safe for concurrent use.
type BackupSink interface {
	RecordWriter
	// Close completes the backup and returns the size in bytes of what the
	// store keeps.
	Close() (int64, error)
	// Abort discards the backup.
	Abort() error
}

// BackupSource reads one stored backup back, record by record. The caller
// closes it.
type BackupSource interface {
	RecordReader
	Close() error
}

// BackupObject is one stored backup: its key, the tenant it belongs to, the
// time it was taken (to the second), its size in bytes and the time the store
// last modified it.
type BackupObject struct {
	Key          string
	TenantID     string
	TakenAt      time.Time
	Size         int64
	LastModified time.Time
}

// BackupKeys names where backups are kept, without reaching the store, so a
// command can check its arguments before it connects to anything.
type BackupKeys interface {
	// Locate returns the tenant and the time of the backup a key names. A key
	// that is not a backup's is ErrNotABackupKey.
	Locate(key string) (tenantID string, takenAt time.Time, err error)
	// CheckTenant returns ErrInvalidTenant for a tenant id that cannot own
	// stored backups.
	CheckTenant(tenantID string) error
}

// BackupStore keeps each tenant's backups, one per key. It reads, lists and
// deletes only backups, never another object, and refuses a key that is not
// a backup's with ErrNotABackupKey and a tenant id that cannot own backups
// with ErrInvalidTenant.
type BackupStore interface {
	BackupKeys
	// Open starts the backup of tenantID taken at at and returns its sink and
	// its key. The store is reached with the call, so a store that refuses
	// writes fails Open before anything is written. Two backups of one tenant
	// taken in the same second share a key, and the later replaces the
	// earlier. When progress is not nil, the sink calls it each time a write to
	// the object store returns, so each part a large record fills counts as
	// progress for the caller's stall watch.
	Open(ctx context.Context, tenantID string, at time.Time, progress func()) (BackupSink, string, error)
	// OpenRead opens the backup stored at key and reads its header. A missing
	// backup is ErrBackupNotFound.
	OpenRead(ctx context.Context, key string) (BackupSource, error)
	// Validate reads the whole backup stored at key, and touches nothing else.
	// It returns the header when the backup is one a restore of tenantID can
	// load exactly: it names tenantID, every record is one the catalog graph
	// holds, its counts match its header, no two nodes share an identity and
	// every relationship names a node of the backup. A missing backup is
	// ErrBackupNotFound. Its memory grows with the number of nodes.
	Validate(ctx context.Context, key, tenantID string) (graphbackup.Header, error)
	// List returns the tenant's stored backups, newest first.
	List(ctx context.Context, tenantID string) ([]BackupObject, error)
	// Delete deletes the backup stored at key. A missing backup is not an
	// error.
	Delete(ctx context.Context, key string) error
}
