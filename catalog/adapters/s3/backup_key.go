package s3

import (
	"fmt"
	"strings"
	"time"

	"github.com/carolsimone/continuo/catalog/service/ports"
)

// Each backup of a tenant's graph is stored as one object under the tenant's
// backup prefix, named by the UTC second it was taken at:
//
//	tenants/<tenant>/backups/catalog/<20060102T150405Z>.jsonl.gz
//
// Keys of one tenant sort by the time they name, so the newest backup has the
// greatest key.
const (
	backupKeyRoot   = "tenants/"
	backupKeyDir    = "/backups/catalog/"
	backupKeyTime   = "20060102T150405Z"
	backupKeySuffix = ".jsonl.gz"
)

// BackupKeys implements ports.BackupKeys over the key layout above. It needs
// no object store, so a command checks a key or a tenant with it before it
// connects to anything.
type BackupKeys struct{}

var _ ports.BackupKeys = BackupKeys{}

// Locate returns the tenant and the time of the backup stored at key.
func (BackupKeys) Locate(key string) (string, time.Time, error) {
	return parseBackupKey(key)
}

// CheckTenant refuses a tenant id that cannot name a backup prefix.
func (BackupKeys) CheckTenant(tenantID string) error {
	_, err := backupPrefix(tenantID)
	return err
}

// backupPrefix returns the prefix every backup of tenantID is stored under. A
// tenant id that is empty, "." or "..", or holds a "/", names no prefix: it
// would name no directory, or another one.
func backupPrefix(tenantID string) (string, error) {
	if tenantID == "" || tenantID == "." || tenantID == ".." || strings.Contains(tenantID, "/") {
		return "", fmt.Errorf("%w: %q", ports.ErrInvalidTenant, tenantID)
	}
	return backupKeyRoot + tenantID + backupKeyDir, nil
}

// backupKey returns the key of tenantID's backup taken at at, to the second in
// UTC. Two backups of one tenant taken in the same UTC second share a key, so
// the later replaces the earlier.
func backupKey(tenantID string, at time.Time) (string, error) {
	prefix, err := backupPrefix(tenantID)
	if err != nil {
		return "", err
	}
	return prefix + at.UTC().Format(backupKeyTime) + backupKeySuffix, nil
}

// parseBackupKey returns the tenant and the time of the backup stored at key.
// A key backupKey would not write is ports.ErrNotABackupKey.
func parseBackupKey(key string) (tenantID string, at time.Time, err error) {
	notABackup := fmt.Errorf("%w: %q", ports.ErrNotABackupKey, key)
	rest, ok := strings.CutPrefix(key, backupKeyRoot)
	if !ok {
		return "", time.Time{}, notABackup
	}
	tenantID, name, ok := strings.Cut(rest, backupKeyDir)
	if !ok {
		return "", time.Time{}, notABackup
	}
	stamp, ok := strings.CutSuffix(name, backupKeySuffix)
	if !ok {
		return "", time.Time{}, notABackup
	}
	at, err = time.Parse(backupKeyTime, stamp)
	if err != nil {
		return "", time.Time{}, notABackup
	}
	// Only the exact text backupKey writes is a backup's key.
	if again, err := backupKey(tenantID, at); err != nil || again != key {
		return "", time.Time{}, notABackup
	}
	return tenantID, at, nil
}
