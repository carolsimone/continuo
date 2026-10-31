package s3

import (
	"fmt"
	"strings"
	"time"

	"github.com/carolsimone/continuo/catalog/service/ports"
)

// Each export of a tenant's graph is stored as one object under the tenant's
// backup prefix, named by the UTC second it was taken at:
//
//	tenants/<tenant>/backups/catalog/<20060102T150405Z>.jsonl.gz
//
// Keys of one tenant sort by the time they name, so the newest export has the
// greatest key.
const (
	exportKeyRoot   = "tenants/"
	exportKeyDir    = "/backups/catalog/"
	exportKeyTime   = "20060102T150405Z"
	exportKeySuffix = ".jsonl.gz"
)

// BackupKeys implements ports.BackupKeys over the key layout above. It needs
// no object store, so a command checks a key or a tenant with it before it
// connects to anything.
type BackupKeys struct{}

var _ ports.BackupKeys = BackupKeys{}

// Locate returns the tenant and the time of the export stored at key.
func (BackupKeys) Locate(key string) (string, time.Time, error) {
	return parseExportKey(key)
}

// CheckTenant refuses a tenant id that cannot name a backup prefix.
func (BackupKeys) CheckTenant(tenantID string) error {
	_, err := exportPrefix(tenantID)
	return err
}

// exportPrefix returns the prefix every export of tenantID is stored under. A
// tenant id that is empty, "." or "..", or holds a "/", names no prefix: it
// would name no directory, or another one.
func exportPrefix(tenantID string) (string, error) {
	if tenantID == "" || tenantID == "." || tenantID == ".." || strings.Contains(tenantID, "/") {
		return "", fmt.Errorf("%w: %q", ports.ErrInvalidTenant, tenantID)
	}
	return exportKeyRoot + tenantID + exportKeyDir, nil
}

// exportKey returns the key of tenantID's export taken at at, to the second in
// UTC. Two exports of one tenant taken in the same UTC second share a key, so
// the later replaces the earlier.
func exportKey(tenantID string, at time.Time) (string, error) {
	prefix, err := exportPrefix(tenantID)
	if err != nil {
		return "", err
	}
	return prefix + at.UTC().Format(exportKeyTime) + exportKeySuffix, nil
}

// parseExportKey returns the tenant and the time of the export stored at key.
// A key exportKey would not write is ports.ErrNotAnExportKey.
func parseExportKey(key string) (tenantID string, at time.Time, err error) {
	notAnExport := fmt.Errorf("%w: %q", ports.ErrNotAnExportKey, key)
	rest, ok := strings.CutPrefix(key, exportKeyRoot)
	if !ok {
		return "", time.Time{}, notAnExport
	}
	tenantID, name, ok := strings.Cut(rest, exportKeyDir)
	if !ok {
		return "", time.Time{}, notAnExport
	}
	stamp, ok := strings.CutSuffix(name, exportKeySuffix)
	if !ok {
		return "", time.Time{}, notAnExport
	}
	at, err = time.Parse(exportKeyTime, stamp)
	if err != nil {
		return "", time.Time{}, notAnExport
	}
	// Only the exact text exportKey writes is an export's key.
	if again, err := exportKey(tenantID, at); err != nil || again != key {
		return "", time.Time{}, notAnExport
	}
	return tenantID, at, nil
}
