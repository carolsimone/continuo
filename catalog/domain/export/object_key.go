package export

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Each export of a tenant's graph is stored as one object under the tenant's
// backup prefix, named by the UTC second it was taken at:
//
//	tenants/<tenant>/backups/catalog/<20060102T150405Z>.jsonl.gz
//
// Keys of one tenant sort by the time they name, so the newest export has the
// greatest key.
const (
	objectKeyRoot   = "tenants/"
	objectKeyDir    = "/backups/catalog/"
	objectKeyTime   = "20060102T150405Z"
	objectKeySuffix = ".jsonl.gz"
)

// ErrNotAnExportKey means a key is not one ObjectKey writes, or a tenant id
// cannot name a backup prefix.
var ErrNotAnExportKey = errors.New("not the object key of a catalog export")

// ObjectPrefix returns the prefix every export of tenantID is stored under. A
// tenant id that is empty or holds a "/" names no prefix.
func ObjectPrefix(tenantID string) (string, error) {
	if tenantID == "" || strings.Contains(tenantID, "/") {
		return "", fmt.Errorf("%w: tenant id %q", ErrNotAnExportKey, tenantID)
	}
	return objectKeyRoot + tenantID + objectKeyDir, nil
}

// ObjectKey returns the key of tenantID's export taken at at, to the second in
// UTC.
func ObjectKey(tenantID string, at time.Time) (string, error) {
	prefix, err := ObjectPrefix(tenantID)
	if err != nil {
		return "", err
	}
	return prefix + at.UTC().Format(objectKeyTime) + objectKeySuffix, nil
}

// ParseObjectKey returns the tenant and the time of the export stored at key.
// A key ObjectKey would not write is ErrNotAnExportKey.
func ParseObjectKey(key string) (tenantID string, at time.Time, err error) {
	rest, ok := strings.CutPrefix(key, objectKeyRoot)
	if !ok {
		return "", time.Time{}, fmt.Errorf("%w: %q", ErrNotAnExportKey, key)
	}
	tenantID, name, ok := strings.Cut(rest, objectKeyDir)
	if !ok {
		return "", time.Time{}, fmt.Errorf("%w: %q", ErrNotAnExportKey, key)
	}
	stamp, ok := strings.CutSuffix(name, objectKeySuffix)
	if !ok {
		return "", time.Time{}, fmt.Errorf("%w: %q", ErrNotAnExportKey, key)
	}
	at, err = time.Parse(objectKeyTime, stamp)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("%w: %q", ErrNotAnExportKey, key)
	}
	// Only the exact text ObjectKey writes is an export's key.
	if again, err := ObjectKey(tenantID, at); err != nil || again != key {
		return "", time.Time{}, fmt.Errorf("%w: %q", ErrNotAnExportKey, key)
	}
	return tenantID, at, nil
}
