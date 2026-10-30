package s3

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/export"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/pkg/objectstore"
)

// backupContentType is the content type of a stored export: gzipped JSON
// lines.
const backupContentType = "application/gzip"

// backupObjects is the part of an object store the backup store uses:
// streaming writes and reads, listing a prefix and deleting one object.
type backupObjects interface {
	objectstore.StreamWriter
	objectstore.StreamReader
	objectstore.Lister
	objectstore.Deleter
}

// BackupStore implements ports.BackupStore in the object store's default
// bucket, one object per export at the key export.ObjectKey names. It reads,
// lists and deletes only keys of that form, so it never touches another
// object.
type BackupStore struct {
	objects backupObjects
}

var _ ports.BackupStore = (*BackupStore)(nil)

// NewBackupStore builds a BackupStore over objects.
func NewBackupStore(objects backupObjects) *BackupStore {
	return &BackupStore{objects: objects}
}

// Open opens the multipart upload of tenantID's export taken at at.
func (s *BackupStore) Open(ctx context.Context, tenantID string, at time.Time) (ports.BackupWriter, string, error) {
	key, err := export.ObjectKey(tenantID, at)
	if err != nil {
		return nil, "", err
	}
	w, err := s.objects.OpenWriter(ctx, objectstore.Ref{Key: key}, backupContentType)
	if err != nil {
		return nil, "", fmt.Errorf("open the export %s: %w", key, err)
	}
	return w, key, nil
}

// List lists the tenant's backup prefix and returns its exports, newest
// first by the time their key names.
func (s *BackupStore) List(ctx context.Context, tenantID string) ([]ports.BackupObject, error) {
	prefix, err := export.ObjectPrefix(tenantID)
	if err != nil {
		return nil, err
	}
	objects, err := s.objects.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("list the exports of tenant %q: %w", tenantID, err)
	}
	type dated struct {
		obj ports.BackupObject
		at  time.Time
	}
	var exports []dated
	for _, o := range objects {
		owner, at, err := export.ParseObjectKey(o.Key)
		if err != nil || owner != tenantID {
			continue
		}
		exports = append(exports, dated{ports.BackupObject{Key: o.Key, Size: o.Size, LastModified: o.LastModified}, at})
	}
	slices.SortFunc(exports, func(a, b dated) int { return cmp.Compare(b.at.UnixNano(), a.at.UnixNano()) })
	out := make([]ports.BackupObject, len(exports))
	for i, e := range exports {
		out[i] = e.obj
	}
	return out, nil
}

// Read streams the export at key. A missing object is ports.ErrBackupNotFound.
func (s *BackupStore) Read(ctx context.Context, key string) (io.ReadCloser, error) {
	if _, _, err := export.ParseObjectKey(key); err != nil {
		return nil, err
	}
	rc, err := s.objects.GetStream(ctx, objectstore.Ref{Key: key})
	switch {
	case errors.Is(err, objectstore.ErrNotFound):
		return nil, fmt.Errorf("%w: %s", ports.ErrBackupNotFound, key)
	case err != nil:
		return nil, fmt.Errorf("read the export %s: %w", key, err)
	}
	return rc, nil
}

// Delete deletes the export at key.
func (s *BackupStore) Delete(ctx context.Context, key string) error {
	if _, _, err := export.ParseObjectKey(key); err != nil {
		return err
	}
	if err := s.objects.Delete(ctx, objectstore.Ref{Key: key}); err != nil {
		return fmt.Errorf("delete the export %s: %w", key, err)
	}
	return nil
}
