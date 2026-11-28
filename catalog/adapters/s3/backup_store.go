package s3

import (
	"cmp"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/carolsimone/continuo/catalog/serialization/backupfile"
	"github.com/carolsimone/continuo/catalog/service/graphbackup"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/pkg/objectstore"
)

// backupContentType is the content type of a stored backup: gzipped JSON
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
// bucket: one object per backup, at the key backup_key.go lays out, holding
// the backup file backupfile encodes, gzipped. It reads, lists and deletes
// only keys of that layout, so it never touches another object.
type BackupStore struct {
	BackupKeys
	objects backupObjects
}

var (
	_ ports.BackupStore  = (*BackupStore)(nil)
	_ ports.BackupSink   = (*backupSink)(nil)
	_ ports.BackupSource = (*backupSource)(nil)
	_ ports.RecordWriter = (*backupfile.Writer)(nil)
	_ ports.RecordReader = (*backupfile.Reader)(nil)
)

// NewBackupStore builds a BackupStore over objects.
func NewBackupStore(objects backupObjects) *BackupStore {
	return &BackupStore{objects: objects}
}

// Open opens the multipart upload of tenantID's backup taken at at, and
// returns the sink that encodes its records into it. progress, when not nil,
// is called after each write to the upload returns.
func (s *BackupStore) Open(ctx context.Context, tenantID string, at time.Time, progress func()) (ports.BackupSink, string, error) {
	key, err := backupKey(tenantID, at)
	if err != nil {
		return nil, "", err
	}
	w, err := s.objects.OpenWriter(ctx, objectstore.Ref{Key: key}, backupContentType)
	if err != nil {
		return nil, "", fmt.Errorf("open the backup %s: %w", key, storeError(key, err))
	}
	return newBackupSink(w, key, progress), key, nil
}

// OpenRead streams the backup at key and reads its header.
func (s *BackupStore) OpenRead(ctx context.Context, key string) (ports.BackupSource, error) {
	body, err := s.stream(ctx, key)
	if err != nil {
		return nil, err
	}
	zr, err := gzip.NewReader(body)
	if err != nil {
		_ = body.Close()
		return nil, fmt.Errorf("decompress the backup %s: %w", key, err)
	}
	r, err := backupfile.NewReader(zr)
	if err != nil {
		_ = body.Close()
		return nil, fmt.Errorf("read the backup %s: %w", key, err)
	}
	return &backupSource{Reader: r, body: body}, nil
}

// Validate streams the backup at key once through backupfile.Validate.
func (s *BackupStore) Validate(ctx context.Context, key, tenantID string) (graphbackup.Header, error) {
	body, err := s.stream(ctx, key)
	if err != nil {
		return graphbackup.Header{}, err
	}
	defer func() { _ = body.Close() }()
	zr, err := gzip.NewReader(body)
	if err != nil {
		return graphbackup.Header{}, fmt.Errorf("decompress the backup %s: %w", key, err)
	}
	return backupfile.Validate(zr, tenantID)
}

// stream opens the object stored at the backup key key.
func (s *BackupStore) stream(ctx context.Context, key string) (io.ReadCloser, error) {
	if _, _, err := parseBackupKey(key); err != nil {
		return nil, err
	}
	rc, err := s.objects.GetStream(ctx, objectstore.Ref{Key: key})
	switch {
	case errors.Is(err, objectstore.ErrNotFound):
		return nil, storeError(key, err)
	case err != nil:
		return nil, fmt.Errorf("read the backup %s: %w", key, storeError(key, err))
	}
	return rc, nil
}

// List lists the tenant's backup prefix and returns its backups, newest
// first by the time their key names.
func (s *BackupStore) List(ctx context.Context, tenantID string) ([]ports.BackupObject, error) {
	prefix, err := backupPrefix(tenantID)
	if err != nil {
		return nil, err
	}
	objects, err := s.objects.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("list the backups of tenant %q: %w", tenantID, err)
	}
	var backups []ports.BackupObject
	for _, o := range objects {
		owner, at, err := parseBackupKey(o.Key)
		if err != nil || owner != tenantID {
			continue
		}
		backups = append(backups, ports.BackupObject{
			Key: o.Key, TenantID: owner, TakenAt: at, Size: o.Size, LastModified: o.LastModified,
		})
	}
	slices.SortFunc(backups, func(a, b ports.BackupObject) int { return cmp.Compare(b.TakenAt.UnixNano(), a.TakenAt.UnixNano()) })
	return backups, nil
}

// Delete deletes the backup at key.
func (s *BackupStore) Delete(ctx context.Context, key string) error {
	if _, _, err := parseBackupKey(key); err != nil {
		return err
	}
	if err := s.objects.Delete(ctx, objectstore.Ref{Key: key}); err != nil {
		return fmt.Errorf("delete the backup %s: %w", key, storeError(key, err))
	}
	return nil
}

// storeError translates what the object store reports about the object at
// key, or about its writer, into the port's errors, and returns every other
// error, which keeps its cause, as it is.
func storeError(key string, err error) error {
	switch {
	case errors.Is(err, objectstore.ErrNotFound):
		return fmt.Errorf("%w: %s", ports.ErrBackupNotFound, key)
	case errors.Is(err, objectstore.ErrWriterClosed):
		return fmt.Errorf("%w: %s", ports.ErrBackupClosed, key)
	case errors.Is(err, objectstore.ErrInvalidRef):
		return fmt.Errorf("%w: %q", ports.ErrNotABackupKey, key)
	}
	return err
}

// backupSink encodes a backup's records as JSON lines, gzips them and
// streams them into one object writer.
type backupSink struct {
	key     string
	records *backupfile.Writer
	zw      *gzip.Writer
	object  *countingObject
	state   sinkState
}

type sinkState int

const (
	sinkOpen sinkState = iota
	sinkClosed
	sinkAborted
)

func newBackupSink(w objectstore.ObjectWriter, key string, progress func()) *backupSink {
	object := &countingObject{w: w, key: key, progress: progress}
	zw := gzip.NewWriter(object)
	return &backupSink{key: key, records: backupfile.NewWriter(zw), zw: zw, object: object}
}

func (s *backupSink) WriteHeader(h graphbackup.Header) error {
	if err := s.usable(); err != nil {
		return err
	}
	return s.records.WriteHeader(h)
}

func (s *backupSink) WriteNode(n graphbackup.NodeRecord) error {
	if err := s.usable(); err != nil {
		return err
	}
	return s.records.WriteNode(n)
}

func (s *backupSink) WriteRel(r graphbackup.RelRecord) error {
	if err := s.usable(); err != nil {
		return err
	}
	return s.records.WriteRel(r)
}

// Close flushes the compressor and completes the object. It returns the
// object's size: the compressed bytes the store was given.
func (s *backupSink) Close() (int64, error) {
	if err := s.usable(); err != nil {
		return 0, err
	}
	if err := s.zw.Close(); err != nil {
		return 0, fmt.Errorf("compress the backup %s: %w", s.key, err)
	}
	if err := s.object.w.Close(); err != nil {
		return 0, fmt.Errorf("complete the backup %s: %w", s.key, storeError(s.key, err))
	}
	s.state = sinkClosed
	return s.object.n, nil
}

// Abort discards the upload, unless Close completed it.
func (s *backupSink) Abort() error {
	if s.state == sinkClosed {
		return nil
	}
	s.state = sinkAborted
	if err := s.object.w.Abort(); err != nil {
		return fmt.Errorf("abort the backup %s: %w", s.key, storeError(s.key, err))
	}
	return nil
}

// usable returns ports.ErrBackupClosed once the sink was completed or
// aborted.
func (s *backupSink) usable() error {
	if s.state != sinkOpen {
		return fmt.Errorf("%w: %s", ports.ErrBackupClosed, s.key)
	}
	return nil
}

// countingObject writes through to an object writer, counting the bytes the
// store took, calling progress after each write that succeeds and translating
// its errors. The object writer uploads a part inside the write that fills it,
// so a record that fills several parts calls progress once per write.
type countingObject struct {
	w        objectstore.ObjectWriter
	key      string
	n        int64
	progress func()
}

func (c *countingObject) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	if err != nil {
		return n, storeError(c.key, err)
	}
	if c.progress != nil {
		c.progress()
	}
	return n, nil
}

// backupSource reads a backup's records from the decompressed stream of its
// object; Close closes the stream.
type backupSource struct {
	*backupfile.Reader
	body io.ReadCloser
}

func (s *backupSource) Close() error { return s.body.Close() }
