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

	"github.com/carolsimone/continuo/catalog/serialization/exportfile"
	"github.com/carolsimone/continuo/catalog/service/export"
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
// bucket: one object per export, at the key backup_key.go lays out, holding
// the export file exportfile encodes, gzipped. It reads, lists and deletes
// only keys of that layout, so it never touches another object.
type BackupStore struct {
	BackupKeys
	objects backupObjects
}

var (
	_ ports.BackupStore  = (*BackupStore)(nil)
	_ ports.ExportSink   = (*exportSink)(nil)
	_ ports.ExportSource = (*exportSource)(nil)
	_ ports.RecordWriter = (*exportfile.Writer)(nil)
	_ ports.RecordReader = (*exportfile.Reader)(nil)
)

// NewBackupStore builds a BackupStore over objects.
func NewBackupStore(objects backupObjects) *BackupStore {
	return &BackupStore{objects: objects}
}

// Open opens the multipart upload of tenantID's export taken at at, and
// returns the sink that encodes its records into it. progress, when not nil,
// is called after each write to the upload returns.
func (s *BackupStore) Open(ctx context.Context, tenantID string, at time.Time, progress func()) (ports.ExportSink, string, error) {
	key, err := exportKey(tenantID, at)
	if err != nil {
		return nil, "", err
	}
	w, err := s.objects.OpenWriter(ctx, objectstore.Ref{Key: key}, backupContentType)
	if err != nil {
		return nil, "", fmt.Errorf("open the export %s: %w", key, storeError(key, err))
	}
	return newExportSink(w, key, progress), key, nil
}

// OpenRead streams the export at key and reads its header.
func (s *BackupStore) OpenRead(ctx context.Context, key string) (ports.ExportSource, error) {
	body, err := s.stream(ctx, key)
	if err != nil {
		return nil, err
	}
	zr, err := gzip.NewReader(body)
	if err != nil {
		_ = body.Close()
		return nil, fmt.Errorf("decompress the export %s: %w", key, err)
	}
	r, err := exportfile.NewReader(zr)
	if err != nil {
		_ = body.Close()
		return nil, fmt.Errorf("read the export %s: %w", key, err)
	}
	return &exportSource{Reader: r, body: body}, nil
}

// Validate streams the export at key once through exportfile.Validate.
func (s *BackupStore) Validate(ctx context.Context, key, tenantID string) (export.Header, error) {
	body, err := s.stream(ctx, key)
	if err != nil {
		return export.Header{}, err
	}
	defer func() { _ = body.Close() }()
	zr, err := gzip.NewReader(body)
	if err != nil {
		return export.Header{}, fmt.Errorf("decompress the export %s: %w", key, err)
	}
	return exportfile.Validate(zr, tenantID)
}

// stream opens the object stored at the export key key.
func (s *BackupStore) stream(ctx context.Context, key string) (io.ReadCloser, error) {
	if _, _, err := parseExportKey(key); err != nil {
		return nil, err
	}
	rc, err := s.objects.GetStream(ctx, objectstore.Ref{Key: key})
	switch {
	case errors.Is(err, objectstore.ErrNotFound):
		return nil, storeError(key, err)
	case err != nil:
		return nil, fmt.Errorf("read the export %s: %w", key, storeError(key, err))
	}
	return rc, nil
}

// List lists the tenant's backup prefix and returns its exports, newest
// first by the time their key names.
func (s *BackupStore) List(ctx context.Context, tenantID string) ([]ports.BackupObject, error) {
	prefix, err := exportPrefix(tenantID)
	if err != nil {
		return nil, err
	}
	objects, err := s.objects.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("list the exports of tenant %q: %w", tenantID, err)
	}
	var exports []ports.BackupObject
	for _, o := range objects {
		owner, at, err := parseExportKey(o.Key)
		if err != nil || owner != tenantID {
			continue
		}
		exports = append(exports, ports.BackupObject{
			Key: o.Key, TenantID: owner, TakenAt: at, Size: o.Size, LastModified: o.LastModified,
		})
	}
	slices.SortFunc(exports, func(a, b ports.BackupObject) int { return cmp.Compare(b.TakenAt.UnixNano(), a.TakenAt.UnixNano()) })
	return exports, nil
}

// Delete deletes the export at key.
func (s *BackupStore) Delete(ctx context.Context, key string) error {
	if _, _, err := parseExportKey(key); err != nil {
		return err
	}
	if err := s.objects.Delete(ctx, objectstore.Ref{Key: key}); err != nil {
		return fmt.Errorf("delete the export %s: %w", key, storeError(key, err))
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
		return fmt.Errorf("%w: %s", ports.ErrExportClosed, key)
	case errors.Is(err, objectstore.ErrInvalidRef):
		return fmt.Errorf("%w: %q", ports.ErrNotAnExportKey, key)
	}
	return err
}

// exportSink encodes an export's records as JSON lines, gzips them and
// streams them into one object writer.
type exportSink struct {
	key     string
	records *exportfile.Writer
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

func newExportSink(w objectstore.ObjectWriter, key string, progress func()) *exportSink {
	object := &countingObject{w: w, key: key, progress: progress}
	zw := gzip.NewWriter(object)
	return &exportSink{key: key, records: exportfile.NewWriter(zw), zw: zw, object: object}
}

func (s *exportSink) WriteHeader(h export.Header) error {
	if err := s.usable(); err != nil {
		return err
	}
	return s.records.WriteHeader(h)
}

func (s *exportSink) WriteNode(n export.NodeRecord) error {
	if err := s.usable(); err != nil {
		return err
	}
	return s.records.WriteNode(n)
}

func (s *exportSink) WriteRel(r export.RelRecord) error {
	if err := s.usable(); err != nil {
		return err
	}
	return s.records.WriteRel(r)
}

// Close flushes the compressor and completes the object. It returns the
// object's size: the compressed bytes the store was given.
func (s *exportSink) Close() (int64, error) {
	if err := s.usable(); err != nil {
		return 0, err
	}
	if err := s.zw.Close(); err != nil {
		return 0, fmt.Errorf("compress the export %s: %w", s.key, err)
	}
	if err := s.object.w.Close(); err != nil {
		return 0, fmt.Errorf("complete the export %s: %w", s.key, storeError(s.key, err))
	}
	s.state = sinkClosed
	return s.object.n, nil
}

// Abort discards the upload, unless Close completed it.
func (s *exportSink) Abort() error {
	if s.state == sinkClosed {
		return nil
	}
	s.state = sinkAborted
	if err := s.object.w.Abort(); err != nil {
		return fmt.Errorf("abort the export %s: %w", s.key, storeError(s.key, err))
	}
	return nil
}

// usable returns ports.ErrExportClosed once the sink was completed or
// aborted.
func (s *exportSink) usable() error {
	if s.state != sinkOpen {
		return fmt.Errorf("%w: %s", ports.ErrExportClosed, s.key)
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

// exportSource reads an export's records from the decompressed stream of its
// object; Close closes the stream.
type exportSource struct {
	*exportfile.Reader
	body io.ReadCloser
}

func (s *exportSource) Close() error { return s.body.Close() }
