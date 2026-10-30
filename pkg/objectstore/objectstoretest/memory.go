package objectstoretest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/carolsimone/continuo/pkg/objectstore"
)

// Read is one Get that reached a Memory, with its Ref resolved.
type Read struct {
	Ref      objectstore.Ref
	MaxBytes int64
}

// Write is one object a Memory stored through Put or a completed writer, with
// its Ref resolved.
type Write struct {
	Ref         objectstore.Ref
	Body        []byte
	ContentType string
}

// Memory is an in-memory object store implementing the objectstore roles as
// *objectstore.Client does. A Ref with an empty Bucket resolves to the
// default bucket. A Ref that names no object is objectstore.ErrInvalidRef and
// an unusable read limit is an error, both before the store is touched. A
// missing object is objectstore.ErrNotFound and one over the read limit
// objectstore.ErrTooLarge. Bodies are copied in and out. A writer opened with
// OpenWriter stores its object when it is closed, and not before; one that is
// aborted stores nothing. A writer obeys the context it was opened with, as
// the Client's does: once that context is done, Write and Close fail with its
// error and Abort still discards what was written. It is safe for concurrent
// use.
type Memory struct {
	mu               sync.Mutex
	defaultBucket    string
	objects          map[objectstore.Ref]stored
	failGet          map[objectstore.Ref]error
	failPut          map[objectstore.Ref]error
	failDeleteObject map[objectstore.Ref]error
	failDeletePrefix map[string]error
	failList         map[string]error
	reads            []Read
	puts             []Write
}

// stored is an object in a Memory: its bytes and when it was stored.
type stored struct {
	body     []byte
	modified time.Time
}

var (
	_ objectstore.Reader        = (*Memory)(nil)
	_ objectstore.Writer        = (*Memory)(nil)
	_ objectstore.PrefixDeleter = (*Memory)(nil)
	_ objectstore.StreamWriter  = (*Memory)(nil)
	_ objectstore.StreamReader  = (*Memory)(nil)
	_ objectstore.Lister        = (*Memory)(nil)
	_ objectstore.Deleter       = (*Memory)(nil)
)

// NewMemory returns an empty Memory whose default bucket is defaultBucket.
func NewMemory(defaultBucket string) *Memory {
	return &Memory{
		defaultBucket:    defaultBucket,
		objects:          map[objectstore.Ref]stored{},
		failGet:          map[objectstore.Ref]error{},
		failPut:          map[objectstore.Ref]error{},
		failDeleteObject: map[objectstore.Ref]error{},
		failDeletePrefix: map[string]error{},
		failList:         map[string]error{},
	}
}

func (m *Memory) resolve(ref objectstore.Ref) objectstore.Ref {
	if ref.Bucket == "" {
		ref.Bucket = m.defaultBucket
	}
	return ref
}

// Get implements objectstore.Reader. A Get that reaches the store is recorded
// in Reads, including one that fails.
func (m *Memory) Get(_ context.Context, ref objectstore.Ref, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 || maxBytes == math.MaxInt64 {
		return nil, fmt.Errorf("objectstoretest: read limit %d is not usable", maxBytes)
	}
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	ref = m.resolve(ref)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reads = append(m.reads, Read{Ref: ref, MaxBytes: maxBytes})
	if err := m.failGet[ref]; err != nil {
		return nil, err
	}
	obj, ok := m.objects[ref]
	if !ok {
		return nil, fmt.Errorf("%w: %s", objectstore.ErrNotFound, ref)
	}
	body := obj.body
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("%w: %s is %d bytes, over the %d-byte limit",
			objectstore.ErrTooLarge, ref, len(body), maxBytes)
	}
	return bytes.Clone(body), nil
}

// Put implements objectstore.Writer. A stored write is recorded in Puts; a
// write FailPut refuses stores nothing and is not recorded.
func (m *Memory) Put(_ context.Context, ref objectstore.Ref, body []byte, contentType string) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	ref = m.resolve(ref)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.failPut[ref]; err != nil {
		return err
	}
	m.store(ref, bytes.Clone(body), contentType)
	return nil
}

// store keeps body, which it owns, at ref and records the write. The caller
// holds m.mu.
func (m *Memory) store(ref objectstore.Ref, body []byte, contentType string) {
	m.objects[ref] = stored{body: body, modified: time.Now()}
	m.puts = append(m.puts, Write{Ref: ref, Body: bytes.Clone(body), ContentType: contentType})
}

// GetStream implements objectstore.StreamReader. The stream reads a copy of the
// object taken when GetStream is called. A FailGet error applies to it; it is
// not recorded in Reads, which hold reads with a limit.
func (m *Memory) GetStream(_ context.Context, ref objectstore.Ref) (io.ReadCloser, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	ref = m.resolve(ref)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.failGet[ref]; err != nil {
		return nil, err
	}
	obj, ok := m.objects[ref]
	if !ok {
		return nil, fmt.Errorf("%w: %s", objectstore.ErrNotFound, ref)
	}
	return io.NopCloser(bytes.NewReader(bytes.Clone(obj.body))), nil
}

// List implements objectstore.Lister over the default bucket, in key order.
// Each object's LastModified is the time it was stored. A List of a prefix
// FailList names returns its error.
func (m *Memory) List(_ context.Context, prefix string) ([]objectstore.Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.failList[prefix]; err != nil {
		return nil, err
	}
	var objects []objectstore.Object
	for _, ref := range slices.SortedFunc(maps.Keys(m.objects), func(a, b objectstore.Ref) int {
		return strings.Compare(a.Key, b.Key)
	}) {
		if ref.Bucket != m.defaultBucket || !strings.HasPrefix(ref.Key, prefix) {
			continue
		}
		obj := m.objects[ref]
		objects = append(objects, objectstore.Object{Key: ref.Key, Size: int64(len(obj.body)), LastModified: obj.modified})
	}
	return objects, nil
}

// Delete implements objectstore.Deleter. An object that does not exist is not
// an error; a Delete FailDelete refuses deletes nothing.
func (m *Memory) Delete(_ context.Context, ref objectstore.Ref) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	ref = m.resolve(ref)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.failDeleteObject[ref]; err != nil {
		return err
	}
	delete(m.objects, ref)
	return nil
}

// OpenWriter implements objectstore.StreamWriter. A write FailPut refuses fails
// here, as a store refuses the multipart upload an object is written through,
// and so does a ctx that is already done.
func (m *Memory) OpenWriter(ctx context.Context, ref objectstore.Ref, contentType string) (objectstore.ObjectWriter, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	ref = m.resolve(ref)
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("open writer for %s: %w", ref, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.failPut[ref]; err != nil {
		return nil, err
	}
	return &memoryWriter{mem: m, ctx: ctx, ref: ref, contentType: contentType}, nil
}

// memoryWriter is the ObjectWriter of a Memory: it buffers what is written and
// stores it when it is closed. Write and Close fail once ctx is done. It is not
// safe for concurrent use.
type memoryWriter struct {
	mem         *Memory
	ctx         context.Context
	ref         objectstore.Ref // resolved
	contentType string
	buf         bytes.Buffer
	state       writerState
	err         error // a failed Close or a done ctx; every later Write and Close returns it
}

type writerState int

const (
	writerOpen writerState = iota
	writerCompleted
	writerAborted
)

func (w *memoryWriter) Write(p []byte) (int, error) {
	if err := w.usable(); err != nil {
		return 0, err
	}
	return w.buf.Write(p)
}

// Close stores the object. A write FailPut refuses after the writer was opened
// fails here and stores nothing; the writer can still be aborted.
func (w *memoryWriter) Close() error {
	if err := w.usable(); err != nil {
		return err
	}
	w.mem.mu.Lock()
	defer w.mem.mu.Unlock()
	if err := w.mem.failPut[w.ref]; err != nil {
		w.err = err
		return err
	}
	w.mem.store(w.ref, w.buf.Bytes(), w.contentType)
	w.state = writerCompleted
	w.buf = bytes.Buffer{}
	return nil
}

// Abort discards what was written. After Close or a second Abort it does
// nothing.
func (w *memoryWriter) Abort() error {
	if w.state == writerOpen {
		w.state = writerAborted
		w.buf = bytes.Buffer{}
	}
	return nil
}

// usable reports why the writer cannot take a Write or a Close, or nil.
func (w *memoryWriter) usable() error {
	if w.state != writerOpen {
		return fmt.Errorf("%w: %s", objectstore.ErrWriterClosed, w.ref)
	}
	if w.err != nil {
		return w.err
	}
	if err := w.ctx.Err(); err != nil {
		w.err = fmt.Errorf("object writer for %s: %w", w.ref, err)
		return w.err
	}
	return nil
}

// DeletePrefix implements objectstore.PrefixDeleter over the default bucket.
func (m *Memory) DeletePrefix(_ context.Context, prefix string) error {
	if prefix == "" {
		return errors.New("objectstoretest: refusing to delete an empty prefix")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.failDeletePrefix[prefix]; err != nil {
		return err
	}
	for ref := range m.objects {
		if ref.Bucket == m.defaultBucket && strings.HasPrefix(ref.Key, prefix) {
			delete(m.objects, ref)
		}
	}
	return nil
}

// FailGet makes every later Get of ref (resolved against the default bucket)
// return err.
func (m *Memory) FailGet(ref objectstore.Ref, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failGet[m.resolve(ref)] = err
}

// FailPut makes every later Put of ref (resolved against the default bucket)
// return err and store nothing.
func (m *Memory) FailPut(ref objectstore.Ref, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failPut[m.resolve(ref)] = err
}

// FailDeletePrefix makes every later DeletePrefix of exactly prefix return
// err and delete nothing.
func (m *Memory) FailDeletePrefix(prefix string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failDeletePrefix[prefix] = err
}

// FailDelete makes every later Delete of ref (resolved against the default
// bucket) return err and delete nothing.
func (m *Memory) FailDelete(ref objectstore.Ref, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failDeleteObject[m.resolve(ref)] = err
}

// FailList makes every later List of exactly prefix return err.
func (m *Memory) FailList(prefix string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failList[prefix] = err
}

// Reads returns every Get that reached the store, in order.
func (m *Memory) Reads() []Read {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.reads)
}

// Puts returns every object Put stored, in order, with copies of the bodies.
func (m *Memory) Puts() []Write {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Write, len(m.puts))
	for i, w := range m.puts {
		out[i] = Write{Ref: w.Ref, Body: bytes.Clone(w.Body), ContentType: w.ContentType}
	}
	return out
}
