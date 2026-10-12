package objectstoretest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"

	"github.com/carolsimone/continuo/pkg/objectstore"
)

// Read is one Get that reached a Memory, with its Ref resolved.
type Read struct {
	Ref      objectstore.Ref
	MaxBytes int64
}

// Write is one object a Memory stored through Put, with its Ref resolved.
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
// objectstore.ErrTooLarge. Bodies are copied in and out. It is safe for
// concurrent use.
type Memory struct {
	mu            sync.Mutex
	defaultBucket string
	objects       map[objectstore.Ref][]byte
	failGet       map[objectstore.Ref]error
	failPut       map[objectstore.Ref]error
	failDelete    map[string]error
	reads         []Read
	puts          []Write
}

var (
	_ objectstore.Reader        = (*Memory)(nil)
	_ objectstore.Writer        = (*Memory)(nil)
	_ objectstore.PrefixDeleter = (*Memory)(nil)
)

// NewMemory returns an empty Memory whose default bucket is defaultBucket.
func NewMemory(defaultBucket string) *Memory {
	return &Memory{
		defaultBucket: defaultBucket,
		objects:       map[objectstore.Ref][]byte{},
		failGet:       map[objectstore.Ref]error{},
		failPut:       map[objectstore.Ref]error{},
		failDelete:    map[string]error{},
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
	body, ok := m.objects[ref]
	if !ok {
		return nil, fmt.Errorf("%w: %s", objectstore.ErrNotFound, ref)
	}
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
	m.objects[ref] = bytes.Clone(body)
	m.puts = append(m.puts, Write{Ref: ref, Body: bytes.Clone(body), ContentType: contentType})
	return nil
}

// DeletePrefix implements objectstore.PrefixDeleter over the default bucket.
func (m *Memory) DeletePrefix(_ context.Context, prefix string) error {
	if prefix == "" {
		return errors.New("objectstoretest: refusing to delete an empty prefix")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.failDelete[prefix]; err != nil {
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
	m.failDelete[prefix] = err
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
