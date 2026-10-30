// Package exportfile is the file format of a tenant's graph export: JSON
// lines, the header first, then one line per node, then one line per
// relationship.
//
//	{"format_version":1,"tenant_id":"default","exported_at":"2026-10-30T02:00:00.5Z","catalog_version":"…","nodes":{"Node":1,"NodeVersion":1},"relationships":{"CURRENT":1}}
//	{"node":["Node"],"props":{"active":true,"code_version_promotion_seq":{"$int":"7"},"tenant_id":"default","unique_id":"analytics.table_a"}}
//	{"node":["NodeVersion"],"props":{"content_hash":"9f2c","tenant_id":"default","unique_id":"analytics.table_a"}}
//	{"rel":"CURRENT","from":["Node","analytics.table_a"],"to":["NodeVersion","analytics.table_a","9f2c"],"props":{"release_id":"r7"}}
//
// A relationship names each end by a label and the values of that label's
// identity properties (graphschema.Identity). Property values and key values
// keep their type (see value.go); the header's counts and version are plain
// JSON numbers.
//
// The Writer and the Reader apply the same checks to every record (those of
// package export), so a file the Writer completes is one the Reader accepts.
// Validate reads a whole file before a restore touches the graph.
package exportfile

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/export"
	"github.com/carolsimone/continuo/catalog/service/ports"
)

// headerLine is the JSON shape of the header.
type headerLine struct {
	FormatVersion  int              `json:"format_version"`
	TenantID       string           `json:"tenant_id"`
	ExportedAt     string           `json:"exported_at"`
	CatalogVersion string           `json:"catalog_version"`
	Nodes          map[string]int64 `json:"nodes"`
	Relationships  map[string]int64 `json:"relationships"`
}

// nodeLine is the JSON shape of a node.
type nodeLine struct {
	Node  []string       `json:"node"`
	Props map[string]any `json:"props"`
}

// relLine is the JSON shape of a relationship. From and To hold the end's
// label, then its key values.
type relLine struct {
	Rel   string         `json:"rel"`
	From  []any          `json:"from"`
	To    []any          `json:"to"`
	Props map[string]any `json:"props"`
}

// writerStage is how far a Writer has come.
type writerStage int

const (
	stageNoHeader writerStage = iota
	stageNodes
	stageRelationships
)

// Writer writes an export file to an io.Writer, one Write per line. It is
// not safe for concurrent use.
type Writer struct {
	w        io.Writer
	buf      bytes.Buffer
	enc      *json.Encoder
	stage    writerStage
	tenantID string
	err      error
}

var _ ports.RecordWriter = (*Writer)(nil)

// NewWriter returns a Writer that writes to w.
func NewWriter(w io.Writer) *Writer {
	wr := &Writer{w: w}
	wr.enc = json.NewEncoder(&wr.buf)
	wr.enc.SetEscapeHTML(false)
	return wr
}

// WriteHeader writes the header line. It must come first, once.
func (w *Writer) WriteHeader(h export.Header) error {
	if w.err != nil {
		return w.err
	}
	if w.stage != stageNoHeader {
		return errors.New("exportfile: the header is already written")
	}
	if err := h.Check(); err != nil {
		return err
	}
	if err := w.writeLine(headerLine{
		FormatVersion:  h.FormatVersion,
		TenantID:       h.TenantID,
		ExportedAt:     h.ExportedAt.UTC().Format(time.RFC3339Nano),
		CatalogVersion: h.CatalogVersion,
		Nodes:          nonNil(h.Nodes),
		Relationships:  nonNil(h.Relationships),
	}); err != nil {
		return err
	}
	w.tenantID = h.TenantID
	w.stage = stageNodes
	return nil
}

// WriteNode writes a node of the header's tenant. Every node comes before the
// first relationship.
func (w *Writer) WriteNode(n export.NodeRecord) error {
	if w.err != nil {
		return w.err
	}
	switch w.stage {
	case stageNoHeader:
		return errors.New("exportfile: a node before the header")
	case stageRelationships:
		return errors.New("exportfile: a node after the relationships")
	}
	if err := n.Check(w.tenantID); err != nil {
		return err
	}
	props, err := encodeProps(n.Props)
	if err != nil {
		return fmt.Errorf("node %v: %w", n.Labels, err)
	}
	return w.writeLine(nodeLine{Node: n.Labels, Props: props})
}

// WriteRel writes a relationship.
func (w *Writer) WriteRel(r export.RelRecord) error {
	if w.err != nil {
		return w.err
	}
	if w.stage == stageNoHeader {
		return errors.New("exportfile: a relationship before the header")
	}
	if err := r.Check(); err != nil {
		return err
	}
	from, err := encodeRef(r.From)
	if err != nil {
		return fmt.Errorf("relationship :%s: %w", r.Type, err)
	}
	to, err := encodeRef(r.To)
	if err != nil {
		return fmt.Errorf("relationship :%s: %w", r.Type, err)
	}
	props, err := encodeProps(r.Props)
	if err != nil {
		return fmt.Errorf("relationship :%s: %w", r.Type, err)
	}
	if err := w.writeLine(relLine{Rel: r.Type, From: from, To: to, Props: props}); err != nil {
		return err
	}
	w.stage = stageRelationships
	return nil
}

// writeLine encodes v as one line and writes it. A failed write may have
// written part of the line, so every later call returns its error.
func (w *Writer) writeLine(v any) error {
	w.buf.Reset()
	if err := w.enc.Encode(v); err != nil {
		return fmt.Errorf("exportfile: encode a line: %w", err)
	}
	if _, err := w.w.Write(w.buf.Bytes()); err != nil {
		w.err = fmt.Errorf("exportfile: write a line: %w", err)
		return w.err
	}
	return nil
}

func encodeProps(props map[string]any) (map[string]any, error) {
	out := make(map[string]any, len(props))
	for name, v := range props {
		enc, err := encodeValue(v)
		if err != nil {
			return nil, fmt.Errorf("property %q: %w", name, err)
		}
		out[name] = enc
	}
	return out, nil
}

func encodeRef(ref export.NodeRef) ([]any, error) {
	out := make([]any, 0, len(ref.Key)+1)
	out = append(out, ref.Label)
	for _, v := range ref.Key {
		enc, err := encodeValue(v)
		if err != nil {
			return nil, fmt.Errorf("a :%s's key: %w", ref.Label, err)
		}
		out = append(out, enc)
	}
	return out, nil
}

func nonNil(m map[string]int64) map[string]int64 {
	if m == nil {
		return map[string]int64{}
	}
	return m
}
