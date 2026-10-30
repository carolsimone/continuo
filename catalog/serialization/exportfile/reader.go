package exportfile

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/carolsimone/continuo/catalog/domain/export"
	"github.com/carolsimone/continuo/catalog/service/ports"
)

var (
	// ErrMalformed means a line is not one the Writer writes: not JSON of the
	// layout, cut short, blank, longer than a line may be, or not UTF-8.
	ErrMalformed = errors.New("exportfile: malformed export line")
	// ErrCountMismatch means the file holds other node or relationship counts
	// than its header says, as a file cut short at a line boundary does.
	ErrCountMismatch = errors.New("exportfile: the records disagree with the header's counts")
	// ErrDuplicateNode means two nodes of the file share one identity.
	ErrDuplicateNode = errors.New("exportfile: two nodes with one identity")
	// ErrUnresolvedEnd means a relationship names a node the file does not
	// hold.
	ErrUnresolvedEnd = errors.New("exportfile: a relationship end names no node of the export")
)

// maxLineBytes bounds one line, so a damaged file cannot make the reader
// buffer without limit.
const maxLineBytes = 64 << 20

// Reader reads an export file line by line. Each record passes the checks
// of package export against the header's tenant, every node comes before the
// first relationship, and the end of the file is io.EOF only when the records
// match the header's counts. Once Next returns an error it returns it again.
// It is not safe for concurrent use.
type Reader struct {
	src      *bufio.Reader
	limit    int
	line     int
	header   export.Header
	seenRels bool
	nodes    map[string]int64
	rels     map[string]int64
	err      error
}

var _ ports.RecordReader = (*Reader)(nil)

// NewReader reads and checks the header line of r.
func NewReader(r io.Reader) (*Reader, error) {
	return newReader(r, maxLineBytes)
}

func newReader(r io.Reader, limit int) (*Reader, error) {
	rd := &Reader{
		src: bufio.NewReaderSize(r, 64<<10), limit: limit,
		nodes: map[string]int64{}, rels: map[string]int64{},
	}
	line, err := rd.readLine()
	if errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: an empty file, with no header", ErrMalformed)
	}
	if err != nil {
		return nil, err
	}
	h, err := decodeHeader(line)
	if err == nil {
		err = h.Check()
	}
	if err != nil {
		return nil, fmt.Errorf("line 1, the header: %w", err)
	}
	rd.header = h
	return rd, nil
}

// Header returns the file's header.
func (r *Reader) Header() export.Header {
	h := r.header
	h.Nodes = maps.Clone(h.Nodes)
	h.Relationships = maps.Clone(h.Relationships)
	return h
}

// Next returns the next record, an export.NodeRecord or an export.RelRecord,
// and io.EOF after the last.
func (r *Reader) Next() (any, error) {
	if r.err != nil {
		return nil, r.err
	}
	rec, err := r.next()
	if err != nil {
		r.err = err
	}
	return rec, err
}

func (r *Reader) next() (any, error) {
	line, err := r.readLine()
	if errors.Is(err, io.EOF) {
		if err := r.checkCounts(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	if err != nil {
		return nil, err
	}
	rec, err := decodeRecord(line)
	if err != nil {
		return nil, fmt.Errorf("line %d: %w", r.line, err)
	}
	switch x := rec.(type) {
	case export.NodeRecord:
		if r.seenRels {
			return nil, fmt.Errorf("line %d: %w: a node after the relationships", r.line, ErrMalformed)
		}
		if err := x.Check(r.header.TenantID); err != nil {
			return nil, fmt.Errorf("line %d: %w", r.line, err)
		}
		for _, l := range x.Labels {
			r.nodes[l]++
		}
	case export.RelRecord:
		r.seenRels = true
		if err := x.Check(); err != nil {
			return nil, fmt.Errorf("line %d: %w", r.line, err)
		}
		r.rels[x.Type]++
	}
	return rec, nil
}

// checkCounts compares what the file held with the header's counts.
func (r *Reader) checkCounts() error {
	if err := sameCounts("label", r.header.Nodes, r.nodes); err != nil {
		return err
	}
	return sameCounts("relationship type", r.header.Relationships, r.rels)
}

func sameCounts(what string, want, got map[string]int64) error {
	names := slices.Sorted(maps.Keys(want))
	for name := range got {
		if _, ok := want[name]; !ok {
			names = append(names, name)
		}
	}
	for _, name := range names {
		if want[name] != got[name] {
			return fmt.Errorf("%w: %s %s: the header says %d, the file holds %d", ErrCountMismatch, what, name, want[name], got[name])
		}
	}
	return nil
}

// readLine returns the next line without its newline, and io.EOF at the end
// of the file. A last line without a newline means the file was cut short.
func (r *Reader) readLine() ([]byte, error) {
	var line []byte
	for {
		chunk, err := r.src.ReadSlice('\n')
		if len(line)+len(chunk) > r.limit {
			return nil, fmt.Errorf("%w: line %d is longer than %d bytes", ErrMalformed, r.line+1, r.limit)
		}
		line = append(line, chunk...)
		switch {
		case err == nil:
			r.line++
			body := line[:len(line)-1]
			if len(body) == 0 {
				return nil, fmt.Errorf("%w: line %d is blank", ErrMalformed, r.line)
			}
			if !utf8.Valid(body) {
				return nil, fmt.Errorf("%w: line %d is not UTF-8", ErrMalformed, r.line)
			}
			return body, nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			if len(line) == 0 {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("%w: line %d ends without a newline, so the file is cut short", ErrMalformed, r.line+1)
		default:
			return nil, fmt.Errorf("exportfile: read line %d: %w", r.line+1, err)
		}
	}
}

// fields returns a line's members by name, refusing a member outside allowed
// and one of required that is missing.
func fields(members []member, allowed, required []string) (map[string]json.RawMessage, error) {
	out := make(map[string]json.RawMessage, len(members))
	for _, m := range members {
		if !slices.Contains(allowed, m.name) {
			return nil, fmt.Errorf("%w: unknown member %q", ErrMalformed, m.name)
		}
		out[m.name] = m.value
	}
	for _, name := range required {
		if _, ok := out[name]; !ok {
			return nil, fmt.Errorf("%w: no member %q", ErrMalformed, name)
		}
	}
	return out, nil
}

// unmarshal decodes one member of a line into v.
func unmarshal(m map[string]json.RawMessage, name string, v any) error {
	if err := json.Unmarshal(m[name], v); err != nil {
		return fmt.Errorf("%w: member %q: %v", ErrMalformed, name, err)
	}
	return nil
}

var headerMembers = []string{"format_version", "tenant_id", "exported_at", "catalog_version", "nodes", "relationships"}

func decodeHeader(line []byte) (export.Header, error) {
	members, err := decodeObject(line)
	if err != nil {
		return export.Header{}, err
	}
	m, err := fields(members, headerMembers, headerMembers)
	if err != nil {
		return export.Header{}, err
	}
	var h export.Header
	var exportedAt string
	for _, f := range []struct {
		name string
		v    any
	}{
		{"format_version", &h.FormatVersion}, {"tenant_id", &h.TenantID},
		{"exported_at", &exportedAt}, {"catalog_version", &h.CatalogVersion},
	} {
		if err := unmarshal(m, f.name, f.v); err != nil {
			return export.Header{}, err
		}
	}
	at, err := time.Parse(time.RFC3339Nano, exportedAt)
	if err != nil {
		return export.Header{}, fmt.Errorf("%w: exported_at: %v", ErrMalformed, err)
	}
	h.ExportedAt = at.UTC()
	if h.Nodes, err = decodeCounts(m["nodes"]); err != nil {
		return export.Header{}, fmt.Errorf("nodes: %w", err)
	}
	if h.Relationships, err = decodeCounts(m["relationships"]); err != nil {
		return export.Header{}, fmt.Errorf("relationships: %w", err)
	}
	return h, nil
}

// decodeCounts reads an object of names to whole numbers.
func decodeCounts(raw json.RawMessage) (map[string]int64, error) {
	members, err := decodeObject(raw)
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(members))
	for _, m := range members {
		var n int64
		if err := json.Unmarshal(m.value, &n); err != nil {
			return nil, fmt.Errorf("%w: count of %q: %v", ErrMalformed, m.name, err)
		}
		out[m.name] = n
	}
	return out, nil
}

var (
	nodeMembers = []string{"node", "props"}
	relMembers  = []string{"rel", "from", "to", "props"}
)

// decodeRecord reads a node or a relationship line.
func decodeRecord(line []byte) (any, error) {
	members, err := decodeObject(line)
	if err != nil {
		return nil, err
	}
	var isNode, isRel bool
	for _, m := range members {
		isNode = isNode || m.name == "node"
		isRel = isRel || m.name == "rel"
	}
	switch {
	case isNode && !isRel:
		m, err := fields(members, nodeMembers, nodeMembers)
		if err != nil {
			return nil, err
		}
		var n export.NodeRecord
		if err := unmarshal(m, "node", &n.Labels); err != nil {
			return nil, err
		}
		if n.Props, err = decodeProps(m["props"]); err != nil {
			return nil, err
		}
		return n, nil
	case isRel && !isNode:
		m, err := fields(members, relMembers, relMembers)
		if err != nil {
			return nil, err
		}
		var r export.RelRecord
		if err := unmarshal(m, "rel", &r.Type); err != nil {
			return nil, err
		}
		if r.From, err = decodeRef(m["from"]); err != nil {
			return nil, fmt.Errorf("from: %w", err)
		}
		if r.To, err = decodeRef(m["to"]); err != nil {
			return nil, fmt.Errorf("to: %w", err)
		}
		if r.Props, err = decodeProps(m["props"]); err != nil {
			return nil, err
		}
		return r, nil
	default:
		return nil, fmt.Errorf("%w: a line that is neither a node nor a relationship", ErrMalformed)
	}
}

func decodeProps(raw json.RawMessage) (map[string]any, error) {
	members, err := decodeObject(raw)
	if err != nil {
		return nil, fmt.Errorf("props: %w", err)
	}
	out := make(map[string]any, len(members))
	for _, m := range members {
		v, err := decodeValue(m.value)
		if err != nil {
			return nil, fmt.Errorf("property %q: %w", m.name, err)
		}
		out[m.name] = v
	}
	return out, nil
}

// decodeRef reads [label, key value...].
func decodeRef(raw json.RawMessage) (export.NodeRef, error) {
	var parts []json.RawMessage
	if err := json.Unmarshal(raw, &parts); err != nil || len(parts) == 0 {
		return export.NodeRef{}, fmt.Errorf("%w: an end is a list of its label and key values", ErrMalformed)
	}
	var ref export.NodeRef
	if err := json.Unmarshal(parts[0], &ref.Label); err != nil {
		return export.NodeRef{}, fmt.Errorf("%w: an end's label is not text", ErrMalformed)
	}
	ref.Key = make([]any, 0, len(parts)-1)
	for _, p := range parts[1:] {
		v, err := decodeValue(p)
		if err != nil {
			return export.NodeRef{}, fmt.Errorf("a :%s's key: %w", ref.Label, err)
		}
		ref.Key = append(ref.Key, v)
	}
	return ref, nil
}

// Validate reads a whole export file and touches nothing else. It returns
// the header when the file is one a restore of tenantID can load exactly:
// the header names tenantID; every line decodes; every label, type and
// value is the catalog graph's; the counts match the header; no two nodes
// share an identity; and every relationship names a node of the file. It
// holds one identity per node, so its memory grows with the export.
func Validate(r io.Reader, tenantID string) (export.Header, error) {
	rd, err := NewReader(r)
	if err != nil {
		return export.Header{}, err
	}
	h := rd.Header()
	if h.TenantID != tenantID {
		return export.Header{}, fmt.Errorf("the export is of tenant %q, not %q: %w", h.TenantID, tenantID, export.ErrTenantMismatch)
	}
	ids := map[string]struct{}{}
	for {
		rec, err := rd.Next()
		if errors.Is(err, io.EOF) {
			return h, nil
		}
		if err != nil {
			return export.Header{}, err
		}
		switch x := rec.(type) {
		case export.NodeRecord:
			if x.IsLease() {
				continue
			}
			labels, _ := x.SchemaLabels() // Next has checked them
			for _, l := range labels {
				key, ok := x.Identity(l)
				if !ok {
					continue
				}
				id, err := identityText(export.NodeRef{Label: string(l), Key: key})
				if err != nil {
					return export.Header{}, fmt.Errorf("line %d: %w", rd.line, err)
				}
				if _, dup := ids[id]; dup {
					return export.Header{}, fmt.Errorf("line %d: %w: a second :%s %v", rd.line, ErrDuplicateNode, l, key)
				}
				ids[id] = struct{}{}
			}
		case export.RelRecord:
			for _, end := range []export.NodeRef{x.From, x.To} {
				id, err := identityText(end)
				if err != nil {
					return export.Header{}, fmt.Errorf("line %d: %w", rd.line, err)
				}
				if _, ok := ids[id]; !ok {
					return export.Header{}, fmt.Errorf("line %d: %w: :%s %v", rd.line, ErrUnresolvedEnd, end.Label, end.Key)
				}
			}
		}
	}
}

// identityText renders a node's label and key values as one text, with each
// value in its typed form, so the string "1" and the integer 1 differ.
func identityText(ref export.NodeRef) (string, error) {
	enc, err := encodeRef(ref)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(enc)
	if err != nil {
		return "", fmt.Errorf("exportfile: render an identity: %w", err)
	}
	return string(b), nil
}
