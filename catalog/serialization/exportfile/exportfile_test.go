package exportfile_test

import (
	"bytes"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/serialization/exportfile"
	"github.com/carolsimone/continuo/catalog/service/export"
)

const tenant = "t1"

var exportedAt = time.Date(2026, 10, 30, 2, 0, 0, 123456789, time.UTC)

func header(nodes, rels map[string]int64) export.Header {
	return export.Header{
		FormatVersion: export.FormatVersion, TenantID: tenant, ExportedAt: exportedAt,
		CatalogVersion: "test", Nodes: nodes, Relationships: rels,
	}
}

func node(label string, props map[string]any) export.NodeRecord {
	all := map[string]any{"tenant_id": tenant}
	for k, v := range props {
		all[k] = v
	}
	return export.NodeRecord{Labels: []string{label}, Props: all}
}

func ref(label string, key ...any) export.NodeRef { return export.NodeRef{Label: label, Key: key} }

// write renders h and records through the Writer and fails the test on any
// error.
func write(t *testing.T, h export.Header, records ...any) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := exportfile.NewWriter(&buf)
	if err := w.WriteHeader(h); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}
	for _, rec := range records {
		var err error
		switch r := rec.(type) {
		case export.NodeRecord:
			err = w.WriteNode(r)
		case export.RelRecord:
			err = w.WriteRel(r)
		default:
			t.Fatalf("unknown record %T", rec)
		}
		if err != nil {
			t.Fatalf("write %+v: %v", rec, err)
		}
	}
	return buf.Bytes()
}

// smallGraph is two nodes of composite and simple identity joined by one
// relationship.
func smallGraph() (export.Header, []any) {
	return header(map[string]int64{"Node": 1, "NodeVersion": 1}, map[string]int64{"CURRENT": 1}),
		[]any{
			node("Node", map[string]any{"unique_id": "analytics.table_a", "active": true, "seq": int64(7)}),
			node("NodeVersion", map[string]any{"unique_id": "analytics.table_a", "content_hash": "9f2c"}),
			export.RelRecord{
				Type: "CURRENT", From: ref("Node", "analytics.table_a"), To: ref("NodeVersion", "analytics.table_a", "9f2c"),
				Props: map[string]any{"promoted_at": time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)},
			},
		}
}

// TestExportfile_LineLayout pins the bytes of the header, a node and a
// relationship line.
func TestExportfile_LineLayout(t *testing.T) {
	h, records := smallGraph()
	got := string(write(t, h, records...))
	want := `{"format_version":1,"tenant_id":"t1","exported_at":"2026-10-30T02:00:00.123456789Z","catalog_version":"test","nodes":{"Node":1,"NodeVersion":1},"relationships":{"CURRENT":1}}` + "\n" +
		`{"node":["Node"],"props":{"active":true,"seq":{"$int":"7"},"tenant_id":"t1","unique_id":"analytics.table_a"}}` + "\n" +
		`{"node":["NodeVersion"],"props":{"content_hash":"9f2c","tenant_id":"t1","unique_id":"analytics.table_a"}}` + "\n" +
		`{"rel":"CURRENT","from":["Node","analytics.table_a"],"to":["NodeVersion","analytics.table_a","9f2c"],"props":{"promoted_at":{"$datetime":"2026-10-20T09:00:00Z[UTC]"}}}` + "\n"
	if got != want {
		t.Errorf("file =\n%s\nwant\n%s", got, want)
	}
}

func TestExportfile_ReaderReturnsTheRecordsInOrder(t *testing.T) {
	h, records := smallGraph()
	r, err := exportfile.NewReader(bytes.NewReader(write(t, h, records...)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got := r.Header()
	if got.TenantID != tenant || !got.ExportedAt.Equal(exportedAt) || got.CatalogVersion != "test" ||
		got.FormatVersion != export.FormatVersion || got.Nodes["NodeVersion"] != 1 || got.Relationships["CURRENT"] != 1 {
		t.Errorf("Header = %+v", got)
	}
	for i := range records {
		rec, err := r.Next()
		if err != nil {
			t.Fatalf("Next %d: %v", i, err)
		}
		switch want := records[i].(type) {
		case export.NodeRecord:
			n, ok := rec.(export.NodeRecord)
			if !ok || n.Labels[0] != want.Labels[0] || n.Props["unique_id"] != want.Props["unique_id"] {
				t.Errorf("record %d = %#v, want %#v", i, rec, want)
			}
		case export.RelRecord:
			rel, ok := rec.(export.RelRecord)
			if !ok || rel.Type != "CURRENT" || rel.To.Key[1] != "9f2c" ||
				!rel.Props["promoted_at"].(time.Time).Equal(want.Props["promoted_at"].(time.Time)) {
				t.Errorf("record %d = %#v, want %#v", i, rec, want)
			}
		}
	}
	for range 2 {
		if rec, err := r.Next(); !errors.Is(err, io.EOF) {
			t.Fatalf("Next after the last record = %v, %v, want io.EOF", rec, err)
		}
	}
}

func TestExportfile_ReaderKeepsEveryValueType(t *testing.T) {
	props := map[string]any{
		"s":       "héllo 世界 🚀\nsecond line\t\"quoted\" <tag> & \\",
		"min":     int64(math.MinInt64),
		"max":     int64(math.MaxInt64),
		"tenth":   0.1,
		"negzero": math.Copysign(0, -1),
		"nan":     math.NaN(),
		"inf":     math.Inf(-1),
		"yes":     true,
		"no":      false,
		"utc":     time.Date(2026, 10, 30, 2, 0, 0, 123456789, time.UTC),
		"offset":  time.Date(2026, 10, 30, 2, 0, 0, 1, time.FixedZone("Offset", 0)),
		"strings": []any{"a", "b"},
		"ints":    []any{int64(1), int64(math.MinInt64)},
		"empty":   []any{},
		"long":    strings.Repeat("x", 3<<20),
	}
	n := node("Node", props)
	data := write(t, header(map[string]int64{"Node": 1}, nil), n)
	r, err := exportfile.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	rec, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	got := rec.(export.NodeRecord).Props
	for k, want := range n.Props {
		if !sameValue(got[k], want) {
			t.Errorf("property %s = %#v, want %#v", k, got[k], want)
		}
	}
	if len(got) != len(n.Props) {
		t.Errorf("read %d properties, want %d", len(got), len(n.Props))
	}
}

// sameValue reports whether a and b are the same property value: the same Go
// type, the same float bits (any NaN equals any NaN), and the same instant,
// location and offset of a time.
func sameValue(a, b any) bool {
	switch x := a.(type) {
	case float64:
		y, ok := b.(float64)
		if !ok {
			return false
		}
		if math.IsNaN(x) || math.IsNaN(y) {
			return math.IsNaN(x) && math.IsNaN(y)
		}
		return math.Float64bits(x) == math.Float64bits(y)
	case time.Time:
		y, ok := b.(time.Time)
		if !ok {
			return false
		}
		_, xo := x.Zone()
		_, yo := y.Zone()
		return x.Equal(y) && x.Location().String() == y.Location().String() && xo == yo
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !sameValue(x[i], y[i]) {
				return false
			}
		}
		return true
	default:
		return a == b
	}
}

func TestExportfile_ValidateAcceptsAWellFormedFile(t *testing.T) {
	h, records := smallGraph()
	got, err := exportfile.Validate(bytes.NewReader(write(t, h, records...)), tenant)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if got.TenantID != tenant || got.Nodes["Node"] != 1 || got.Relationships["CURRENT"] != 1 {
		t.Errorf("Validate returned header %+v", got)
	}
}

func TestExportfile_ValidateAcceptsAnEmptyGraph(t *testing.T) {
	data := write(t, header(nil, nil))
	if _, err := exportfile.Validate(bytes.NewReader(data), tenant); err != nil {
		t.Fatalf("Validate of a header-only export: %v", err)
	}
}

// lines joins raw lines into a file, each ending in a newline.
func lines(ls ...string) []byte { return []byte(strings.Join(ls, "\n") + "\n") }

const (
	headerLine1Node = `{"format_version":1,"tenant_id":"t1","exported_at":"2026-10-30T02:00:00Z","catalog_version":"x","nodes":{"Node":1},"relationships":{}}`
	nodeA           = `{"node":["Node"],"props":{"tenant_id":"t1","unique_id":"a"}}`
)

func TestExportfile_ValidateRefusesLabelOutsideSchema(t *testing.T) {
	cases := map[string][]byte{
		"a node's label": lines(headerLine1Node, `{"node":["Table"],"props":{"tenant_id":"t1","unique_id":"a"}}`),
		"a second label": lines(headerLine1Node, `{"node":["Node","Run"],"props":{"tenant_id":"t1","unique_id":"a"}}`),
		"a label that injects Cypher": lines(headerLine1Node,
			`{"node":["Node {tenant_id: 'other'}) DETACH DELETE (n"],"props":{"tenant_id":"t1","unique_id":"a"}}`),
		"the header's counts": lines(`{"format_version":1,"tenant_id":"t1","exported_at":"2026-10-30T02:00:00Z","catalog_version":"x","nodes":{"Table":1},"relationships":{}}`,
			`{"node":["Table"],"props":{"tenant_id":"t1","unique_id":"a"}}`),
		"a relationship end": lines(
			`{"format_version":1,"tenant_id":"t1","exported_at":"2026-10-30T02:00:00Z","catalog_version":"x","nodes":{"Node":1},"relationships":{"DEPENDS_ON":1}}`,
			nodeA, `{"rel":"DEPENDS_ON","from":["Node","a"],"to":["Table","a"],"props":{}}`),
	}
	for name, data := range cases {
		if _, err := exportfile.Validate(bytes.NewReader(data), tenant); !errors.Is(err, export.ErrOutsideSchema) {
			t.Errorf("%s: Validate = %v, want ErrOutsideSchema", name, err)
		}
	}
}

func TestExportfile_ValidateRefusesRelationshipTypeOutsideSchema(t *testing.T) {
	h := `{"format_version":1,"tenant_id":"t1","exported_at":"2026-10-30T02:00:00Z","catalog_version":"x","nodes":{"Node":1},"relationships":{"DEPENDS_ON":1}}`
	cases := map[string][]byte{
		"a retired type":    lines(h, nodeA, `{"rel":"PREVIOUS","from":["Node","a"],"to":["Node","a"],"props":{}}`),
		"a lower-case type": lines(h, nodeA, `{"rel":"depends_on","from":["Node","a"],"to":["Node","a"],"props":{}}`),
		"a type that injects Cypher": lines(h, nodeA,
			`{"rel":"DEPENDS_ON]->(b) DETACH DELETE b //","from":["Node","a"],"to":["Node","a"],"props":{}}`),
		"the header's counts": lines(
			`{"format_version":1,"tenant_id":"t1","exported_at":"2026-10-30T02:00:00Z","catalog_version":"x","nodes":{"Node":1},"relationships":{"EXECUTES":1}}`,
			nodeA, `{"rel":"EXECUTES","from":["Node","a"],"to":["Node","a"],"props":{}}`),
	}
	for name, data := range cases {
		if _, err := exportfile.Validate(bytes.NewReader(data), tenant); !errors.Is(err, export.ErrOutsideSchema) {
			t.Errorf("%s: Validate = %v, want ErrOutsideSchema", name, err)
		}
	}
}

func TestExportfile_ValidateRefusesTenantMismatch(t *testing.T) {
	h, records := smallGraph()
	if _, err := exportfile.Validate(bytes.NewReader(write(t, h, records...)), "t2"); !errors.Is(err, export.ErrTenantMismatch) {
		t.Errorf("Validate for another tenant = %v, want ErrTenantMismatch", err)
	}
	foreignNode := lines(headerLine1Node, `{"node":["Node"],"props":{"tenant_id":"t2","unique_id":"a"}}`)
	if _, err := exportfile.Validate(bytes.NewReader(foreignNode), tenant); !errors.Is(err, export.ErrTenantMismatch) {
		t.Errorf("Validate of a node of another tenant = %v, want ErrTenantMismatch", err)
	}
	noTenant := lines(headerLine1Node, `{"node":["Node"],"props":{"unique_id":"a"}}`)
	if _, err := exportfile.Validate(bytes.NewReader(noTenant), tenant); !errors.Is(err, export.ErrTenantMismatch) {
		t.Errorf("Validate of a node without a tenant = %v, want ErrTenantMismatch", err)
	}
}

func TestExportfile_ValidateRefusesCountMismatch(t *testing.T) {
	two := []any{
		node("Node", map[string]any{"unique_id": "a"}),
		node("Node", map[string]any{"unique_id": "b"}),
	}
	// The Writer writes whatever counts it is given; the file is cut to two
	// nodes while the header says three.
	data := write(t, header(map[string]int64{"Node": 3}, nil), two...)
	if _, err := exportfile.Validate(bytes.NewReader(data), tenant); !errors.Is(err, exportfile.ErrCountMismatch) {
		t.Errorf("header says 3 nodes, file holds 2: Validate = %v, want ErrCountMismatch", err)
	}
	more := write(t, header(map[string]int64{"Node": 1}, nil), two...)
	if _, err := exportfile.Validate(bytes.NewReader(more), tenant); !errors.Is(err, exportfile.ErrCountMismatch) {
		t.Errorf("header says 1 node, file holds 2: Validate = %v, want ErrCountMismatch", err)
	}
	h, records := smallGraph()
	h.Relationships = map[string]int64{"CURRENT": 2}
	if _, err := exportfile.Validate(bytes.NewReader(write(t, h, records...)), tenant); !errors.Is(err, exportfile.ErrCountMismatch) {
		t.Errorf("header says 2 relationships, file holds 1: Validate = %v, want ErrCountMismatch", err)
	}
	h.Relationships = map[string]int64{"CURRENT": 1, "DEPENDS_ON": 1}
	if _, err := exportfile.Validate(bytes.NewReader(write(t, h, records...)), tenant); !errors.Is(err, exportfile.ErrCountMismatch) {
		t.Errorf("header counts a type the file lacks: Validate = %v, want ErrCountMismatch", err)
	}

	// The reader ends in the same error instead of io.EOF.
	r, err := exportfile.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err = r.Next(); err != nil {
			break
		}
	}
	if !errors.Is(err, exportfile.ErrCountMismatch) {
		t.Errorf("reader at the end of a short file = %v, want ErrCountMismatch", err)
	}
}

func TestExportfile_RefusesAPlainJSONNumber(t *testing.T) {
	// Read side: a plain number anywhere a property value or a key value goes.
	read := map[string][]byte{
		"a property": lines(headerLine1Node, `{"node":["Node"],"props":{"tenant_id":"t1","unique_id":"a","n":42}}`),
		"a float":    lines(headerLine1Node, `{"node":["Node"],"props":{"tenant_id":"t1","unique_id":"a","n":1.5}}`),
		"a list":     lines(headerLine1Node, `{"node":["Node"],"props":{"tenant_id":"t1","unique_id":"a","n":[1,2]}}`),
		"a key value": lines(
			`{"format_version":1,"tenant_id":"t1","exported_at":"2026-10-30T02:00:00Z","catalog_version":"x","nodes":{"Node":1},"relationships":{"DEPENDS_ON":1}}`,
			nodeA, `{"rel":"DEPENDS_ON","from":["Node",1],"to":["Node","a"],"props":{}}`),
		"a typed value's text": lines(headerLine1Node, `{"node":["Node"],"props":{"tenant_id":"t1","unique_id":"a","n":{"$int":42}}}`),
	}
	for name, data := range read {
		if _, err := exportfile.Validate(bytes.NewReader(data), tenant); !errors.Is(err, export.ErrUnsupportedValue) {
			t.Errorf("read, %s: Validate = %v, want ErrUnsupportedValue", name, err)
		}
		r, err := exportfile.NewReader(bytes.NewReader(data))
		if err != nil {
			t.Fatalf("read, %s: NewReader: %v", name, err)
		}
		for err == nil {
			_, err = r.Next()
		}
		if !errors.Is(err, export.ErrUnsupportedValue) {
			t.Errorf("read, %s: Next = %v, want ErrUnsupportedValue", name, err)
		}
	}

	// Write side: a Go number that is not an int64 or a float64.
	for name, v := range map[string]any{
		"int": 42, "int32": int32(42), "uint64": uint64(42), "float32": float32(1.5), "list of int": []any{1},
	} {
		w := exportfile.NewWriter(io.Discard)
		if err := w.WriteHeader(header(map[string]int64{"Node": 1}, nil)); err != nil {
			t.Fatal(err)
		}
		err := w.WriteNode(node("Node", map[string]any{"unique_id": "a", "n": v}))
		if !errors.Is(err, export.ErrUnsupportedValue) {
			t.Errorf("write, %s: WriteNode = %v, want ErrUnsupportedValue", name, err)
		}
	}
}

// TestExportfile_ValidateRefusesAMalformedFile feeds Validate files that are
// cut short, garbled or shaped otherwise than the Writer writes them: each is
// an error, and none panics.
func TestExportfile_ValidateRefusesAMalformedFile(t *testing.T) {
	h, records := smallGraph()
	good := write(t, h, records...)
	cases := map[string][]byte{
		"empty file":                  nil,
		"only a newline":              []byte("\n"),
		"header without its newline":  []byte(headerLine1Node),
		"cut inside the header":       good[:40],
		"cut inside a record":         good[:len(good)-30],
		"last line without a newline": good[:len(good)-1],
		"garbage":                     []byte("\x1f\x8b\x08\x00 not json at all\n"),
		"garbage after the header":    lines(headerLine1Node, `this is not json`),
		"a blank line":                lines(headerLine1Node, "", nodeA),
		"invalid UTF-8":               lines(headerLine1Node, "{\"node\":[\"Node\"],\"props\":{\"tenant_id\":\"t1\",\"unique_id\":\"\xff\"}}"),
		"two values on one line":      lines(headerLine1Node, nodeA+" "+nodeA),
		"a record first":              lines(nodeA),
		"a JSON array header":         lines(`[1,2,3]`, nodeA),
		"a header with an unknown member": lines(
			`{"format_version":1,"tenant_id":"t1","exported_at":"2026-10-30T02:00:00Z","catalog_version":"x","nodes":{"Node":1},"relationships":{},"extra":1}`, nodeA),
		"a header member twice": lines(
			`{"format_version":1,"tenant_id":"t1","tenant_id":"t1","exported_at":"2026-10-30T02:00:00Z","catalog_version":"x","nodes":{"Node":1},"relationships":{}}`, nodeA),
		"a header without its tenant": lines(
			`{"format_version":1,"exported_at":"2026-10-30T02:00:00Z","catalog_version":"x","nodes":{"Node":1},"relationships":{}}`, nodeA),
		"another format version": lines(
			`{"format_version":2,"tenant_id":"t1","exported_at":"2026-10-30T02:00:00Z","catalog_version":"x","nodes":{"Node":1},"relationships":{}}`, nodeA),
		"a fractional count": lines(
			`{"format_version":1,"tenant_id":"t1","exported_at":"2026-10-30T02:00:00Z","catalog_version":"x","nodes":{"Node":1.5},"relationships":{}}`, nodeA),
		"a bad export time": lines(
			`{"format_version":1,"tenant_id":"t1","exported_at":"yesterday","catalog_version":"x","nodes":{"Node":1},"relationships":{}}`, nodeA),
		"a record with an unknown member": lines(headerLine1Node, `{"node":["Node"],"props":{"tenant_id":"t1","unique_id":"a"},"x":1}`),
		"a record that is node and rel":   lines(headerLine1Node, `{"node":["Node"],"rel":"CURRENT","props":{"tenant_id":"t1","unique_id":"a"}}`),
		"a record without props":          lines(headerLine1Node, `{"node":["Node"]}`),
		"null props":                      lines(headerLine1Node, `{"node":["Node"],"props":null}`),
		"a property named twice":          lines(headerLine1Node, `{"node":["Node"],"props":{"tenant_id":"t1","unique_id":"a","unique_id":"b"}}`),
		"labels that are not a list":      lines(headerLine1Node, `{"node":"Node","props":{"tenant_id":"t1","unique_id":"a"}}`),
		"a typed value with two members": lines(headerLine1Node,
			`{"node":["Node"],"props":{"tenant_id":"t1","unique_id":"a","n":{"$int":"1","$float":"1"}}}`),
		"an unknown typed value": lines(headerLine1Node, `{"node":["Node"],"props":{"tenant_id":"t1","unique_id":"a","n":{"$date":"2026-10-30"}}}`),
		"a null property":        lines(headerLine1Node, `{"node":["Node"],"props":{"tenant_id":"t1","unique_id":"a","n":null}}`),
		"a node after a relationship": lines(
			`{"format_version":1,"tenant_id":"t1","exported_at":"2026-10-30T02:00:00Z","catalog_version":"x","nodes":{"Node":2},"relationships":{"DEPENDS_ON":1}}`,
			nodeA, `{"rel":"DEPENDS_ON","from":["Node","a"],"to":["Node","a"],"props":{}}`,
			`{"node":["Node"],"props":{"tenant_id":"t1","unique_id":"b"}}`),
		"an end without a key": lines(
			`{"format_version":1,"tenant_id":"t1","exported_at":"2026-10-30T02:00:00Z","catalog_version":"x","nodes":{"Node":1},"relationships":{"DEPENDS_ON":1}}`,
			nodeA, `{"rel":"DEPENDS_ON","from":["Node"],"to":["Node","a"],"props":{}}`),
		"an end whose label is not text": lines(
			`{"format_version":1,"tenant_id":"t1","exported_at":"2026-10-30T02:00:00Z","catalog_version":"x","nodes":{"Node":1},"relationships":{"DEPENDS_ON":1}}`,
			nodeA, `{"rel":"DEPENDS_ON","from":[{"$int":"1"},"a"],"to":["Node","a"],"props":{}}`),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := exportfile.Validate(bytes.NewReader(data), tenant); err == nil {
				t.Errorf("Validate accepted it")
			}
		})
	}
}

func TestExportfile_ValidateRefusesADuplicateNode(t *testing.T) {
	data := write(t, header(map[string]int64{"NodeVersion": 2}, nil),
		node("NodeVersion", map[string]any{"unique_id": "a", "content_hash": "h", "version_seq": int64(1)}),
		node("NodeVersion", map[string]any{"unique_id": "a", "content_hash": "h", "version_seq": int64(2)}),
	)
	if _, err := exportfile.Validate(bytes.NewReader(data), tenant); !errors.Is(err, exportfile.ErrDuplicateNode) {
		t.Errorf("two node versions with one identity: Validate = %v, want ErrDuplicateNode", err)
	}
	distinct := write(t, header(map[string]int64{"NodeVersion": 2}, nil),
		node("NodeVersion", map[string]any{"unique_id": "a", "content_hash": "h"}),
		node("NodeVersion", map[string]any{"unique_id": "b", "content_hash": "h"}),
	)
	if _, err := exportfile.Validate(bytes.NewReader(distinct), tenant); err != nil {
		t.Errorf("two node versions that share one identity property: Validate = %v", err)
	}
}

func TestExportfile_ValidateRefusesAnUnresolvedEnd(t *testing.T) {
	h, records := smallGraph()
	rel := records[2].(export.RelRecord)
	rel.To = ref("NodeVersion", "analytics.table_a", "0000")
	records[2] = rel
	if _, err := exportfile.Validate(bytes.NewReader(write(t, h, records...)), tenant); !errors.Is(err, exportfile.ErrUnresolvedEnd) {
		t.Errorf("a relationship to a node the file lacks: Validate = %v, want ErrUnresolvedEnd", err)
	}
}

// TestExportfile_LeaseNode: a file may carry a tenant's export lease node,
// which the importer does not load, but no relationship of it.
func TestExportfile_LeaseNode(t *testing.T) {
	lease := node("Meta", map[string]any{"key": "export", "holder": "h", "kind": "export"})
	data := write(t, header(map[string]int64{"Meta": 1}, nil), lease)
	if _, err := exportfile.Validate(bytes.NewReader(data), tenant); err != nil {
		t.Errorf("Validate of a file with a lease node: %v", err)
	}
	withRel := lines(
		`{"format_version":1,"tenant_id":"t1","exported_at":"2026-10-30T02:00:00Z","catalog_version":"x","nodes":{"Meta":1,"Node":1},"relationships":{"DEPENDS_ON":1}}`,
		`{"node":["Meta"],"props":{"tenant_id":"t1","key":"export"}}`, nodeA,
		`{"rel":"DEPENDS_ON","from":["Node","a"],"to":["Meta","export"],"props":{}}`)
	if _, err := exportfile.Validate(bytes.NewReader(withRel), tenant); !errors.Is(err, export.ErrInvalidRecord) {
		t.Errorf("Validate of a relationship of the lease node = %v, want ErrInvalidRecord", err)
	}
}

func TestExportfile_WriterRefusesWhatTheReaderWouldRefuse(t *testing.T) {
	h, _ := smallGraph()
	newWriter := func(t *testing.T) *exportfile.Writer {
		w := exportfile.NewWriter(io.Discard)
		if err := w.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		return w
	}

	if err := exportfile.NewWriter(io.Discard).WriteNode(node("Node", map[string]any{"unique_id": "a"})); err == nil {
		t.Error("WriteNode before WriteHeader succeeded")
	}
	if err := newWriter(t).WriteHeader(h); err == nil {
		t.Error("a second WriteHeader succeeded")
	}
	bad := h
	bad.Nodes = map[string]int64{"Table": 1}
	if err := exportfile.NewWriter(io.Discard).WriteHeader(bad); !errors.Is(err, export.ErrOutsideSchema) {
		t.Errorf("WriteHeader of a label outside the schema = %v, want ErrOutsideSchema", err)
	}
	if err := newWriter(t).WriteNode(node("Table", nil)); !errors.Is(err, export.ErrOutsideSchema) {
		t.Errorf("WriteNode of a label outside the schema = %v, want ErrOutsideSchema", err)
	}
	foreign := export.NodeRecord{Labels: []string{"Node"}, Props: map[string]any{"tenant_id": "t2"}}
	if err := newWriter(t).WriteNode(foreign); !errors.Is(err, export.ErrTenantMismatch) {
		t.Errorf("WriteNode of another tenant's node = %v, want ErrTenantMismatch", err)
	}
	if err := newWriter(t).WriteRel(export.RelRecord{Type: "EXECUTES", From: ref("Node", "a"), To: ref("Node", "b")}); !errors.Is(err, export.ErrOutsideSchema) {
		t.Errorf("WriteRel of a type outside the schema = %v, want ErrOutsideSchema", err)
	}
	if err := newWriter(t).WriteNode(node("Node", map[string]any{"at": time.Now()})); !errors.Is(err, export.ErrUnsupportedValue) {
		t.Errorf("WriteNode of a time in the Local zone = %v, want ErrUnsupportedValue", err)
	}

	w := newWriter(t)
	if err := w.WriteRel(export.RelRecord{Type: "DEPENDS_ON", From: ref("Node", "a"), To: ref("Node", "b")}); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteNode(node("Node", map[string]any{"unique_id": "c"})); err == nil {
		t.Error("WriteNode after WriteRel succeeded")
	}
}

// FuzzValidate checks that no input makes Validate or the Reader panic.
func FuzzValidate(f *testing.F) {
	h, records := smallGraph()
	var buf bytes.Buffer
	w := exportfile.NewWriter(&buf)
	if err := w.WriteHeader(h); err != nil {
		f.Fatal(err)
	}
	for _, rec := range records {
		switch r := rec.(type) {
		case export.NodeRecord:
			_ = w.WriteNode(r)
		case export.RelRecord:
			_ = w.WriteRel(r)
		}
	}
	good := buf.Bytes()
	f.Add(good)
	f.Add(good[:len(good)/2])
	f.Add([]byte{})
	f.Add([]byte("{}\n{}\n"))
	f.Add([]byte(headerLine1Node + "\n" + `{"node":["Node"],"props":{"tenant_id":"t1","n":{"$float":"NaN"},"l":[[{"$datetime":"x"}]]}}` + "\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = exportfile.Validate(bytes.NewReader(data), tenant)
		r, err := exportfile.NewReader(bytes.NewReader(data))
		if err != nil {
			return
		}
		for range 1000 {
			if _, err := r.Next(); err != nil {
				return
			}
		}
	})
}

// A tampered file can hold what a check of typed text would pass and the
// graph would refuse after the wipe: a key the graph cannot match (NaN), two
// identities the graph takes for one (1 and 1.0), or two processed-message
// markers of one group and outbox entry id, which the graph's uniqueness
// constraint refuses. Validate refuses each before a restore touches the
// graph.
func TestExportfile_ValidateRefusesWhatTheGraphCouldNotLoad(t *testing.T) {
	cases := map[string]struct {
		body string
		want error
	}{
		"a NaN key": {
			`{"format_version":1,"tenant_id":"t1","exported_at":"2026-10-30T02:00:00Z","catalog_version":"x","nodes":{"NodeVersion":1},"relationships":{}}` + "\n" +
				`{"node":["NodeVersion"],"props":{"tenant_id":"t1","unique_id":"a","content_hash":{"$float":"NaN"}}}` + "\n",
			export.ErrInvalidRecord,
		},
		"1 and 1.0 as unique_id": {
			`{"format_version":1,"tenant_id":"t1","exported_at":"2026-10-30T02:00:00Z","catalog_version":"x","nodes":{"Node":2},"relationships":{}}` + "\n" +
				`{"node":["Node"],"props":{"tenant_id":"t1","unique_id":{"$int":"1"}}}` + "\n" +
				`{"node":["Node"],"props":{"tenant_id":"t1","unique_id":{"$float":"1"}}}` + "\n",
			export.ErrInvalidRecord,
		},
		"a relationship end named by a number": {
			`{"format_version":1,"tenant_id":"t1","exported_at":"2026-10-30T02:00:00Z","catalog_version":"x","nodes":{"Node":1},"relationships":{"DEPENDS_ON":1}}` + "\n" +
				`{"node":["Node"],"props":{"tenant_id":"t1","unique_id":"a"}}` + "\n" +
				`{"rel":"DEPENDS_ON","from":["Node","a"],"to":["Node",{"$float":"1"}],"props":{}}` + "\n",
			export.ErrInvalidRecord,
		},
		"two markers of one outbox entry": {
			`{"format_version":1,"tenant_id":"t1","exported_at":"2026-10-30T02:00:00Z","catalog_version":"x","nodes":{"ProcessedMessage":2},"relationships":{}}` + "\n" +
				`{"node":["ProcessedMessage"],"props":{"tenant_id":"t1","group":"g","message_id":"1-0","outbox_entry_id":"e1"}}` + "\n" +
				`{"node":["ProcessedMessage"],"props":{"tenant_id":"t1","group":"g","message_id":"2-0","outbox_entry_id":"e1"}}` + "\n",
			exportfile.ErrDuplicateNode,
		},
		"a marker whose outbox entry id is a number": {
			`{"format_version":1,"tenant_id":"t1","exported_at":"2026-10-30T02:00:00Z","catalog_version":"x","nodes":{"ProcessedMessage":1},"relationships":{}}` + "\n" +
				`{"node":["ProcessedMessage"],"props":{"tenant_id":"t1","group":"g","message_id":"1-0","outbox_entry_id":{"$int":"1"}}}` + "\n",
			export.ErrInvalidRecord,
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := exportfile.Validate(strings.NewReader(c.body), tenant); !errors.Is(err, c.want) {
				t.Errorf("Validate = %v, want %v", err, c.want)
			}
		})
	}

	distinct := write(t, header(map[string]int64{"ProcessedMessage": 3}, nil),
		node("ProcessedMessage", map[string]any{"group": "g", "message_id": "1-0", "outbox_entry_id": "e1"}),
		node("ProcessedMessage", map[string]any{"group": "h", "message_id": "1-0", "outbox_entry_id": "e1"}),
		node("ProcessedMessage", map[string]any{"group": "g", "message_id": "2-0"}),
	)
	if _, err := exportfile.Validate(bytes.NewReader(distinct), tenant); err != nil {
		t.Errorf("markers of one outbox entry in two groups, and one with none: Validate = %v", err)
	}
}
