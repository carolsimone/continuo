package exportfile

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
	_ "time/tzdata" // the named-zone cases must not depend on the host's zone database

	"github.com/carolsimone/continuo/catalog/domain/export"
)

// encodeJSON renders v as the file writes a property value.
func encodeJSON(t *testing.T, v any) (string, error) {
	t.Helper()
	enc, err := encodeValue(v)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	e := json.NewEncoder(&buf)
	e.SetEscapeHTML(false)
	if err := e.Encode(enc); err != nil {
		t.Fatalf("marshal %#v: %v", enc, err)
	}
	return strings.TrimSuffix(buf.String(), "\n"), nil
}

func TestValue_EncodesEachTypeAsTheFormatSays(t *testing.T) {
	berlin, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		v    any
		want string
	}{
		{"string", "héllo 世界\n\"x\" <y> & z", `"héllo 世界\n\"x\" <y> & z"`},
		{"empty string", "", `""`},
		{"true", true, `true`},
		{"false", false, `false`},
		{"null", nil, `null`},
		{"int", int64(42), `{"$int":"42"}`},
		{"negative int", int64(-7), `{"$int":"-7"}`},
		{"min int", int64(math.MinInt64), `{"$int":"-9223372036854775808"}`},
		{"max int", int64(math.MaxInt64), `{"$int":"9223372036854775807"}`},
		{"float", 1.5, `{"$float":"1.5"}`},
		{"tenth", 0.1, `{"$float":"0.1"}`},
		{"zero", 0.0, `{"$float":"0"}`},
		{"negative zero", math.Copysign(0, -1), `{"$float":"-0"}`},
		{"NaN", math.NaN(), `{"$float":"NaN"}`},
		{"+Inf", math.Inf(1), `{"$float":"+Inf"}`},
		{"-Inf", math.Inf(-1), `{"$float":"-Inf"}`},
		{"large", 1e21, `{"$float":"1e+21"}`},
		{"smallest", 5e-324, `{"$float":"5e-324"}`},
		{"largest", math.MaxFloat64, `{"$float":"1.7976931348623157e+308"}`},
		{"datetime at offset zero", time.Date(2026, 10, 30, 2, 0, 0, 1, time.FixedZone("Offset", 0)), `{"$datetime":"2026-10-30T02:00:00.000000001Z"}`},
		{"datetime at +02:00", time.Date(2026, 10, 30, 4, 0, 0, 0, time.FixedZone("Offset", 7200)), `{"$datetime":"2026-10-30T04:00:00+02:00"}`},
		{"datetime in UTC", time.Date(2026, 10, 30, 2, 0, 0, 500000000, time.UTC), `{"$datetime":"2026-10-30T02:00:00.5Z[UTC]"}`},
		{"datetime in a named zone", time.Date(2026, 10, 30, 3, 0, 0, 0, berlin), `{"$datetime":"2026-10-30T03:00:00+01:00[Europe/Berlin]"}`},
		{"list", []any{"a", int64(1), []any{}}, `["a",{"$int":"1"},[]]`},
		{"empty list", []any{}, `[]`},
		{"nil list", []any(nil), `[]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := encodeJSON(t, c.v)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if got != c.want {
				t.Errorf("encode = %s, want %s", got, c.want)
			}
			back, err := decodeValue([]byte(c.want))
			if err != nil {
				t.Fatalf("decode %s: %v", c.want, err)
			}
			want := c.v
			if l, ok := want.([]any); ok && l == nil {
				want = []any{}
			}
			if !sameValue(back, want) {
				t.Errorf("decode %s = %#v, want %#v", c.want, back, want)
			}
		})
	}
}

// sameValue reports whether a and b are the same property value: the same Go
// type, the same float bits (any NaN equals any NaN), and the same instant,
// location name and offset of a time.
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

// TestValue_DecodedOffsetIsTheDriversOffsetZone: a datetime written without a
// zone name comes back in a fixed zone named Offset, which the Neo4j driver
// sends as a datetime with an offset; one with a zone name comes back in that
// zone, which the driver sends by name.
func TestValue_DecodedOffsetIsTheDriversOffsetZone(t *testing.T) {
	v, err := decodeValue([]byte(`{"$datetime":"2026-10-30T02:00:00Z"}`))
	if err != nil {
		t.Fatal(err)
	}
	if name, off := v.(time.Time).Zone(); name != "Offset" || off != 0 {
		t.Errorf("zone = %q %d, want Offset 0", name, off)
	}
	v, err = decodeValue([]byte(`{"$datetime":"2026-10-30T02:00:00Z[UTC]"}`))
	if err != nil {
		t.Fatal(err)
	}
	if v.(time.Time).Location() != time.UTC {
		t.Errorf("location = %v, want time.UTC", v.(time.Time).Location())
	}
}

func TestValue_DecodeRefusesWhatTheWriterNeverWrites(t *testing.T) {
	cases := []string{
		`42`, `-1`, `1.5`, `1e3`, `0`,
		`[1]`, `["a",2]`,
		`{"$int":42}`, `{"$int":"042"}`, `{"$int":"+1"}`, `{"$int":" 1"}`, `{"$int":"1.0"}`, `{"$int":"9223372036854775808"}`, `{"$int":""}`,
		`{"$float":1.5}`, `{"$float":"1.50"}`, `{"$float":"inf"}`, `{"$float":"Inf"}`, `{"$float":"Infinity"}`, `{"$float":"nan"}`,
		`{"$float":"0x1p-2"}`, `{"$float":"1e400"}`, `{"$float":"1e21"}`, `{"$float":""}`,
		`{"$datetime":"2026-10-30T02:00:00+00:00"}`, `{"$datetime":"2026-10-30T02:00:00.100Z"}`, `{"$datetime":"2026-10-30"}`,
		`{"$datetime":"2026-10-30T02:00:00Z[Local]"}`, `{"$datetime":"2026-10-30T02:00:00Z[]"}`, `{"$datetime":"2026-10-30T02:00:00Z[Nowhere/Zone]"}`,
		`{"$datetime":"2026-10-30T02:00:00+02:00[UTC]"}`, `{"$datetime":"2026-10-30T02:00:00Z[UTC"}`, `{"$datetime":2026}`,
		`{"$int":"1","$float":"1"}`, `{"$int":"1","$int":"2"}`, `{"$date":"2026-10-30"}`, `{}`, `{"a":"b"}`,
		`{"$int":"1"} 2`, ``, `tru`, `"unterminated`,
	}
	for _, c := range cases {
		if v, err := decodeValue([]byte(c)); err == nil {
			t.Errorf("decode %s = %#v, want an error", c, v)
		}
	}
	if _, err := decodeValue([]byte(`7`)); !errors.Is(err, export.ErrUnsupportedValue) {
		t.Errorf("decode of a plain number = %v, want ErrUnsupportedValue", err)
	}
}

func TestValue_EncodeRefusesAnotherGoType(t *testing.T) {
	cases := map[string]any{
		"int":           42,
		"int32":         int32(42),
		"uint64":        uint64(42),
		"float32":       float32(1.5),
		"typed list":    []string{"a"},
		"map":           map[string]any{"a": "b"},
		"bytes":         []byte("a"),
		"pointer":       new(int64),
		"json number":   json.Number("42"),
		"local time":    time.Now(),
		"unnamed zone":  time.Date(2026, 10, 30, 0, 0, 0, 0, time.FixedZone("", 3600)),
		"invented zone": time.Date(2026, 10, 30, 0, 0, 0, 0, time.FixedZone("Somewhere", 3600)),
		"list of int":   []any{1},
		"invalid UTF-8": "\xff",
	}
	for name, v := range cases {
		if _, err := encodeValue(v); !errors.Is(err, export.ErrUnsupportedValue) {
			t.Errorf("%s: encode = %v, want ErrUnsupportedValue", name, err)
		}
	}
}

// TestReader_RefusesALineLongerThanItsLimit reads with a small limit, so the
// test need not build a line of the production size.
func TestReader_RefusesALineLongerThanItsLimit(t *testing.T) {
	head := `{"format_version":1,"tenant_id":"t1","exported_at":"2026-10-30T02:00:00Z","catalog_version":"x","nodes":{"Node":1},"relationships":{}}` + "\n"
	long := `{"node":["Node"],"props":{"tenant_id":"t1","unique_id":"` + strings.Repeat("a", 4096) + `"}}` + "\n"
	r, err := newReader(strings.NewReader(head+long), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); !errors.Is(err, ErrMalformed) {
		t.Errorf("Next of a line over the limit = %v, want ErrMalformed", err)
	}
	r, err = newReader(strings.NewReader(head+long), 8192)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != nil {
		t.Errorf("Next of a line under the limit = %v", err)
	}
}

// TestWriter_RefusesALineLongerThanAReaderAccepts writes with a small limit,
// so the test need not build a line of the production size: a record whose
// line a Reader with the same limit would refuse fails the export, naming
// the record and its size, and writes nothing; a line at the limit is
// written and read back. NewWriter and NewReader share maxLineBytes.
func TestWriter_RefusesALineLongerThanAReaderAccepts(t *testing.T) {
	if got := NewWriter(&bytes.Buffer{}).limit; got != maxLineBytes {
		t.Fatalf("NewWriter's limit = %d, want maxLineBytes (%d), the Reader's", got, maxLineBytes)
	}
	h := export.Header{
		FormatVersion: export.FormatVersion, TenantID: "t1", ExportedAt: time.Date(2026, 10, 30, 2, 0, 0, 0, time.UTC),
		CatalogVersion: "x", Nodes: map[string]int64{"NodeVersion": 1}, Relationships: map[string]int64{},
	}
	version := func(rawCode string) export.NodeRecord {
		return export.NodeRecord{Labels: []string{"NodeVersion"}, Props: map[string]any{
			"tenant_id": "t1", "unique_id": "svc.a", "content_hash": "h", "raw_code": rawCode,
		}}
	}
	const limit = 1024

	var buf bytes.Buffer
	w := newWriter(&buf, limit)
	if err := w.WriteHeader(h); err != nil {
		t.Fatal(err)
	}
	written := buf.Len()
	err := w.WriteNode(version(strings.Repeat("x", 2*limit)))
	if !errors.Is(err, ErrLineTooLong) {
		t.Fatalf("WriteNode of a line over the limit = %v, want ErrLineTooLong", err)
	}
	if msg := err.Error(); !strings.Contains(msg, "NodeVersion") || !strings.Contains(msg, "bytes") {
		t.Errorf("the error %q names neither the record nor its size", msg)
	}
	if buf.Len() != written {
		t.Errorf("the refused line wrote %d bytes", buf.Len()-written)
	}

	// The node line without raw_code, and the raw_code that fills it to
	// exactly the limit, newline included.
	empty := mustLine(t, version(""))
	atLimit := version(strings.Repeat("x", limit-empty))
	if err := w.WriteNode(atLimit); err != nil {
		t.Fatalf("WriteNode of a line at the limit = %v", err)
	}
	r, err := newReader(bytes.NewReader(buf.Bytes()), limit)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != nil {
		t.Errorf("a Reader with the same limit refuses the line the Writer wrote: %v", err)
	}
}

// mustLine returns the length, newline included, of n's line.
func mustLine(t *testing.T, n export.NodeRecord) int {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	h := export.Header{FormatVersion: export.FormatVersion, TenantID: "t1", ExportedAt: time.Date(2026, 10, 30, 2, 0, 0, 0, time.UTC), CatalogVersion: "x"}
	if err := w.WriteHeader(h); err != nil {
		t.Fatal(err)
	}
	before := buf.Len()
	if err := w.WriteNode(n); err != nil {
		t.Fatal(err)
	}
	return buf.Len() - before
}
