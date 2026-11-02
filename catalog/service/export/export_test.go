package export_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/service/export"
	"github.com/carolsimone/continuo/catalog/service/export/graphschema"
)

const tenant = "t1"

func node(labels []string, props map[string]any) export.NodeRecord {
	all := map[string]any{"tenant_id": tenant}
	for k, v := range props {
		all[k] = v
	}
	return export.NodeRecord{Labels: labels, Props: all}
}

func TestNodeRecord_CheckAcceptsEveryValueKind(t *testing.T) {
	n := node([]string{"Node", "Meta"}, map[string]any{
		"s":        "héllo\nworld",
		"b":        false,
		"i":        int64(math.MinInt64),
		"f":        math.NaN(),
		"negzero":  math.Copysign(0, -1),
		"dt":       time.Date(2026, 10, 30, 2, 0, 0, 1, time.UTC),
		"strings":  []any{"a", "b"},
		"ints":     []any{int64(1), int64(2)},
		"floats":   []any{0.5, math.Inf(1)},
		"bools":    []any{true},
		"datetime": []any{time.Unix(0, 0).UTC(), time.Unix(1, 0).In(time.FixedZone("Offset", 0))},
		"empty":    []any{},
		"$int":     "a property named like a typed value",
	})
	if err := n.Check(tenant); err != nil {
		t.Fatalf("Check: %v", err)
	}
}

func TestNodeRecord_CheckRefusesALabelOutsideTheSchema(t *testing.T) {
	for _, label := range []string{"Table", "Run", "node", "", "Node {tenant_id: 'x'}) DETACH DELETE (n", "Node:Meta"} {
		err := node([]string{"Node", label}, nil).Check(tenant)
		if !errors.Is(err, export.ErrOutsideSchema) {
			t.Errorf("label %q: Check = %v, want ErrOutsideSchema", label, err)
		}
	}
}

func TestNodeRecord_CheckRefusesAnotherTenant(t *testing.T) {
	cases := map[string]export.NodeRecord{
		"another tenant": {Labels: []string{"Node"}, Props: map[string]any{"tenant_id": "t2"}},
		"no tenant":      {Labels: []string{"Node"}, Props: map[string]any{"unique_id": "a"}},
		"not a string":   {Labels: []string{"Node"}, Props: map[string]any{"tenant_id": int64(1)}},
		"no properties":  {Labels: []string{"Node"}},
	}
	for name, n := range cases {
		if err := n.Check(tenant); !errors.Is(err, export.ErrTenantMismatch) {
			t.Errorf("%s: Check = %v, want ErrTenantMismatch", name, err)
		}
	}
}

func TestNodeRecord_CheckRefusesAValueTheGraphCannotHold(t *testing.T) {
	cases := map[string]any{
		"null":                    nil,
		"int":                     42,
		"int32":                   int32(42),
		"uint64":                  uint64(42),
		"float32":                 float32(1.5),
		"typed list":              []string{"a"},
		"list holding a list":     []any{[]any{"a"}},
		"list holding null":       []any{"a", nil},
		"list of int and float":   []any{int64(1), 2.0},
		"list of string and bool": []any{"a", true},
		"list holding an int":     []any{1},
		"map":                     map[string]any{"a": "b"},
		"bytes":                   []byte("a"),
		"invalid UTF-8":           "\xff",
		"pointer":                 new(int64),
	}
	for name, v := range cases {
		err := node([]string{"Node"}, map[string]any{"p": v}).Check(tenant)
		if !errors.Is(err, export.ErrUnsupportedValue) {
			t.Errorf("%s: Check = %v, want ErrUnsupportedValue", name, err)
		}
	}
}

func TestNodeRecord_CheckRefusesABadShape(t *testing.T) {
	cases := map[string]export.NodeRecord{
		"no label":             {Props: map[string]any{"tenant_id": tenant}},
		"a label twice":        {Labels: []string{"Node", "Node"}, Props: map[string]any{"tenant_id": tenant}},
		"empty property name":  node([]string{"Node"}, map[string]any{"": "x"}),
		"NUL in property name": node([]string{"Node"}, map[string]any{"a\x00b": "x"}),
		"invalid UTF-8 name":   node([]string{"Node"}, map[string]any{"\xff": "x"}),
	}
	for name, n := range cases {
		if err := n.Check(tenant); !errors.Is(err, export.ErrInvalidRecord) {
			t.Errorf("%s: Check = %v, want ErrInvalidRecord", name, err)
		}
	}
}

func TestNodeRecord_SchemaLabelsAreTheSchemasConstantsInSchemaOrder(t *testing.T) {
	got, err := node([]string{"ProcessedMessage", "Node"}, nil).SchemaLabels()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != graphschema.Node || got[1] != graphschema.ProcessedMessage {
		t.Errorf("SchemaLabels = %v, want [Node ProcessedMessage]", got)
	}
	if _, err := node([]string{"Evil"}, nil).SchemaLabels(); !errors.Is(err, export.ErrOutsideSchema) {
		t.Errorf("SchemaLabels of a label outside the schema = %v, want ErrOutsideSchema", err)
	}
}

func TestNodeRecord_IsLease(t *testing.T) {
	cases := []struct {
		name string
		n    export.NodeRecord
		want bool
	}{
		{"the lease node", node([]string{"Meta"}, map[string]any{"key": "export", "holder": "h"}), true},
		{"the lease node with another label", node([]string{"Node", "Meta"}, map[string]any{"key": "export"}), true},
		{"another meta node", node([]string{"Meta"}, map[string]any{"key": "current_release"}), false},
		{"key export on another label", node([]string{"Node"}, map[string]any{"key": "export"}), false},
		{"a meta node without a key", node([]string{"Meta"}, nil), false},
	}
	for _, c := range cases {
		if got := c.n.IsLease(); got != c.want {
			t.Errorf("%s: IsLease = %v, want %v", c.name, got, c.want)
		}
	}
}

// Every identity property of the schema is text. A node whose identity
// property holds another value is refused, and a reference by one: the graph
// compares numbers by value (1 equals 1.0) and NaN equals nothing, so a file
// such a value passed the check of would fail to load after the wipe.
func TestNodeRecord_CheckRefusesAnIdentityThatIsNotText(t *testing.T) {
	cases := map[string]export.NodeRecord{
		"an integer unique_id":  node([]string{"Node"}, map[string]any{"unique_id": int64(1)}),
		"a float unique_id":     node([]string{"Node"}, map[string]any{"unique_id": 1.0}),
		"a NaN content_hash":    node([]string{"NodeVersion"}, map[string]any{"unique_id": "a", "content_hash": math.NaN()}),
		"a boolean message_id":  node([]string{"ProcessedMessage"}, map[string]any{"group": "g", "message_id": true}),
		"a datetime key":        node([]string{"Meta"}, map[string]any{"key": time.Unix(0, 0).UTC()}),
		"a list as unique_id":   node([]string{"Node"}, map[string]any{"unique_id": []any{"a"}}),
		"a second label's role": node([]string{"Node", "Meta"}, map[string]any{"unique_id": "a", "key": int64(2)}),
	}
	for name, n := range cases {
		if err := n.Check(tenant); !errors.Is(err, export.ErrInvalidRecord) {
			t.Errorf("%s: Check = %v, want ErrInvalidRecord", name, err)
		}
		labels, _ := n.SchemaLabels()
		for _, l := range labels {
			if key, ok := n.Identity(l); ok {
				for _, v := range key {
					if _, isText := v.(string); !isText {
						t.Errorf("%s: Identity(%s) = %v, a value that is not text", name, l, key)
					}
				}
			}
		}
	}
	if err := node([]string{"Node"}, map[string]any{"unique_id": "a", "seq": int64(1)}).Check(tenant); err != nil {
		t.Errorf("a node whose identity is text: Check = %v", err)
	}
	for name, r := range map[string]export.NodeRef{
		"an integer key": ref("Node", int64(1)),
		"a float key":    ref("Node", 1.0),
		"a NaN key":      ref("NodeVersion", "a", math.NaN()),
		"a boolean key":  ref("ProcessedMessage", "g", false),
	} {
		if err := r.Check(); !errors.Is(err, export.ErrInvalidRecord) {
			t.Errorf("%s: Check = %v, want ErrInvalidRecord", name, err)
		}
	}
}

func TestNodeRecord_IdentityFollowsTheSchema(t *testing.T) {
	n := node([]string{"NodeVersion"}, map[string]any{"content_hash": "h", "unique_id": "a.b", "version_seq": int64(3)})
	key, ok := n.Identity(graphschema.NodeVersion)
	if !ok || len(key) != 2 || key[0] != "a.b" || key[1] != "h" {
		t.Errorf("Identity(NodeVersion) = %v, %v, want [a.b h], true", key, ok)
	}
	if _, ok := node([]string{"NodeVersion"}, map[string]any{"unique_id": "a.b"}).Identity(graphschema.NodeVersion); ok {
		t.Error("Identity of a node that lacks content_hash is ok, want not ok")
	}
}

func ref(label string, key ...any) export.NodeRef { return export.NodeRef{Label: label, Key: key} }

func TestRelRecord_Check(t *testing.T) {
	good := export.RelRecord{
		Type: "CURRENT", From: ref("Node", "a"), To: ref("NodeVersion", "a", "h"),
		Props: map[string]any{"promoted_at": time.Unix(0, 0).UTC(), "release_id": "r1"},
	}
	if err := good.Check(); err != nil {
		t.Fatalf("Check of a well-formed relationship: %v", err)
	}
	if typ, err := good.SchemaType(); err != nil || typ != graphschema.RelCurrent {
		t.Errorf("SchemaType = %v, %v, want CURRENT", typ, err)
	}

	cases := []struct {
		name string
		edit func(r *export.RelRecord)
		want error
	}{
		{"type outside the schema", func(r *export.RelRecord) { r.Type = "PREVIOUS" }, export.ErrOutsideSchema},
		{"type that injects Cypher", func(r *export.RelRecord) { r.Type = "CURRENT]->(x) DETACH DELETE x //" }, export.ErrOutsideSchema},
		{"end label outside the schema", func(r *export.RelRecord) { r.To = ref("Table", "a") }, export.ErrOutsideSchema},
		{"key shorter than the identity", func(r *export.RelRecord) { r.To = ref("NodeVersion", "a") }, export.ErrInvalidRecord},
		{"key longer than the identity", func(r *export.RelRecord) { r.From = ref("Node", "a", "b") }, export.ErrInvalidRecord},
		{"null key value", func(r *export.RelRecord) { r.From = ref("Node", nil) }, export.ErrUnsupportedValue},
		{"list key value", func(r *export.RelRecord) { r.From = ref("Node", []any{"a"}) }, export.ErrUnsupportedValue},
		{"end is the lease node", func(r *export.RelRecord) { r.From = ref("Meta", "export") }, export.ErrInvalidRecord},
		{"property the graph cannot hold", func(r *export.RelRecord) { r.Props = map[string]any{"p": 1} }, export.ErrUnsupportedValue},
		{"empty property name", func(r *export.RelRecord) { r.Props = map[string]any{"": "x"} }, export.ErrInvalidRecord},
	}
	for _, c := range cases {
		r := good
		c.edit(&r)
		if err := r.Check(); !errors.Is(err, c.want) {
			t.Errorf("%s: Check = %v, want %v", c.name, err, c.want)
		}
	}
}

func TestHeader_Check(t *testing.T) {
	good := export.Header{
		FormatVersion: export.FormatVersion, TenantID: tenant, ExportedAt: time.Unix(1, 0).UTC(),
		Nodes: map[string]int64{"Node": 2, "Meta": 0}, Relationships: map[string]int64{"DEPENDS_ON": 1},
	}
	if err := good.Check(); err != nil {
		t.Fatalf("Check of a well-formed header: %v", err)
	}
	cases := []struct {
		name string
		edit func(h *export.Header)
		want error
	}{
		{"another format version", func(h *export.Header) { h.FormatVersion = 2 }, export.ErrInvalidRecord},
		{"no tenant", func(h *export.Header) { h.TenantID = "" }, export.ErrInvalidRecord},
		{"no export time", func(h *export.Header) { h.ExportedAt = time.Time{} }, export.ErrInvalidRecord},
		{"label outside the schema", func(h *export.Header) { h.Nodes = map[string]int64{"Table": 1} }, export.ErrOutsideSchema},
		{"type outside the schema", func(h *export.Header) { h.Relationships = map[string]int64{"EXECUTES": 1} }, export.ErrOutsideSchema},
		{"negative count", func(h *export.Header) { h.Nodes = map[string]int64{"Node": -1} }, export.ErrInvalidRecord},
	}
	for _, c := range cases {
		h := good
		c.edit(&h)
		if err := h.Check(); !errors.Is(err, c.want) {
			t.Errorf("%s: Check = %v, want %v", c.name, err, c.want)
		}
	}
}
