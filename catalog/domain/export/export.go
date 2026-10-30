// Package export is the record model of a tenant's graph export: a header,
// one record per node, then one record per relationship, whose ends are named
// by the identities of graphschema. It also holds the checks a record passes
// before it is written to an export file and before it is loaded back into
// the graph, so neither side takes a label, a type, a tenant or a value the
// catalog graph cannot hold.
package export

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/carolsimone/continuo/catalog/domain/graphschema"
)

// FormatVersion is the version of the export layout this code writes and
// reads.
const FormatVersion = 1

// TenantProperty is the property that names a node's tenant.
const TenantProperty = "tenant_id"

// LeaseNodeKey is the key of the (:Meta) node that holds a tenant's export
// lease. That node is neither exported nor restored: a restored lease would
// pause the tenant's writes.
const LeaseNodeKey = "export"

var (
	// ErrOutsideSchema means a label or a relationship type is not one of the
	// catalog graph's.
	ErrOutsideSchema = errors.New("outside the catalog graph schema")
	// ErrTenantMismatch means a header or a node names another tenant than
	// the export's, or none.
	ErrTenantMismatch = errors.New("another tenant than the export's")
	// ErrUnsupportedValue means a property or key value is not one the
	// catalog graph holds: a string, a bool, an int64, a float64, a datetime,
	// or a list of values of one of those kinds.
	ErrUnsupportedValue = errors.New("a value the catalog graph cannot hold")
	// ErrInvalidRecord means a header or a record is shaped otherwise than an
	// export of the catalog graph.
	ErrInvalidRecord = errors.New("an invalid export record")
)

// Header opens an export: who and what it holds. Nodes counts the node
// records per label (a node with two labels counts under each), and
// Relationships the relationship records per type.
type Header struct {
	FormatVersion  int
	TenantID       string
	ExportedAt     time.Time
	CatalogVersion string
	Nodes          map[string]int64
	Relationships  map[string]int64
}

// NodeRecord is one node: its labels and every property, tenant_id included.
type NodeRecord struct {
	Labels []string
	Props  map[string]any
}

// RelRecord is one relationship: its type, its two ends and its properties.
type RelRecord struct {
	Type     string
	From, To NodeRef
	Props    map[string]any
}

// NodeRef names a node of the export's tenant by one of its labels and the
// values of that label's identity properties (graphschema.Identity), in
// order.
type NodeRef struct {
	Label string
	Key   []any
}

// Check refuses a header of another format version, without a tenant or an
// export time, or whose counts name a label or type outside the schema or
// are negative.
func (h Header) Check() error {
	if h.FormatVersion != FormatVersion {
		return fmt.Errorf("%w: format version %d, want %d", ErrInvalidRecord, h.FormatVersion, FormatVersion)
	}
	if h.TenantID == "" {
		return fmt.Errorf("%w: the header names no tenant", ErrInvalidRecord)
	}
	if h.ExportedAt.IsZero() {
		return fmt.Errorf("%w: the header has no export time", ErrInvalidRecord)
	}
	for label, n := range h.Nodes {
		if _, ok := schemaLabel(label); !ok {
			return fmt.Errorf("header counts label %q: %w", label, ErrOutsideSchema)
		}
		if n < 0 {
			return fmt.Errorf("%w: header counts %d :%s nodes", ErrInvalidRecord, n, label)
		}
	}
	for typ, n := range h.Relationships {
		if _, ok := schemaRelType(typ); !ok {
			return fmt.Errorf("header counts relationship type %q: %w", typ, ErrOutsideSchema)
		}
		if n < 0 {
			return fmt.Errorf("%w: header counts %d :%s relationships", ErrInvalidRecord, n, typ)
		}
	}
	return nil
}

// Check refuses a node of a label outside the schema, with no label or a
// label twice, whose tenant_id is not tenantID, or with a property the graph
// cannot hold.
func (n NodeRecord) Check(tenantID string) error {
	if _, err := n.SchemaLabels(); err != nil {
		return err
	}
	if t, ok := n.Props[TenantProperty].(string); !ok || t != tenantID {
		return fmt.Errorf("node %v has tenant_id %#v, want %q: %w", n.Labels, n.Props[TenantProperty], tenantID, ErrTenantMismatch)
	}
	if err := checkProps(n.Props); err != nil {
		return fmt.Errorf("node %v: %w", n.Labels, err)
	}
	return nil
}

// SchemaLabels returns the node's labels as the schema's own constants, in
// the schema's order, so a statement built from them never carries text from
// the record. It refuses a node with no label, a label twice or a label
// outside the schema.
func (n NodeRecord) SchemaLabels() ([]graphschema.Label, error) {
	if len(n.Labels) == 0 {
		return nil, fmt.Errorf("%w: a node with no label", ErrInvalidRecord)
	}
	has := make(map[graphschema.Label]bool, len(n.Labels))
	for _, name := range n.Labels {
		l, ok := schemaLabel(name)
		if !ok {
			return nil, fmt.Errorf("node label %q: %w", name, ErrOutsideSchema)
		}
		if has[l] {
			return nil, fmt.Errorf("%w: node label %q twice", ErrInvalidRecord, name)
		}
		has[l] = true
	}
	out := make([]graphschema.Label, 0, len(has))
	for _, l := range graphschema.Labels() {
		if has[l] {
			out = append(out, l)
		}
	}
	return out, nil
}

// IsLease reports whether n is a tenant's export lease node.
func (n NodeRecord) IsLease() bool {
	for _, l := range n.Labels {
		if l == string(graphschema.Meta) {
			key, _ := n.Props["key"].(string)
			return key == LeaseNodeKey
		}
	}
	return false
}

// Identity returns the values of the properties that identify n under label
// l, in graphschema.Identity order; ok is false when n lacks one of them.
func (n NodeRecord) Identity(l graphschema.Label) (key []any, ok bool) {
	props := graphschema.Identity(l)
	if props == nil {
		return nil, false
	}
	key = make([]any, 0, len(props))
	for _, p := range props {
		v, found := n.Props[p]
		if !found || v == nil {
			return nil, false
		}
		key = append(key, v)
	}
	return key, true
}

// Check refuses a relationship of a type outside the schema, an end that is
// not a node of the schema named by its label's identity, an end that is the
// export lease node, or a property the graph cannot hold.
func (r RelRecord) Check() error {
	if _, err := r.SchemaType(); err != nil {
		return err
	}
	for _, end := range []NodeRef{r.From, r.To} {
		if err := end.Check(); err != nil {
			return fmt.Errorf("relationship :%s: %w", r.Type, err)
		}
	}
	if err := checkProps(r.Props); err != nil {
		return fmt.Errorf("relationship :%s: %w", r.Type, err)
	}
	return nil
}

// SchemaType returns the relationship's type as the schema's own constant.
func (r RelRecord) SchemaType() (graphschema.RelType, error) {
	t, ok := schemaRelType(r.Type)
	if !ok {
		return "", fmt.Errorf("relationship type %q: %w", r.Type, ErrOutsideSchema)
	}
	return t, nil
}

// Check refuses a reference by a label outside the schema, a key that does
// not match the label's identity, or the export lease node.
func (ref NodeRef) Check() error {
	l, err := ref.SchemaLabel()
	if err != nil {
		return err
	}
	if want := len(graphschema.Identity(l)); len(ref.Key) != want {
		return fmt.Errorf("%w: a :%s is named by %d values, want %d", ErrInvalidRecord, l, len(ref.Key), want)
	}
	for _, v := range ref.Key {
		if err := checkScalar(v); err != nil {
			return fmt.Errorf("a :%s's key: %w", l, err)
		}
	}
	if l == graphschema.Meta && ref.Key[0] == LeaseNodeKey {
		return fmt.Errorf("%w: an end is the export lease node", ErrInvalidRecord)
	}
	return nil
}

// SchemaLabel returns the reference's label as the schema's own constant.
func (ref NodeRef) SchemaLabel() (graphschema.Label, error) {
	l, ok := schemaLabel(ref.Label)
	if !ok {
		return "", fmt.Errorf("relationship end label %q: %w", ref.Label, ErrOutsideSchema)
	}
	return l, nil
}

func schemaLabel(name string) (graphschema.Label, bool) {
	for _, l := range graphschema.Labels() {
		if string(l) == name {
			return l, true
		}
	}
	return "", false
}

func schemaRelType(name string) (graphschema.RelType, bool) {
	for _, t := range graphschema.RelTypes() {
		if string(t) == name {
			return t, true
		}
	}
	return "", false
}

// checkProps refuses a property name the graph refuses (empty, holding NUL,
// or not UTF-8) and a value it cannot hold.
func checkProps(props map[string]any) error {
	for name, v := range props {
		if name == "" || strings.ContainsRune(name, 0) || !utf8.ValidString(name) {
			return fmt.Errorf("%w: property name %q", ErrInvalidRecord, name)
		}
		if err := checkValue(v); err != nil {
			return fmt.Errorf("property %q: %w", name, err)
		}
	}
	return nil
}

// checkValue accepts a scalar or a list of scalars of one kind: Neo4j stores
// no null, no list in a list and no list of mixed kinds (it would turn a
// list of integers and floats into floats).
func checkValue(v any) error {
	list, ok := v.([]any)
	if !ok {
		return checkScalar(v)
	}
	var first string
	for i, e := range list {
		if err := checkScalar(e); err != nil {
			return fmt.Errorf("list element %d: %w", i, err)
		}
		k := kind(e)
		if i == 0 {
			first = k
		} else if k != first {
			return fmt.Errorf("%w: a list of %s and %s", ErrUnsupportedValue, first, k)
		}
	}
	return nil
}

func checkScalar(v any) error {
	switch x := v.(type) {
	case string:
		if !utf8.ValidString(x) {
			return fmt.Errorf("%w: a string that is not UTF-8", ErrUnsupportedValue)
		}
		return nil
	case bool, int64, float64, time.Time:
		return nil
	case nil:
		return fmt.Errorf("%w: null", ErrUnsupportedValue)
	default:
		return fmt.Errorf("%w: a value of type %T", ErrUnsupportedValue, v)
	}
}

func kind(v any) string {
	return fmt.Sprintf("%T", v)
}
