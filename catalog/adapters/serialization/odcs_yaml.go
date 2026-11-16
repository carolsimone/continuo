// Package serialization renders catalog's documents into the byte forms that
// leave the process for other systems to read.
package serialization

import (
	"bytes"
	"fmt"
	"slices"
	"unicode/utf8"

	"gopkg.in/yaml.v3"

	"github.com/carolsimone/continuo/catalog/service/ports"
)

// maxFlowLine is the widest line, in characters and with its indentation, on
// which a mapping is written in flow style.
const maxFlowLine = 160

// yamlIndent is the number of spaces each nesting level indents.
const yamlIndent = 2

// rootKeyOrder is the order of a contract's top-level keys, ODCS's own
// field order; any other top-level key follows, sorted.
var rootKeyOrder = []string{
	"apiVersion", "kind", "id", "name", "version", "status", "domain", "tenant", "servers",
	"description", "team", "tags", "roles", "support", "price", "slaDefaultElement", "slaProperties",
	"authoritativeDefinitions", "customProperties", "schema",
}

// Fixed key orders of the mappings catalog itself writes into the published
// form; any other key of such a mapping follows, sorted.
var (
	nameFirst            = []string{"name"}
	serverKeyOrder       = []string{"server", "type", "host", "port", "database", "catalog", "schema"}
	customPropertyOrder  = []string{"vendor", "property", "value"}
	provenanceValueOrder = []string{
		"contractHash", "contentHash", "releaseId", "promotionSeq", "promotedAt", "continuoContracts", "sourcePath",
	}
)

// ODCSYAML renders a published contract as YAML. Key order is fixed: the root
// in ODCS's field order, a schema object and a property with name first, a
// servers entry and a custom property in the order catalog writes them, the
// provenance value in the order of its facts, and every other mapping sorted;
// keys a fixed order does not name follow it, sorted. A mapping that holds no
// list is written in flow style when it fits on one line of maxFlowLine
// characters, and in block style otherwise; lists are block sequences. A
// string is double-quoted whenever YAML would not read it back, plain, as the
// same string in a flow collection. Lines are never wrapped. So the same
// document always renders the same bytes.
type ODCSYAML struct{}

var _ ports.PublishedRenderer = ODCSYAML{}

// keyOrder names the fixed key order of the mapping being built.
type keyOrder int

const (
	orderSorted keyOrder = iota
	orderRoot
	orderNameFirst
	orderServer
	orderCustomProperty
	orderProvenance
)

// Render returns doc as YAML. A value that is not a string, a bool, an
// integer, a float64, nil, a map[string]any or a []any is an error.
func (ODCSYAML) Render(doc map[string]any) ([]byte, error) {
	root, err := buildMapping(doc, orderRoot, 0)
	if err != nil {
		return nil, err
	}
	return encode(root)
}

// encode writes node as one YAML document, with no line wrapping.
func encode(node *yaml.Node) ([]byte, error) {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(yamlIndent)
	if err := enc.Encode(node); err != nil {
		return nil, fmt.Errorf("encode the published contract: %w", err)
	}
	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("encode the published contract: %w", err)
	}
	return buf.Bytes(), nil
}

// buildMapping builds m with its keys in order. column is the column its keys
// start at when it is written as a block.
func buildMapping(m map[string]any, order keyOrder, column int) (*yaml.Node, error) {
	node := &yaml.Node{Kind: yaml.MappingNode}
	for _, key := range orderedKeys(m, order) {
		value, err := buildValue(m[key], childOrder(m, order, key), column+yamlIndent)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		if value.Kind == yaml.MappingNode {
			chooseFlow(value, column+utf8.RuneCountInString(key)+len(": "))
		}
		node.Content = append(node.Content, stringNode(key), value)
	}
	return node, nil
}

// buildSequence builds s as a block sequence whose items' content starts at
// column + yamlIndent.
func buildSequence(s []any, itemOrder keyOrder, column int) (*yaml.Node, error) {
	node := &yaml.Node{Kind: yaml.SequenceNode}
	itemColumn := column + yamlIndent
	for i, item := range s {
		value, err := buildValue(item, itemOrder, itemColumn)
		if err != nil {
			return nil, fmt.Errorf("[%d]: %w", i, err)
		}
		if value.Kind == yaml.MappingNode {
			chooseFlow(value, itemColumn)
		}
		node.Content = append(node.Content, value)
	}
	return node, nil
}

// buildValue builds one value. column is where a mapping's keys start, and
// where a sequence's dashes start less one indent, when written as a block.
func buildValue(v any, order keyOrder, column int) (*yaml.Node, error) {
	switch t := v.(type) {
	case map[string]any:
		return buildMapping(t, order, column)
	case []any:
		return buildSequence(t, order, column-yamlIndent)
	case string:
		return stringNode(t), nil
	case nil, bool, int, int32, int64, float64:
		node := &yaml.Node{}
		if err := node.Encode(t); err != nil {
			return nil, err
		}
		return node, nil
	default:
		return nil, fmt.Errorf("a value of type %T has no YAML form", v)
	}
}

// orderedKeys lists m's keys: those order fixes, in its order, then the rest
// sorted.
func orderedKeys(m map[string]any, order keyOrder) []string {
	var fixed []string
	switch order {
	case orderRoot:
		fixed = rootKeyOrder
	case orderNameFirst:
		fixed = nameFirst
	case orderServer:
		fixed = serverKeyOrder
	case orderCustomProperty:
		fixed = customPropertyOrder
	case orderProvenance:
		fixed = provenanceValueOrder
	case orderSorted:
	}
	keys := make([]string, 0, len(m))
	for _, k := range fixed {
		if _, ok := m[k]; ok {
			keys = append(keys, k)
		}
	}
	rest := make([]string, 0, len(m)-len(keys))
	for k := range m {
		if !slices.Contains(fixed, k) {
			rest = append(rest, k)
		}
	}
	slices.Sort(rest)
	return append(keys, rest...)
}

// childOrder is the key order of the value under key in parent, a mapping
// built in order: a root servers entry, a schema object, a property (any item
// of a properties list, and an array property's items), a custom property, and
// the value of continuo's provenance property each have their own.
func childOrder(parent map[string]any, order keyOrder, key string) keyOrder {
	switch {
	case key == "customProperties":
		return orderCustomProperty
	case order == orderRoot && key == "servers":
		return orderServer
	case order == orderRoot && key == "schema":
		return orderNameFirst
	case order == orderNameFirst && (key == "properties" || key == "items"):
		return orderNameFirst
	case order == orderCustomProperty && key == "value" &&
		parent["vendor"] == "continuo" && parent["property"] == "provenance":
		return orderProvenance
	default:
		return orderSorted
	}
}

// chooseFlow marks node, a mapping whose first line starts at column, as flow
// style when it holds no list and its flow form fits within maxFlowLine.
// Inside a flow mapping every nested mapping is written in flow too.
func chooseFlow(node *yaml.Node, column int) {
	if holdsSequence(node) {
		return
	}
	node.Style = yaml.FlowStyle
	text, err := encode(node)
	if err != nil || column+utf8.RuneCount(bytes.TrimSuffix(text, []byte("\n"))) > maxFlowLine {
		node.Style = 0
	}
}

// holdsSequence reports whether node or anything below it is a sequence.
func holdsSequence(node *yaml.Node) bool {
	if node.Kind == yaml.SequenceNode {
		return true
	}
	return slices.ContainsFunc(node.Content, holdsSequence)
}

// stringNode is s as a string scalar, double-quoted when YAML would not read
// it back plain as the same string inside a flow collection: a string that
// reads as another type, holds an indicator, a line break or edge spaces.
func stringNode(s string) *yaml.Node {
	node := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: s}
	if !plainInFlow(s) {
		node.Style = yaml.DoubleQuotedStyle
	}
	return node
}

// plainInFlow reports whether the YAML encoder writes s unquoted as the item
// of a flow sequence, which it does only when the plain text reads back as
// the string s.
func plainInFlow(s string) bool {
	probe := &yaml.Node{Kind: yaml.SequenceNode, Style: yaml.FlowStyle, Content: []*yaml.Node{
		{Kind: yaml.ScalarNode, Tag: "!!str", Value: s},
	}}
	out, err := yaml.Marshal(probe)
	return err == nil && string(out) == "["+s+"]\n"
}
