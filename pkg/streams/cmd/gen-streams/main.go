// Command gen-streams reads pkg/streams/contract.yaml and writes one file per
// generated surface:
//
//   - pkg/streams/streams.gen.go — Go stream and consumer-group constants.
//   - pkg/streams/streams_test_access.gen.go — test-only accessor for those
//     constants, read by the contract parity test.
//   - pkg/domain/model/vocabulary.gen.go — Go types for the contract
//     vocabularies. They live in the shared domain package, not in the
//     transport package, so domain code names a vocabulary value without
//     importing the Redis stream contract.
//   - pkg/domain/model/vocabulary_test_access.gen.go — test-only accessor for
//     the vocabulary values, read by the vocabulary parity test.
//   - topology-controller/streams_contract.py — Python stream and group
//     constants for topology-controller.
//   - topology-controller/domain/contract_vocabulary.py — Python vocabulary
//     enums, the domain-layer counterpart of vocabulary.gen.go.
//   - ui/src/server/generated/vocabulary.gen.ts — TypeScript unions and value
//     lists for the ui's server and client, the counterpart of the Go and
//     Python vocabulary files.
package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"

	"gopkg.in/yaml.v3"
)

type Contract struct {
	Streams      []Stream     `yaml:"streams"`
	Vocabularies []Vocabulary `yaml:"vocabularies"`
}

type Stream struct {
	Name        string     `yaml:"name"`
	Const       string     `yaml:"const"`
	Description string     `yaml:"description"`
	Producers   []string   `yaml:"producers"`
	Consumers   []Consumer `yaml:"consumers"`
}

type Consumer struct {
	Service string `yaml:"service"`
	Group   string `yaml:"group"`
	Const   string `yaml:"const"`
}

// Vocabulary is a closed set of string values two or more services agree on,
// emitted as a typed string in Go and a StrEnum in Python. Declaration order
// is significant: a consumer that must pick one value from several uses it as
// precedence.
type Vocabulary struct {
	Name        string            `yaml:"name"`
	Const       string            `yaml:"const"`
	Description string            `yaml:"description"`
	Values      []VocabularyValue `yaml:"values"`
}

type VocabularyValue struct {
	Value       string `yaml:"value"`
	Const       string `yaml:"const"`
	Healable    bool   `yaml:"healable"`
	FullRefresh bool   `yaml:"full_refresh"`
	SecretRef   bool   `yaml:"secret_ref"`
	Runtime     string `yaml:"runtime"`
	Description string `yaml:"description"`
}

// nodeRuntimeVocabulary names the vocabulary whose values a "runtime:" attribute may take.
const nodeRuntimeVocabulary = "node_runtime"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "gen-streams:", err)
		os.Exit(1)
	}
}

func run() error {
	root, err := findRepoRoot()
	if err != nil {
		return err
	}
	yamlPath := filepath.Join(root, "pkg", "streams", "contract.yaml")
	f, err := os.Open(yamlPath) //nolint:gosec // G304: yamlPath is built from a fixed relative path under a repo root discovered by walking up for go.work, not from external input
	if err != nil {
		return fmt.Errorf("open contract: %w", err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			fmt.Fprintln(os.Stderr, "gen-streams: close contract:", closeErr)
		}
	}()

	c, err := loadAndValidate(f)
	if err != nil {
		return fmt.Errorf("load contract: %w", err)
	}

	goSrc, err := emitGo(c)
	if err != nil {
		return fmt.Errorf("emit go: %w", err)
	}
	goOut := filepath.Join(root, "pkg", "streams", "streams.gen.go")
	if err := os.WriteFile(goOut, []byte(goSrc), 0o600); err != nil {
		return fmt.Errorf("write go: %w", err)
	}

	accessSrc, err := emitGoTestAccess(c)
	if err != nil {
		return fmt.Errorf("emit go test access: %w", err)
	}
	accessOut := filepath.Join(root, "pkg", "streams", "streams_test_access.gen.go")
	if err := os.WriteFile(accessOut, []byte(accessSrc), 0o600); err != nil {
		return fmt.Errorf("write go test access: %w", err)
	}

	vocabSrc, err := emitGoVocabulary(c)
	if err != nil {
		return fmt.Errorf("emit go vocabulary: %w", err)
	}
	vocabOut := filepath.Join(root, "pkg", "domain", "model", "vocabulary.gen.go")
	if err := os.WriteFile(vocabOut, []byte(vocabSrc), 0o600); err != nil {
		return fmt.Errorf("write go vocabulary: %w", err)
	}

	vocabAccessSrc, err := emitGoVocabularyTestAccess(c)
	if err != nil {
		return fmt.Errorf("emit go vocabulary test access: %w", err)
	}
	vocabAccessOut := filepath.Join(root, "pkg", "domain", "model", "vocabulary_test_access.gen.go")
	if err := os.WriteFile(vocabAccessOut, []byte(vocabAccessSrc), 0o600); err != nil {
		return fmt.Errorf("write go vocabulary test access: %w", err)
	}

	pySrc, err := emitPythonStreams(c, "topology-controller")
	if err != nil {
		return fmt.Errorf("emit python: %w", err)
	}
	pyOut := filepath.Join(root, "topology-controller", "streams_contract.py")
	if err := os.WriteFile(pyOut, []byte(pySrc), 0o600); err != nil {
		return fmt.Errorf("write python: %w", err)
	}

	pyVocabSrc, err := emitPythonVocabulary(c)
	if err != nil {
		return fmt.Errorf("emit python vocabulary: %w", err)
	}
	pyVocabOut := filepath.Join(root, "topology-controller", "domain", "contract_vocabulary.py")
	if err := os.WriteFile(pyVocabOut, []byte(pyVocabSrc), 0o600); err != nil {
		return fmt.Errorf("write python vocabulary: %w", err)
	}

	tsVocabSrc, err := emitTSVocabulary(c)
	if err != nil {
		return fmt.Errorf("emit ts vocabulary: %w", err)
	}
	tsVocabDir := filepath.Join(root, "ui", "src", "server", "generated")
	if err := os.MkdirAll(tsVocabDir, 0o750); err != nil {
		return fmt.Errorf("make ts vocabulary dir: %w", err)
	}
	tsVocabOut := filepath.Join(tsVocabDir, "vocabulary.gen.ts")
	if err := os.WriteFile(tsVocabOut, []byte(tsVocabSrc), 0o600); err != nil {
		return fmt.Errorf("write ts vocabulary: %w", err)
	}

	for _, out := range []string{goOut, accessOut, vocabOut, vocabAccessOut, pyOut, pyVocabOut, tsVocabOut} {
		fmt.Fprintln(os.Stderr, "gen-streams: wrote", out)
	}
	return nil
}

func findRepoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("repo root not found (no go.work above %s)", wd)
		}
		dir = parent
	}
}

func parseContract(r io.Reader) (*Contract, error) {
	var c Contract
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	return &c, nil
}

func loadAndValidate(r io.Reader) (*Contract, error) {
	c, err := parseContract(r)
	if err != nil {
		return nil, err
	}
	if err := validate(c); err != nil {
		return nil, err
	}
	return c, nil
}

func emitGo(c *Contract) (string, error) {
	type groupRow struct {
		Const, Group, Service, Stream string
	}
	var streamRows []struct {
		Const, Name, Description string
	}
	var groupRows []groupRow
	for _, s := range c.Streams {
		streamRows = append(streamRows, struct{ Const, Name, Description string }{s.Const, s.Name, s.Description})
		for _, cons := range s.Consumers {
			groupRows = append(groupRows, groupRow{cons.Const, cons.Group, cons.Service, s.Name})
		}
	}

	tmpl := `// Code generated by gen-streams. DO NOT EDIT.
// Source: pkg/streams/contract.yaml
package streams

// Stream names.
const (
{{- range .Streams }}
	// {{ .Const }} — {{ .Description }}
	{{ .Const }} = "{{ .Name }}"
{{- end }}
)

// Consumer groups.
const (
{{- range .Groups }}
	// {{ .Const }} — {{ .Service }} consumer group on {{ .Stream }}.
	{{ .Const }} = "{{ .Group }}"
{{- end }}
)

// All is every stream name from contract.yaml, in contract order — for callers
// that must operate over all streams (e.g. the stream reaper).
var All = []string{
{{- range .Streams }}
	{{ .Const }},
{{- end }}
}
`
	t, err := template.New("go").Parse(tmpl)
	if err != nil {
		return "", err
	}
	var buf strings.Builder
	err = t.Execute(&buf, struct {
		Streams []struct{ Const, Name, Description string }
		Groups  []groupRow
	}{streamRows, groupRows})
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

// runtimeVocabulary returns the node_runtime vocabulary, or nil when the
// contract declares none.
func runtimeVocabulary(c *Contract) *Vocabulary {
	for i := range c.Vocabularies {
		if c.Vocabularies[i].Name == nodeRuntimeVocabulary {
			return &c.Vocabularies[i]
		}
	}
	return nil
}

func constForValue(v *Vocabulary, value string) string {
	for _, val := range v.Values {
		if val.Value == value {
			return val.Const
		}
	}
	return ""
}

func hasHealable(v Vocabulary) bool {
	for _, val := range v.Values {
		if val.Healable {
			return true
		}
	}
	return false
}

func hasFullRefresh(v Vocabulary) bool {
	for _, val := range v.Values {
		if val.FullRefresh {
			return true
		}
	}
	return false
}

func hasSecretRef(v Vocabulary) bool {
	for _, val := range v.Values {
		if val.SecretRef {
			return true
		}
	}
	return false
}

// fullRefreshListConst names the list of values that support a single-node
// full refresh, e.g. NODE_TYPES_SUPPORTING_FULL_REFRESH. Python and TypeScript
// share the name.
func fullRefreshListConst(v Vocabulary) string {
	return toScreamingSnake(v.Const) + "S_SUPPORTING_FULL_REFRESH"
}

// secretRefListConst names the list of values whose contract may name a
// Secret, e.g. NODE_TYPES_WITH_SECRET_REF. Python and TypeScript share the name.
func secretRefListConst(v Vocabulary) string {
	return toScreamingSnake(v.Const) + "S_WITH_SECRET_REF"
}

func usesRuntime(v Vocabulary) bool { return len(v.Values) > 0 && v.Values[0].Runtime != "" }

// pyMember is the Python enum member name for a vocabulary value.
func pyMember(value string) string { return strings.ToUpper(strings.ReplaceAll(value, "-", "_")) }

// goVocabRow is one contract vocabulary shaped for the Go templates. Type is
// repeated on every value row because a nested {{ range }} cannot reach the
// enclosing vocabulary's fields.
type goVocabRow struct {
	Const, Name, Description string
	HasHealable              bool
	HasFullRefresh           bool
	HasSecretRef             bool
	RuntimeType              string
	Values                   []goVocabValueRow
}

type goVocabValueRow struct {
	Const, Type, Value, Description  string
	Healable, FullRefresh, SecretRef bool
	RuntimeConst                     string
}

func goVocabRows(c *Contract) []goVocabRow {
	rv := runtimeVocabulary(c)
	var rows []goVocabRow
	for _, v := range c.Vocabularies {
		row := goVocabRow{Const: v.Const, Name: v.Name, Description: v.Description, HasHealable: hasHealable(v), HasFullRefresh: hasFullRefresh(v), HasSecretRef: hasSecretRef(v)}
		if usesRuntime(v) && rv != nil {
			row.RuntimeType = rv.Const
		}
		for _, val := range v.Values {
			vr := goVocabValueRow{
				Const:       v.Const + val.Const,
				Type:        v.Const,
				Value:       val.Value,
				Description: val.Description,
				Healable:    val.Healable,
				FullRefresh: val.FullRefresh,
				SecretRef:   val.SecretRef,
			}
			if usesRuntime(v) && rv != nil {
				vr.RuntimeConst = rv.Const + constForValue(rv, val.Runtime)
			}
			row.Values = append(row.Values, vr)
		}
		rows = append(rows, row)
	}
	return rows
}

// emitGoVocabulary renders the contract vocabularies into pkg/domain/model.
// They are shared-domain value types, so they sit in the package domain code
// already depends on rather than in the transport-contract package.
func emitGoVocabulary(c *Contract) (string, error) {
	tmpl := `// Code generated by gen-streams. DO NOT EDIT.
// Source: pkg/streams/contract.yaml
//
// Each type here is a closed value set two or more services agree on. They are
// domain vocabulary, not transport: stream and consumer-group names live in
// pkg/streams, which no domain package imports.
package model
{{- range .Vocabularies }}

// {{ .Const }} — {{ .Description }}
// Values come from the vocabulary "{{ .Name }}" in contract.yaml, in
// declaration order.
type {{ .Const }} string

const (
{{- range .Values }}
	// {{ .Const }} — {{ .Description }}
	{{ .Const }} {{ .Type }} = "{{ .Value }}"
{{- end }}
)

// {{ .Const }}s returns every value in contract.yaml declaration order.
func {{ .Const }}s() []{{ .Const }} {
	return []{{ .Const }}{
{{- range .Values }}
		{{ .Const }},
{{- end }}
	}
}

// IsValid reports whether v is a value declared in contract.yaml.
func (v {{ .Const }}) IsValid() bool {
	switch v {
{{- range .Values }}
	case {{ .Const }}:
		return true
{{- end }}
	}
	return false
}

{{- if .HasHealable }}

// Healable reports whether the contract marks v as fixable by a change to the
// user's source, and therefore worth a remediation attempt.
func (v {{ .Const }}) Healable() bool {
	switch v {
{{- range .Values }}
{{- if .Healable }}
	case {{ .Const }}:
		return true
{{- end }}
{{- end }}
	}
	return false
}
{{- end }}
{{- if .HasFullRefresh }}

// SupportsFullRefresh reports whether a single-node full refresh can rebuild a
// node of this type from scratch.
func (v {{ .Const }}) SupportsFullRefresh() bool {
	switch v {
{{- range .Values }}
{{- if .FullRefresh }}
	case {{ .Const }}:
		return true
{{- end }}
{{- end }}
	}
	return false
}
{{- end }}
{{- if .HasSecretRef }}

// AllowsSecretRef reports whether the contract of a node of this type may name
// one Secret whose keys the node's pod receives as environment variables.
func (v {{ .Const }}) AllowsSecretRef() bool {
	switch v {
{{- range .Values }}
{{- if .SecretRef }}
	case {{ .Const }}:
		return true
{{- end }}
{{- end }}
	}
	return false
}
{{- end }}
{{- if .RuntimeType }}

// Runtime reports which toolchain builds a node of this type.
func (v {{ .Const }}) Runtime() {{ .RuntimeType }} {
	switch v {
{{- range .Values }}
	case {{ .Const }}:
		return {{ .RuntimeConst }}
{{- end }}
	}
	return ""
}
{{- end }}
{{- end }}
`
	t, err := template.New("govocab").Parse(tmpl)
	if err != nil {
		return "", err
	}
	var buf strings.Builder
	if err := t.Execute(&buf, struct{ Vocabularies []goVocabRow }{goVocabRows(c)}); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// emitPythonStreams renders the stream and consumer-group constants the named
// service produces or consumes.
func emitPythonStreams(c *Contract, service string) (string, error) {
	type streamRow struct{ Const, Name, Description string }
	type groupRow struct{ Const, Group, Stream string }

	var streams []streamRow
	var groups []groupRow

	for _, s := range c.Streams {
		involves := false
		for _, p := range s.Producers {
			if p == service {
				involves = true
				break
			}
		}
		if !involves {
			for _, cons := range s.Consumers {
				if cons.Service == service {
					involves = true
					break
				}
			}
		}
		if !involves {
			continue
		}
		streams = append(streams, streamRow{
			Const:       toScreamingSnake(s.Const),
			Name:        s.Name,
			Description: s.Description,
		})
		for _, cons := range s.Consumers {
			if cons.Service != service {
				continue
			}
			groups = append(groups, groupRow{
				Const:  toScreamingSnake(cons.Const),
				Group:  cons.Group,
				Stream: s.Name,
			})
		}
	}

	tmpl := `# Code generated by gen-streams. DO NOT EDIT.
# Source: pkg/streams/contract.yaml

{{ range .Streams }}{{ .Const }} = "{{ .Name }}"
"""{{ .Description }}"""

{{ end }}{{ range .Groups }}{{ .Const }} = "{{ .Group }}"
"""{{ $.Service }} consumer group on {{ .Stream }}."""

{{ end }}`

	t, err := template.New("py").Parse(tmpl)
	if err != nil {
		return "", err
	}
	var buf strings.Builder
	if err := t.Execute(&buf, struct {
		Service string
		Streams []streamRow
		Groups  []groupRow
	}{service, streams, groups}); err != nil {
		return "", err
	}
	out := strings.TrimRight(buf.String(), "\n") + "\n"
	return out, nil
}

// emitPythonVocabulary renders the contract vocabularies as StrEnums for the
// Python domain layer, the counterpart of emitGoVocabulary.
func emitPythonVocabulary(c *Contract) (string, error) {
	type pyValueRow struct{ Const, Value string }
	type pyVocabRow struct {
		Const, Description string
		Values             []pyValueRow
		Healable           []string // "ParseFailureKind.INVALID_SQL", ...
		HealableConst      string   // "PARSE_FAILURE_KIND_HEALABLE"
		HasHealable        bool
		FullRefresh        []string // "NodeType.DBT_MODEL", ...
		FullRefreshConst   string   // "NODE_TYPES_SUPPORTING_FULL_REFRESH"
		SecretRef          []string // "NodeType.PYTHON_API", ...
		SecretRefConst     string   // "NODE_TYPES_WITH_SECRET_REF"
		RuntimeConst       string   // "NODE_TYPE_RUNTIME"
		RuntimeType        string   // "NodeRuntime"
		RuntimeEntries     []string // "NodeType.DBT_MODEL: NodeRuntime.DBT", ...
	}
	rv := runtimeVocabulary(c)
	var vocabs []pyVocabRow
	for _, v := range c.Vocabularies {
		row := pyVocabRow{
			Const:         v.Const,
			Description:   v.Description,
			HealableConst: toScreamingSnake(v.Const) + "_HEALABLE",
			HasHealable:   hasHealable(v),
		}
		if hasFullRefresh(v) {
			row.FullRefreshConst = fullRefreshListConst(v)
		}
		if hasSecretRef(v) {
			row.SecretRefConst = secretRefListConst(v)
		}
		if usesRuntime(v) && rv != nil {
			row.RuntimeConst = toScreamingSnake(v.Const) + "_RUNTIME"
			row.RuntimeType = rv.Const
		}
		for _, val := range v.Values {
			member := pyMember(val.Value)
			row.Values = append(row.Values, pyValueRow{Const: member, Value: val.Value})
			if val.Healable {
				row.Healable = append(row.Healable, v.Const+"."+member)
			}
			if val.FullRefresh {
				row.FullRefresh = append(row.FullRefresh, v.Const+"."+member)
			}
			if val.SecretRef {
				row.SecretRef = append(row.SecretRef, v.Const+"."+member)
			}
			if usesRuntime(v) && rv != nil {
				row.RuntimeEntries = append(row.RuntimeEntries, v.Const+"."+member+": "+rv.Const+"."+pyMember(val.Runtime))
			}
		}
		vocabs = append(vocabs, row)
	}

	tmpl := `# Code generated by gen-streams. DO NOT EDIT.
# Source: pkg/streams/contract.yaml
#
# Each enum here is a closed value set two or more services agree on. They are
# domain vocabulary, not transport: stream and consumer-group names live in
# streams_contract, which no module under domain/ imports.

from enum import StrEnum

{{ range .Vocabularies }}
class {{ .Const }}(StrEnum):
    """{{ .Description }}"""
{{ range .Values }}    {{ .Const }} = "{{ .Value }}"
{{ end }}
{{ if .HasHealable }}
{{ .HealableConst }} = frozenset({ {{- range $i, $h := .Healable }}{{ if $i }}, {{ end }}{{ $h }}{{ end -}} })
"""Values of {{ .Const }} a remediation attempt can fix by changing the user's source."""

{{ end }}{{ if .FullRefreshConst }}
{{ .FullRefreshConst }} = frozenset({ {{- range $i, $f := .FullRefresh }}{{ if $i }}, {{ end }}{{ $f }}{{ end -}} })
"""Values of {{ .Const }} a single-node full refresh can rebuild from scratch."""

{{ end }}{{ if .SecretRefConst }}
{{ .SecretRefConst }} = frozenset({ {{- range $i, $f := .SecretRef }}{{ if $i }}, {{ end }}{{ $f }}{{ end -}} })
"""Values of {{ .Const }} whose contract may name one Secret for the node's pod."""

{{ end }}{{ if .RuntimeType }}
{{ .RuntimeConst }}: dict[{{ .Const }}, {{ .RuntimeType }}] = { {{- range $i, $e := .RuntimeEntries }}{{ if $i }}, {{ end }}{{ $e }}{{ end -}} }
"""Which toolchain builds each {{ .Const }}."""

{{ end }}{{ end }}`

	t, err := template.New("pyvocab").Parse(tmpl)
	if err != nil {
		return "", err
	}
	var buf strings.Builder
	if err := t.Execute(&buf, struct{ Vocabularies []pyVocabRow }{vocabs}); err != nil {
		return "", err
	}
	return strings.TrimRight(buf.String(), "\n") + "\n", nil
}

// emitTSVocabulary renders the contract vocabularies as TypeScript unions and
// value lists for the ui, which imports them from both its server and client.
func emitTSVocabulary(c *Contract) (string, error) {
	type tsRow struct {
		Const, ListConst, Union, List         string
		FullRefreshConst, FullRefreshList     string
		SecretRefConst, SecretRefList         string
		RuntimeConst, RuntimeType, RuntimeMap string
	}
	rv := runtimeVocabulary(c)
	var rows []tsRow
	for _, v := range c.Vocabularies {
		quoted := make([]string, 0, len(v.Values))
		for _, val := range v.Values {
			quoted = append(quoted, "'"+val.Value+"'")
		}
		row := tsRow{
			Const:     v.Const,
			ListConst: toScreamingSnake(v.Const) + "S",
			Union:     strings.Join(quoted, " | "),
			List:      strings.Join(quoted, ", "),
		}
		if hasFullRefresh(v) {
			var fr []string
			for _, val := range v.Values {
				if val.FullRefresh {
					fr = append(fr, "'"+val.Value+"'")
				}
			}
			row.FullRefreshConst = fullRefreshListConst(v)
			row.FullRefreshList = strings.Join(fr, ", ")
		}
		if hasSecretRef(v) {
			var sr []string
			for _, val := range v.Values {
				if val.SecretRef {
					sr = append(sr, "'"+val.Value+"'")
				}
			}
			row.SecretRefConst = secretRefListConst(v)
			row.SecretRefList = strings.Join(sr, ", ")
		}
		if usesRuntime(v) && rv != nil {
			pairs := make([]string, 0, len(v.Values))
			for _, val := range v.Values {
				pairs = append(pairs, "'"+val.Value+"': '"+val.Runtime+"'")
			}
			row.RuntimeConst = toScreamingSnake(v.Const) + "_RUNTIME"
			row.RuntimeType = rv.Const
			row.RuntimeMap = strings.Join(pairs, ", ")
		}
		rows = append(rows, row)
	}
	tmpl := `// Code generated by gen-streams. DO NOT EDIT.
// Source: pkg/streams/contract.yaml
//
// Closed value sets the backend services agree on, for the ui's server and
// client alike.
{{ range . }}
export type {{ .Const }} = {{ .Union }};
export const {{ .ListConst }}: readonly {{ .Const }}[] = [{{ .List }}];
{{- if .FullRefreshConst }}
export const {{ .FullRefreshConst }}: readonly {{ .Const }}[] = [{{ .FullRefreshList }}];
{{- end }}
{{- if .SecretRefConst }}
export const {{ .SecretRefConst }}: readonly {{ .Const }}[] = [{{ .SecretRefList }}];
{{- end }}
{{- if .RuntimeConst }}
export const {{ .RuntimeConst }}: Readonly<Record<{{ .Const }}, {{ .RuntimeType }}>> = { {{ .RuntimeMap }} };
{{- end }}
{{ end }}`
	t, err := template.New("tsvocab").Parse(tmpl)
	if err != nil {
		return "", err
	}
	var buf strings.Builder
	if err := t.Execute(&buf, rows); err != nil {
		return "", err
	}
	return strings.TrimRight(buf.String(), "\n") + "\n", nil
}

func toScreamingSnake(s string) string {
	var b strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			b.WriteByte('_')
		}
		if r >= 'a' && r <= 'z' {
			b.WriteRune(r - 32)
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func emitGoTestAccess(c *Contract) (string, error) {
	type row struct{ Const string }
	var streams []row
	var groups []row
	for _, s := range c.Streams {
		streams = append(streams, row{s.Const})
		for _, cons := range s.Consumers {
			groups = append(groups, row{cons.Const})
		}
	}

	tmpl := `// Code generated by gen-streams. DO NOT EDIT.
// Source: pkg/streams/contract.yaml
package streams

// PkgConstantsForTest returns every constant emitted from contract.yaml,
// keyed by identifier. Test-only accessor used by contract_test.go to verify
// YAML ↔ generated-Go parity.
func PkgConstantsForTest() map[string]string {
	return map[string]string{
{{- range .Streams }}
		"{{ .Const }}": {{ .Const }},
{{- end }}
{{- range .Groups }}
		"{{ .Const }}": {{ .Const }},
{{- end }}
	}
}
`
	t, err := template.New("goaccess").Parse(tmpl)
	if err != nil {
		return "", err
	}
	var buf strings.Builder
	if err := t.Execute(&buf, struct {
		Streams []row
		Groups  []row
	}{streams, groups}); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// emitGoVocabularyTestAccess renders the accessor the vocabulary parity test in
// pkg/domain/model reads.
func emitGoVocabularyTestAccess(c *Contract) (string, error) {
	type vocabAccessRow struct {
		Const  string
		Values []struct{ Value string }
	}
	var vocabs []vocabAccessRow
	for _, v := range c.Vocabularies {
		row := vocabAccessRow{Const: v.Const}
		for _, val := range v.Values {
			row.Values = append(row.Values, struct{ Value string }{val.Value})
		}
		vocabs = append(vocabs, row)
	}

	tmpl := `// Code generated by gen-streams. DO NOT EDIT.
// Source: pkg/streams/contract.yaml
package model

// VocabularyValuesForTest returns every vocabulary's values from contract.yaml
// in declaration order, keyed by the vocabulary's Go type name. Test-only
// accessor used by vocabulary_contract_test.go to verify YAML ↔ generated-Go
// parity.
func VocabularyValuesForTest() map[string][]string {
	return map[string][]string{
{{- range .Vocabularies }}
		"{{ .Const }}": {{ "{" }}{{ range $i, $v := .Values }}{{ if $i }}, {{ end }}"{{ $v.Value }}"{{ end }}{{ "}" }},
{{- end }}
	}
}
`
	t, err := template.New("govocabaccess").Parse(tmpl)
	if err != nil {
		return "", err
	}
	var buf strings.Builder
	if err := t.Execute(&buf, struct{ Vocabularies []vocabAccessRow }{vocabs}); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func validate(c *Contract) error {
	groupRe := regexp.MustCompile(`^[a-z][a-z0-9-]+$`)
	identRe := regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	knownServices := map[string]struct{}{
		"state":                  {},
		"orchestrator":           {},
		"execution-controller":   {},
		"topology-controller":    {},
		"release-controller":     {},
		"remediation":            {},
		"agent-remediation":      {},
		"dead-letter-controller": {},
	}

	streamNames := map[string]int{}
	streamConsts := map[string]int{}
	groupNames := map[string]int{}
	groupConsts := map[string]int{}

	for i, s := range c.Streams {
		if !identRe.MatchString(s.Const) {
			return fmt.Errorf("stream %q: const %q is not a valid identifier", s.Name, s.Const)
		}
		if prev, ok := streamNames[s.Name]; ok {
			return fmt.Errorf("duplicate stream name %q at index %d (first seen at %d)", s.Name, i, prev)
		}
		streamNames[s.Name] = i
		if prev, ok := streamConsts[s.Const]; ok {
			return fmt.Errorf("duplicate stream const %q at index %d (first seen at %d)", s.Const, i, prev)
		}
		streamConsts[s.Const] = i

		for j, cons := range s.Consumers {
			if _, ok := knownServices[cons.Service]; !ok {
				return fmt.Errorf("stream %q consumer[%d]: unknown service %q", s.Name, j, cons.Service)
			}
			if !groupRe.MatchString(cons.Group) {
				return fmt.Errorf("stream %q consumer[%d]: group %q must match %s", s.Name, j, cons.Group, groupRe.String())
			}
			if !identRe.MatchString(cons.Const) {
				return fmt.Errorf("stream %q consumer[%d]: const %q is not a valid identifier", s.Name, j, cons.Const)
			}
			if prev, ok := groupNames[cons.Group]; ok {
				return fmt.Errorf("duplicate consumer group %q on stream %q[%d] (first seen at stream index %d)", cons.Group, s.Name, j, prev)
			}
			groupNames[cons.Group] = i
			if prev, ok := groupConsts[cons.Const]; ok {
				return fmt.Errorf("duplicate consumer const %q on stream %q[%d] (first seen at stream index %d)", cons.Const, s.Name, j, prev)
			}
			groupConsts[cons.Const] = i
		}
	}

	valueRe := regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)
	vocabNames := map[string]struct{}{}
	vocabConsts := map[string]struct{}{}
	for _, v := range c.Vocabularies {
		if !identRe.MatchString(v.Const) {
			return fmt.Errorf("vocabulary %q: const %q is not a valid identifier", v.Name, v.Const)
		}
		if _, dup := vocabNames[v.Name]; dup {
			return fmt.Errorf("duplicate vocabulary name %q", v.Name)
		}
		vocabNames[v.Name] = struct{}{}
		if _, dup := vocabConsts[v.Const]; dup {
			return fmt.Errorf("duplicate vocabulary const %q", v.Const)
		}
		vocabConsts[v.Const] = struct{}{}
		if len(v.Values) == 0 {
			return fmt.Errorf("vocabulary %q declares no values", v.Name)
		}
		values := map[string]struct{}{}
		consts := map[string]struct{}{}
		for i, val := range v.Values {
			if !valueRe.MatchString(val.Value) {
				return fmt.Errorf("vocabulary %q value[%d]: %q must match %s", v.Name, i, val.Value, valueRe.String())
			}
			if !identRe.MatchString(val.Const) {
				return fmt.Errorf("vocabulary %q value[%d]: const %q is not a valid identifier", v.Name, i, val.Const)
			}
			if _, dup := values[val.Value]; dup {
				return fmt.Errorf("vocabulary %q: duplicate value %q", v.Name, val.Value)
			}
			values[val.Value] = struct{}{}
			if _, dup := consts[val.Const]; dup {
				return fmt.Errorf("vocabulary %q: duplicate const %q", v.Name, val.Const)
			}
			consts[val.Const] = struct{}{}
		}
	}

	runtimeValues := map[string]struct{}{}
	runtimeDeclared := false
	for _, v := range c.Vocabularies {
		if v.Name == nodeRuntimeVocabulary {
			runtimeDeclared = true
			for _, val := range v.Values {
				runtimeValues[val.Value] = struct{}{}
			}
			continue
		}
		withRuntime := 0
		for _, val := range v.Values {
			if val.Runtime != "" {
				withRuntime++
			}
		}
		if withRuntime == 0 {
			continue
		}
		if withRuntime != len(v.Values) {
			return fmt.Errorf("vocabulary %q: runtime must be set on every value or none", v.Name)
		}
		if !runtimeDeclared {
			return fmt.Errorf("vocabulary %q uses runtime, so vocabulary %q must be declared before it", v.Name, nodeRuntimeVocabulary)
		}
		for i, val := range v.Values {
			if _, ok := runtimeValues[val.Runtime]; !ok {
				return fmt.Errorf("vocabulary %q value[%d]: runtime %q is not a value of %q", v.Name, i, val.Runtime, nodeRuntimeVocabulary)
			}
		}
	}
	return nil
}
