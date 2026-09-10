// Command gen-streams reads pkg/streams/contract.yaml and writes
// pkg/streams/streams.gen.go, pkg/streams/streams_test_access.gen.go,
// and topology-controller/streams_contract.py.
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
	Description string `yaml:"description"`
}

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

	pySrc, err := emitPython(c, "topology-controller")
	if err != nil {
		return fmt.Errorf("emit python: %w", err)
	}
	pyOut := filepath.Join(root, "topology-controller", "streams_contract.py")
	if err := os.WriteFile(pyOut, []byte(pySrc), 0o600); err != nil {
		return fmt.Errorf("write python: %w", err)
	}

	fmt.Fprintln(os.Stderr, "gen-streams: wrote", goOut)
	fmt.Fprintln(os.Stderr, "gen-streams: wrote", accessOut)
	fmt.Fprintln(os.Stderr, "gen-streams: wrote", pyOut)
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

	// Type is repeated on every value row because a nested {{ range }} cannot
	// reach the enclosing vocabulary's fields.
	type valueRow struct {
		Const, Type, Value, Description string
		Healable                        bool
	}
	type vocabRow struct {
		Const, Name, Description string
		Values                   []valueRow
	}
	var vocabRows []vocabRow
	for _, v := range c.Vocabularies {
		row := vocabRow{Const: v.Const, Name: v.Name, Description: v.Description}
		for _, val := range v.Values {
			row.Values = append(row.Values, valueRow{Const: v.Const + val.Const, Type: v.Const, Value: val.Value, Description: val.Description, Healable: val.Healable})
		}
		vocabRows = append(vocabRows, row)
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

// {{ .Const }}s returns every value in contract order, which is also the
// precedence a consumer applies when several apply at once.
func {{ .Const }}s() []{{ .Const }} {
	return []{{ .Const }}{
{{- range .Values }}
		{{ .Const }},
{{- end }}
	}
}

// IsValid reports whether k is a value declared in contract.yaml.
func (k {{ .Const }}) IsValid() bool {
	switch k {
{{- range .Values }}
	case {{ .Const }}:
		return true
{{- end }}
	}
	return false
}

// Healable reports whether the contract marks k as fixable by a change to the
// user's source, and therefore worth a remediation attempt.
func (k {{ .Const }}) Healable() bool {
	switch k {
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
`
	t, err := template.New("go").Parse(tmpl)
	if err != nil {
		return "", err
	}
	var buf strings.Builder
	err = t.Execute(&buf, struct {
		Streams      []struct{ Const, Name, Description string }
		Groups       []groupRow
		Vocabularies []vocabRow
	}{streamRows, groupRows, vocabRows})
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func emitPython(c *Contract, service string) (string, error) {
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

	type pyValueRow struct{ Const, Value string }
	type pyVocabRow struct {
		Const, Description string
		Values             []pyValueRow
		Healable           []string // "ParseFailureKind.INVALID_SQL", ...
		HealableConst      string   // "PARSE_FAILURE_KIND_HEALABLE"
	}
	var vocabs []pyVocabRow
	for _, v := range c.Vocabularies {
		row := pyVocabRow{Const: v.Const, Description: v.Description, HealableConst: toScreamingSnake(v.Const) + "_HEALABLE"}
		for _, val := range v.Values {
			member := strings.ToUpper(val.Value)
			row.Values = append(row.Values, pyValueRow{Const: member, Value: val.Value})
			if val.Healable {
				row.Healable = append(row.Healable, v.Const+"."+member)
			}
		}
		vocabs = append(vocabs, row)
	}

	tmpl := `# Code generated by gen-streams. DO NOT EDIT.
# Source: pkg/streams/contract.yaml
{{ if .Vocabularies }}
from enum import StrEnum
{{ end }}
{{ range .Streams }}{{ .Const }} = "{{ .Name }}"
"""{{ .Description }}"""

{{ end }}{{ range .Groups }}{{ .Const }} = "{{ .Group }}"
"""{{ $.Service }} consumer group on {{ .Stream }}."""

{{ end }}{{ range .Vocabularies }}
class {{ .Const }}(StrEnum):
    """{{ .Description }}"""
{{ range .Values }}    {{ .Const }} = "{{ .Value }}"
{{ end }}

{{ .HealableConst }} = frozenset({ {{- range $i, $h := .Healable }}{{ if $i }}, {{ end }}{{ $h }}{{ end -}} })
"""Values of {{ .Const }} a remediation attempt can fix by changing the user's source."""

{{ end }}`

	t, err := template.New("py").Parse(tmpl)
	if err != nil {
		return "", err
	}
	var buf strings.Builder
	if err := t.Execute(&buf, struct {
		Service      string
		Streams      []streamRow
		Groups       []groupRow
		Vocabularies []pyVocabRow
	}{service, streams, groups, vocabs}); err != nil {
		return "", err
	}
	out := strings.TrimRight(buf.String(), "\n") + "\n"
	return out, nil
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

// VocabularyValuesForTest returns every vocabulary's values from contract.yaml
// in declaration order, keyed by the vocabulary's Go type name. Test-only
// accessor used by contract_test.go to verify YAML ↔ generated-Go parity.
func VocabularyValuesForTest() map[string][]string {
	return map[string][]string{
{{- range .Vocabularies }}
		"{{ .Const }}": {{ "{" }}{{ range $i, $v := .Values }}{{ if $i }}, {{ end }}"{{ $v.Value }}"{{ end }}{{ "}" }},
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
		Streams      []row
		Groups       []row
		Vocabularies []vocabAccessRow
	}{streams, groups, vocabs}); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func validate(c *Contract) error {
	groupRe := regexp.MustCompile(`^[a-z][a-z0-9-]+$`)
	identRe := regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	knownServices := map[string]struct{}{
		"state":               {},
		"orchestrator":        {},
		"executor-controller": {},
		"k8s-controller":      {},
		"topology-controller": {},
		"release-controller":  {},
		"remediation":         {},
		"agent-remediation":   {},
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

	valueRe := regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
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
	return nil
}
