package main

import (
	_ "embed"
	"strings"
	"testing"
)

const validYAML = `
streams:
  - name: node.updated:v1
    const: NodeUpdatedV1
    description: Node state transitions.
    producers: [state]
    consumers:
      - service: orchestrator
        group: orchestrator-node-updated
        const: OrchestratorNodeUpdated
`

func TestParseContract_Valid(t *testing.T) {
	c, err := parseContract(strings.NewReader(validYAML))
	if err != nil {
		t.Fatalf("parseContract: %v", err)
	}
	if len(c.Streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(c.Streams))
	}
	s := c.Streams[0]
	if s.Name != "node.updated:v1" {
		t.Errorf("name: got %q", s.Name)
	}
	if s.Const != "NodeUpdatedV1" {
		t.Errorf("const: got %q", s.Const)
	}
	if len(s.Consumers) != 1 || s.Consumers[0].Group != "orchestrator-node-updated" {
		t.Errorf("consumers: %+v", s.Consumers)
	}
}

func TestValidate_DuplicateStreamName(t *testing.T) {
	y := `
streams:
  - name: a:v1
    const: AV1
    producers: [state]
    consumers: []
  - name: a:v1
    const: BV1
    producers: [state]
    consumers: []
`
	_, err := loadAndValidate(strings.NewReader(y))
	if err == nil || !strings.Contains(err.Error(), "duplicate stream name") {
		t.Fatalf("expected duplicate stream name error, got %v", err)
	}
}

func TestValidate_DuplicateStreamConst(t *testing.T) {
	y := `
streams:
  - name: a:v1
    const: SameConst
    producers: [state]
    consumers: []
  - name: b:v1
    const: SameConst
    producers: [state]
    consumers: []
`
	_, err := loadAndValidate(strings.NewReader(y))
	if err == nil || !strings.Contains(err.Error(), "duplicate stream const") {
		t.Fatalf("expected duplicate stream const error, got %v", err)
	}
}

func TestValidate_DuplicateGroup(t *testing.T) {
	y := `
streams:
  - name: a:v1
    const: AV1
    producers: [state]
    consumers:
      - service: orchestrator
        group: same-group
        const: ConstA
  - name: b:v1
    const: BV1
    producers: [state]
    consumers:
      - service: orchestrator
        group: same-group
        const: ConstB
`
	_, err := loadAndValidate(strings.NewReader(y))
	if err == nil || !strings.Contains(err.Error(), "duplicate consumer group") {
		t.Fatalf("expected duplicate group error, got %v", err)
	}
}

func TestValidate_DuplicateGroupConst(t *testing.T) {
	y := `
streams:
  - name: a:v1
    const: AV1
    producers: [state]
    consumers:
      - service: orchestrator
        group: group-a
        const: SameConst
  - name: b:v1
    const: BV1
    producers: [state]
    consumers:
      - service: orchestrator
        group: group-b
        const: SameConst
`
	_, err := loadAndValidate(strings.NewReader(y))
	if err == nil || !strings.Contains(err.Error(), "duplicate consumer const") {
		t.Fatalf("expected duplicate consumer const error, got %v", err)
	}
}

func TestValidate_GroupNotHyphenated(t *testing.T) {
	y := `
streams:
  - name: a:v1
    const: AV1
    producers: [state]
    consumers:
      - service: orchestrator
        group: orchestrator_node_updated
        const: OrchestratorNodeUpdated
`
	_, err := loadAndValidate(strings.NewReader(y))
	if err == nil || !strings.Contains(err.Error(), "group") || !strings.Contains(err.Error(), "must match") {
		t.Fatalf("expected naming policy error, got %v", err)
	}
}

func TestValidate_UnknownService(t *testing.T) {
	y := `
streams:
  - name: a:v1
    const: AV1
    producers: [state]
    consumers:
      - service: nope-controller
        group: nope-group
        const: NopeGroup
`
	_, err := loadAndValidate(strings.NewReader(y))
	if err == nil || !strings.Contains(err.Error(), "unknown service") {
		t.Fatalf("expected unknown service error, got %v", err)
	}
}

func TestValidate_GroupUppercase(t *testing.T) {
	y := `
streams:
  - name: a:v1
    const: AV1
    producers: [state]
    consumers:
      - service: orchestrator
        group: Orchestrator-Node-Updated
        const: OrchestratorNodeUpdated
`
	_, err := loadAndValidate(strings.NewReader(y))
	if err == nil {
		t.Fatal("expected uppercase rejection")
	}
}

func TestValidate_GoIdentifier(t *testing.T) {
	y := `
streams:
  - name: a:v1
    const: 9NotIdentifier
    producers: [state]
    consumers: []
`
	_, err := loadAndValidate(strings.NewReader(y))
	if err == nil || !strings.Contains(err.Error(), "identifier") {
		t.Fatalf("expected identifier error, got %v", err)
	}
}

//go:embed testdata/golden.go.txt
var goldenGo string

func TestEmitGo(t *testing.T) {
	c, err := loadAndValidate(strings.NewReader(validYAML))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	got, err := emitGo(c)
	if err != nil {
		t.Fatalf("emitGo: %v", err)
	}
	if got != goldenGo {
		t.Fatalf("emitGo mismatch.\n--- got ---\n%s\n--- want ---\n%s", got, goldenGo)
	}
}

//go:embed testdata/golden.py.txt
var goldenPy string

const pythonRelevantYAML = `
streams:
  - name: update.graph:v1
    const: UpdateGraphV1
    description: Manifest refresh trigger.
    producers: [state]
    consumers:
      - service: topology-controller
        group: topology-controller-update-graph
        const: ManifestUpdateGraph
  - name: manifest.loaded:v1
    const: ManifestLoadedV1
    description: Topology after manifest load.
    producers: [topology-controller]
    consumers:
      - service: orchestrator
        group: orchestrator-manifest-loaded
        const: OrchestratorManifestLoaded
  - name: irrelevant.to.python:v1
    const: IrrelevantV1
    description: Should not appear in Python output.
    producers: [state]
    consumers:
      - service: orchestrator
        group: orchestrator-irrelevant
        const: OrchestratorIrrelevant
`

func TestEmitPython(t *testing.T) {
	c, err := loadAndValidate(strings.NewReader(pythonRelevantYAML))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	got, err := emitPythonStreams(c, "topology-controller")
	if err != nil {
		t.Fatalf("emitPythonStreams: %v", err)
	}
	if got != goldenPy {
		t.Fatalf("emitPythonStreams mismatch.\n--- got ---\n%s\n--- want ---\n%s", got, goldenPy)
	}
}

const vocabYAML = `
streams:
  - name: node.updated:v1
    const: NodeUpdatedV1
    description: Node state transitions.
    producers: [state]
    consumers: []
vocabularies:
  - name: parse_failure_kind
    const: ParseFailureKind
    description: Why topology-controller could not resolve a candidate release. Declaration order is precedence.
    values:
      - value: invalid_sql
        const: InvalidSQL
        healable: true
        description: sqlglot cannot parse a node's compiled SQL.
      - value: internal
        const: Internal
        healable: false
        description: continuo's own wiring or an S3 write failed.
`

func TestParseContract_Vocabularies(t *testing.T) {
	c, err := loadAndValidate(strings.NewReader(vocabYAML))
	if err != nil {
		t.Fatalf("loadAndValidate: %v", err)
	}
	if len(c.Vocabularies) != 1 || c.Vocabularies[0].Const != "ParseFailureKind" {
		t.Fatalf("vocabularies: %+v", c.Vocabularies)
	}
	if got := c.Vocabularies[0].Values[0]; got.Value != "invalid_sql" || got.Const != "InvalidSQL" || !got.Healable {
		t.Fatalf("first value: %+v", got)
	}
}

func TestValidate_VocabularyRejectsBadValueAndDuplicates(t *testing.T) {
	cases := map[string]string{
		"bad value": `
streams: []
vocabularies:
  - name: k
    const: K
    values:
      - {value: Invalid-SQL, const: A}
`,
		"duplicate value": `
streams: []
vocabularies:
  - name: k
    const: K
    values:
      - {value: a, const: A}
      - {value: a, const: B}
`,
		"duplicate const": `
streams: []
vocabularies:
  - name: k
    const: K
    values:
      - {value: a, const: A}
      - {value: b, const: A}
`,
		"empty": `
streams: []
vocabularies:
  - name: k
    const: K
    values: []
`,
	}
	for name, y := range cases {
		if _, err := loadAndValidate(strings.NewReader(y)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

// TestEmitGo_HoldsNoVocabulary pins the split: streams.gen.go carries stream
// and group names only, so nothing in a domain package needs to import it.
func TestEmitGo_HoldsNoVocabulary(t *testing.T) {
	c, err := loadAndValidate(strings.NewReader(vocabYAML))
	if err != nil {
		t.Fatal(err)
	}
	src, err := emitGo(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, unwanted := range []string{"ParseFailureKind", "Healable"} {
		if strings.Contains(src, unwanted) {
			t.Errorf("emitGo emits %q — vocabularies belong in pkg/domain/model\n%s", unwanted, src)
		}
	}
	access, err := emitGoTestAccess(c)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(access, "VocabularyValuesForTest") {
		t.Errorf("emitGoTestAccess emits the vocabulary accessor\n%s", access)
	}
}

func TestEmitGoVocabulary(t *testing.T) {
	c, err := loadAndValidate(strings.NewReader(vocabYAML))
	if err != nil {
		t.Fatal(err)
	}
	src, err := emitGoVocabulary(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"package model",
		"type ParseFailureKind string",
		`ParseFailureKindInvalidSQL ParseFailureKind = "invalid_sql"`,
		`ParseFailureKindInternal ParseFailureKind = "internal"`,
		"func ParseFailureKinds() []ParseFailureKind",
		"func (v ParseFailureKind) IsValid() bool",
		"func (v ParseFailureKind) Healable() bool",
		"case ParseFailureKindInvalidSQL:\n\t\treturn true",
	} {
		if !strings.Contains(src, want) {
			t.Errorf("emitGoVocabulary missing %q\n%s", want, src)
		}
	}
	access, err := emitGoVocabularyTestAccess(c)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(access, "package model") {
		t.Errorf("emitGoVocabularyTestAccess is not in package model\n%s", access)
	}
	if !strings.Contains(access, `"ParseFailureKind": {"invalid_sql", "internal"}`) {
		t.Errorf("emitGoVocabularyTestAccess missing vocabulary map\n%s", access)
	}
}

// TestEmitPythonStreams_HoldsNoVocabulary is the Python half of the split:
// streams_contract.py carries stream and group names only.
func TestEmitPythonStreams_HoldsNoVocabulary(t *testing.T) {
	c, err := loadAndValidate(strings.NewReader(vocabYAML))
	if err != nil {
		t.Fatal(err)
	}
	src, err := emitPythonStreams(c, "state")
	if err != nil {
		t.Fatal(err)
	}
	for _, unwanted := range []string{"StrEnum", "ParseFailureKind"} {
		if strings.Contains(src, unwanted) {
			t.Errorf("emitPythonStreams emits %q — vocabularies belong in the domain module\n%s", unwanted, src)
		}
	}
}

func TestEmitPythonVocabulary(t *testing.T) {
	c, err := loadAndValidate(strings.NewReader(vocabYAML))
	if err != nil {
		t.Fatal(err)
	}
	src, err := emitPythonVocabulary(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"from enum import StrEnum",
		"class ParseFailureKind(StrEnum):",
		`    INVALID_SQL = "invalid_sql"`,
		`    INTERNAL = "internal"`,
		"PARSE_FAILURE_KIND_HEALABLE = frozenset({ParseFailureKind.INVALID_SQL})",
	} {
		if !strings.Contains(src, want) {
			t.Errorf("emitPythonVocabulary missing %q\n%s", want, src)
		}
	}
}
