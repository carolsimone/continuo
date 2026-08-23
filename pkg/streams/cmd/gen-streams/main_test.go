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
	got, err := emitPython(c, "topology-controller")
	if err != nil {
		t.Fatalf("emitPython: %v", err)
	}
	if got != goldenPy {
		t.Fatalf("emitPython mismatch.\n--- got ---\n%s\n--- want ---\n%s", got, goldenPy)
	}
}
