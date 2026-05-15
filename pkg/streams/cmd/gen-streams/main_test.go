package main

import (
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
