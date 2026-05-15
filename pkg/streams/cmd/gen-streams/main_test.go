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
