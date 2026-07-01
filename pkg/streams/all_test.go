package streams_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/carolsimone/continuo/pkg/streams"
	"gopkg.in/yaml.v3"
)

// TestAllMatchesContract guards streams.All against drift from contract.yaml:
// every stream in the contract must appear in All and vice versa, so a stream
// added to the contract fails CI until it is added to All (which the stream
// reaper trims). yamlContract is defined in contract_test.go (same package).
func TestAllMatchesContract(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("contract.yaml"))
	if err != nil {
		t.Fatalf("read contract.yaml: %v", err)
	}
	var c yamlContract
	if err := yaml.Unmarshal(data, &c); err != nil {
		t.Fatalf("parse contract.yaml: %v", err)
	}

	inAll := make(map[string]bool, len(streams.All))
	for _, s := range streams.All {
		inAll[s] = true
	}

	if len(streams.All) != len(c.Streams) {
		t.Fatalf("streams.All has %d entries, contract.yaml has %d streams — update pkg/streams/all.go",
			len(streams.All), len(c.Streams))
	}
	for _, s := range c.Streams {
		if !inAll[s.Name] {
			t.Errorf("stream %q in contract.yaml is missing from streams.All", s.Name)
		}
	}
}
