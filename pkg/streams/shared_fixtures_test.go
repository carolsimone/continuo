package streams_test

import (
	"os"
	"path/filepath"
	"testing"
)

// sharedFixtures pairs each golden fixture the Go tests read with the copy
// topology-controller's tests read. A Go and a Python implementation of one
// contract are pinned to each other only while the two copies are identical.
var sharedFixtures = []struct{ goPath, pyPath string }{
	{"pkg/events/testdata/consumer_dead_letter_v1.json", "topology-controller/tests/fixtures/consumer_dead_letter_v1.json"},
	{"pkg/topologyartifact/testdata/topology_v1.json", "topology-controller/tests/fixtures/topology_v1.json"},
	{"pkg/topologyartifact/testdata/candidate_object_keys_v1.json", "topology-controller/tests/fixtures/candidate_object_keys_v1.json"},
}

// The test reads both copies from the repository checkout, so it runs where
// the whole checkout is present (make guards), not in a service's dev
// container, which mounts only some directories.
func TestSharedFixtures_PythonCopiesAreIdentical(t *testing.T) {
	root := repoRootFromTest(t)
	for _, f := range sharedFixtures {
		goPath := filepath.Join(root, filepath.FromSlash(f.goPath))
		pyPath := filepath.Join(root, filepath.FromSlash(f.pyPath))
		goCopy, err := os.ReadFile(goPath) //nolint:gosec // G304: path is built from the repo root and fixed segments
		if err != nil {
			t.Fatalf("read %s: %v", goPath, err)
		}
		pyCopy, err := os.ReadFile(pyPath) //nolint:gosec // G304: path is built from the repo root and fixed segments
		if err != nil {
			t.Fatalf("read %s: %v", pyPath, err)
		}
		if string(goCopy) != string(pyCopy) {
			t.Errorf("%s and %s differ; the Go and Python copies of a shared fixture must be identical", f.goPath, f.pyPath)
		}
	}
}
