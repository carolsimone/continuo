package streams_test

import (
	"os"
	"path/filepath"
	"testing"
)

// The Python consumer's tests read their own copy of the dead-letter fixture;
// the Go and Python producers are pinned to the same entry only while the two
// copies are identical. The test reads both from the repository checkout, so it
// runs where the whole checkout is present (make guards), not in a service's dev
// container, which mounts only some directories.
func TestConsumerDeadLetterFixture_PythonCopyIsIdentical(t *testing.T) {
	root := repoRootFromTest(t)
	goPath := filepath.Join(root, "pkg", "events", "testdata", "consumer_dead_letter_v1.json")
	pyPath := filepath.Join(root, "topology-controller", "tests", "fixtures", "consumer_dead_letter_v1.json")
	goCopy, err := os.ReadFile(goPath) //nolint:gosec // G304: path is built from the repo root and fixed segments
	if err != nil {
		t.Fatalf("read %s: %v", goPath, err)
	}
	pyCopy, err := os.ReadFile(pyPath) //nolint:gosec // G304: path is built from the repo root and fixed segments
	if err != nil {
		t.Fatalf("read %s: %v", pyPath, err)
	}
	if string(goCopy) != string(pyCopy) {
		t.Errorf("%s and %s differ; the Go and Python dead-letter fixtures must be identical", goPath, pyPath)
	}
}
