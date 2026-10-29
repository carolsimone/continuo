package streams_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestE2EClientsAreClosedBySetupClients fails when an e2e test closes its
// clients itself. setupClients registers the close with t.Cleanup before it
// returns, so every cleanup a test or a helper registers afterwards (they run
// last in, first out) still has open pools. A test that defers the close runs it
// before any t.Cleanup, and a cleanup that deletes seeded rows then fails on a
// closed Neo4j driver or Postgres pool: that is how a seeding helper called from
// a test in another file broke.
func TestE2EClientsAreClosedBySetupClients(t *testing.T) {
	dir := filepath.Join(repoRootFromTest(t), "tests", "e2e")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || e.Name() == "clients.go" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name())) //nolint:gosec // G304: the path is a file of tests/e2e under the repository root
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(raw), "\n") {
			code := strings.TrimSpace(line)
			if strings.HasPrefix(code, "//") {
				continue
			}
			if strings.Contains(code, "clients.close(") {
				t.Errorf("tests/e2e/%s:%d: %q: setupClients closes the clients; a test must not", e.Name(), i+1, code)
			}
		}
	}
}
