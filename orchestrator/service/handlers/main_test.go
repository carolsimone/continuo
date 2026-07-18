//go:build integration

package handlers_test

import (
	"os"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
)

// TestMain serialises this package's release-promotion topology mutations
// against the other integration test binaries (notably adapters/neo4j) that
// share the same Neo4j instance. The release-promotion path truncates the whole
// :Table graph, so concurrent runs would delete one another's fixtures; the
// shared lock keeps the graph-mutating packages mutually exclusive.
func TestMain(m *testing.M) {
	release, err := neo4jinfra.AcquireGraphTestLock()
	if err == nil {
		neo4jinfra.ResetGraphForTest()
	}
	code := m.Run()
	if err == nil {
		neo4jinfra.ResetGraphForTest()
		release()
	}
	os.Exit(code)
}
