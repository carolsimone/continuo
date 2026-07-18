//go:build integration

package neo4jinfra_test

import (
	"os"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
)

// TestMain serialises this package's Neo4j graph mutations against the other
// integration test binaries (notably service/handlers) that share the same
// Neo4j instance. See AcquireGraphTestLock for why concurrent runs would
// otherwise delete one another's :Table fixtures.
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
