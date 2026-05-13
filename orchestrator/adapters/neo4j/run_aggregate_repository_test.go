//go:build integration
// +build integration

package neo4jinfra_test

// RunAggregateRepository integration tests.
// Run with: go test ./adapters/neo4j/... -tags integration -v
//
// These tests require a live Neo4j instance accessible at the URI in
// test environment variables (see test_helpers_test.go for the env conventions).

import (
	"testing"
)

func TestRunAggregateRepository_LoadHintFull_ReturnsAllNodes(t *testing.T) {
	// Setup: MERGE a :Run with total_nodes=2, terminal_count=0, version=0;
	// MERGE two :Table nodes and :EXECUTES edges with status=PENDING.
	// Call Load(runID, LoadHintFull{}).
	// Assert: TotalNodes=2, TerminalCount=0, Version=0, len(Nodes())==2.
	t.Skip("requires Neo4j fixture wiring — to be implemented against live container")
}

func TestRunAggregateRepository_LoadHintNodeCompletion_Failed_IncludesTransitiveDownstream(t *testing.T) {
	// Setup: A -> B -> C chain.
	// Call Load(runID, LoadHintNodeCompletion{Key: A, Status: "FAILED"}).
	// Assert: A, B, C are all loaded.
	t.Skip("requires Neo4j fixture wiring — to be implemented against live container")
}

func TestRunAggregateRepository_LoadHintNodeCompletion_Succeeded_IncludesNeighbourhood(t *testing.T) {
	// Setup: A -> C, B -> C (C depends on both A and B).
	// Call Load(runID, LoadHintNodeCompletion{Key: A, Status: "SUCCEEDED"}).
	// Assert: A, C, and B (C's other upstream) are all loaded.
	t.Skip("requires Neo4j fixture wiring — to be implemented against live container")
}

func TestRunAggregateRepository_Save_PersistsNodeStatusesAndCounters(t *testing.T) {
	// Setup: seed a run, Load, CompleteNode, Save, reload via LoadHintFull.
	// Assert: node status updated, TerminalCount incremented, Version bumped.
	t.Skip("requires Neo4j fixture wiring — to be implemented against live container")
}

func TestRunAggregateRepository_Save_StaleVersion_ReturnsErrVersionConflict(t *testing.T) {
	// Setup: Load twice, mutate + Save first one → ok. Save second one (stale) → ErrVersionConflict.
	t.Skip("requires Neo4j fixture wiring — to be implemented against live container")
}
