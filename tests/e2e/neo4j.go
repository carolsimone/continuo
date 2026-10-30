package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// queryNeo4jRecords runs a read cypher query against the shared in-process
// Neo4j driver (clients.neo4jDriver, dialed once by setupClients and already
// used by the topology-seeding helpers of the single-node and batched
// agent-remediation tests) and returns every result record. Provenance
// assertions read through this same bolt connection rather than shelling out
// to cypher-shell in a sibling container, since the driver is already here.
func queryNeo4jRecords(t *testing.T, ctx context.Context, clients *testClients, cypher string, params map[string]any) []*neo4jdriver.Record {
	t.Helper()
	session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode: neo4jdriver.AccessModeRead,
	})
	defer func() { _ = session.Close(ctx) }()

	result, err := session.Run(ctx, cypher, params)
	require.NoError(t, err, "run cypher: %s", cypher)

	records, err := result.Collect(ctx)
	require.NoError(t, err, "collect cypher result: %s", cypher)
	return records
}

// deleteSeededNodes removes the default tenant's :Node nodes with the given
// unique ids, with their relationships, and fails the test when it cannot: a
// node left behind leaks into the topology the tests after it read. It matches
// the ids it is given and the tenant, so it never touches another node. It runs
// on a context of its own, so a test whose context has ended still cleans up.
// Register it with t.Cleanup right after the clients are set up, and the close
// of the clients (also a t.Cleanup, registered first) runs after it.
func deleteSeededNodes(t *testing.T, clients *testClients, ids ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode: neo4jdriver.AccessModeWrite,
	})
	defer func() { _ = session.Close(ctx) }()

	result, err := session.Run(ctx, `MATCH (t:Node {tenant_id: 'default'}) WHERE t.unique_id IN $ids DETACH DELETE t`,
		map[string]any{"ids": ids})
	if err == nil {
		_, err = result.Consume(ctx)
	}
	if err != nil {
		t.Errorf("remove the seeded :Node nodes %v from Neo4j: %v", ids, err)
	}
}

// queryNeo4jRows runs cypher and returns every record as a column-name-keyed
// map, for assertions that read more than one column or more than one row —
// e.g. one row per (target :Node, amended) pair off an EDITED fan-out.
func queryNeo4jRows(t *testing.T, ctx context.Context, clients *testClients, cypher string, params map[string]any) []map[string]any {
	t.Helper()
	records := queryNeo4jRecords(t, ctx, clients, cypher, params)
	rows := make([]map[string]any, 0, len(records))
	for _, rec := range records {
		row := make(map[string]any, len(rec.Keys))
		for _, k := range rec.Keys {
			row[k], _ = rec.Get(k)
		}
		rows = append(rows, row)
	}
	return rows
}

// pollNeo4jPRState polls a (proposal, service) :PullRequest node's pr_state
// until it reaches want. Catalog's pr_closed provenance handler
// (catalog/adapters/neo4j/case_base_repository.go RecordPullRequestOutcome)
// writes this state change and every case-base provenance edge it draws
// (RESOLVED_BY, EDITED) in ONE Cypher statement — so once this poll observes
// want, the edges from that same write are visible too and a caller can read
// them with a plain, unpolled query afterward, without a separate race
// against catalog's asynchronous consumption of
// remediation.pr_closed:v1.
func pollNeo4jPRState(
	t *testing.T, ctx context.Context, clients *testClients,
	proposalID, service, want string, timeout time.Duration,
) {
	t.Helper()
	var last string
	pollUntil(t, ctx, timeout, 2*time.Second, func() (bool, error) {
		rows := queryNeo4jRows(t, ctx, clients, `
			MATCH (:Proposal {tenant_id: 'default', proposal_id: $proposal_id})-[:HAS_PR]->(pl:PullRequest {tenant_id: 'default', service: $service})
			RETURN pl.pr_state AS pr_state`,
			map[string]any{"proposal_id": proposalID, "service": service})
		if len(rows) != 1 {
			return false, nil
		}
		state, _ := rows[0]["pr_state"].(string)
		if state != last {
			t.Logf("neo4j :PullRequest proposal=%s service=%q pr_state=%s", proposalID, service, state)
			last = state
		}
		return state == want, nil
	}, fmt.Sprintf("timeout waiting for neo4j :PullRequest proposal=%s service=%q to reach pr_state=%q",
		proposalID, service, want))
}
