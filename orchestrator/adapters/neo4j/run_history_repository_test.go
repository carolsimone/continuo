package neo4jinfra_test

import (
	"context"
	"log/slog"
	"os"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newRunHistoryRepo builds a RunHistoryRepository against the live Neo4j and
// deletes every node the test tags with its name, before and after it runs.
func newRunHistoryRepo(t *testing.T) (*neo4jinfra.RunHistoryRepository, neo4jinfra.Neo4jClient) {
	t.Helper()
	client := newTestClient(t)
	marker := t.Name()
	wipe := func() {
		ctx := context.Background()
		session := client.NewSession(ctx, neo4j.AccessModeWrite)
		defer func() { _ = session.Close(ctx) }()
		_, _ = session.Run(ctx, "MATCH (n) WHERE n.test_marker = $m DETACH DELETE n", map[string]any{"m": marker})
	}
	wipe()
	t.Cleanup(wipe)
	return neo4jinfra.NewRunHistoryRepository(client, slog.New(slog.NewTextHandler(os.Stderr, nil))), client
}

// seedHistoryRun creates a :Run that EXECUTES one :Table of its own, created
// createdDaysAgo days ago and completed completedDaysAgo days ago. A negative
// day count leaves that timestamp unset.
func seedHistoryRun(t *testing.T, client neo4jinfra.Neo4jClient, runID string, createdDaysAgo, completedDaysAgo int) {
	t.Helper()
	ctx := context.Background()
	session := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer func() { _ = session.Close(ctx) }()
	params := map[string]any{
		"run_id":    runID,
		"uid":       t.Name() + "." + runID,
		"m":         t.Name(),
		"created":   createdDaysAgo,
		"completed": completedDaysAgo,
	}
	res, err := session.Run(ctx, `
		CREATE (r:Run {run_id: $run_id, test_marker: $m})
		CREATE (t:Table {unique_id: $uid, service_name: 'svc', schema_name: 'history',
		                 table_name: $run_id, test_marker: $m})
		CREATE (r)-[:EXECUTES]->(t)
		SET r.created_at = CASE WHEN $created >= 0 THEN datetime() - duration({days: $created}) END,
		    r.completed_at = CASE WHEN $completed >= 0 THEN datetime() - duration({days: $completed}) END
	`, params)
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
}

// historyRuns returns the run ids of the test's :Run nodes, sorted.
func historyRuns(t *testing.T, client neo4jinfra.Neo4jClient) []string {
	t.Helper()
	ctx := context.Background()
	session := client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()
	res, err := session.Run(ctx,
		"MATCH (r:Run {test_marker: $m}) RETURN r.run_id AS id ORDER BY id", map[string]any{"m": t.Name()})
	require.NoError(t, err)
	var ids []string
	for res.Next(ctx) {
		id, _ := res.Record().Get("id")
		ids = append(ids, id.(string))
	}
	require.NoError(t, res.Err())
	return ids
}

// A run is deleted once it completed more than the retention ago, or, never
// completed, once it was created more than the retention ago; a run with
// neither timestamp is deleted at once. The tables the runs executed stay.
func TestRunHistoryRepository_DeleteExpiredRuns(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client := newRunHistoryRepo(t)
	seedHistoryRun(t, client, "completed-old", 11, 10)
	seedHistoryRun(t, client, "completed-recent", 2, 1)
	seedHistoryRun(t, client, "unfinished-old", 10, -1)
	seedHistoryRun(t, client, "unfinished-recent", 1, -1)
	seedHistoryRun(t, client, "unstamped", -1, -1)

	require.NoError(t, repo.DeleteExpiredRuns(context.Background(), 7))

	assert.Equal(t, []string{"completed-recent", "unfinished-recent"}, historyRuns(t, client))

	ctx := context.Background()
	session := client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()
	res, err := session.Run(ctx, "MATCH (t:Table {test_marker: $m}) RETURN count(t) AS n", map[string]any{"m": t.Name()})
	require.NoError(t, err)
	rec, err := res.Single(ctx)
	require.NoError(t, err)
	n, _ := rec.Get("n")
	assert.Equal(t, int64(5), n, "deleting a run keeps the tables it executed")
}
