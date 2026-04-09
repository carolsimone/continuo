package e2e

import (
	"context"
	"os/exec"
	"testing"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// cleanupTestData removes all test data from all systems
func cleanupTestData(t *testing.T, ctx context.Context, clients *testClients, scheduleName string) {
	t.Log("Cleaning up test data...")

	// Clean Neo4j
	cleanupNeo4j(t, ctx, clients, scheduleName)

	// Clean PostgreSQL databases
	cleanupPostgres(t, ctx, clients, scheduleName)

	// Clean Redis streams
	cleanupRedis(t, ctx, clients)

	// Clean Kubernetes jobs
	cleanupK8s(t, ctx)

	t.Log("✅ Cleanup complete")
}

// cleanupNeo4j removes test nodes from Neo4j
func cleanupNeo4j(t *testing.T, ctx context.Context, clients *testClients, scheduleName string) {
	session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode: neo4jdriver.AccessModeWrite,
	})
	defer session.Close(ctx)

	_, err := session.Run(ctx, `
		MATCH (t:Table {schedule_name: $schedule_name})
		DETACH DELETE t
	`, map[string]interface{}{
		"schedule_name": scheduleName,
	})
	if err != nil {
		t.Logf("Warning: Failed to cleanup Neo4j: %v", err)
	}

	// The manifest reload in fixBrokenTableEAndReloadGraph causes the manifest-controller
	// to overwrite failure DAG nodes' schedule_name from "e2e-schedule-failure" back to
	// "e2e-schedule" (their value in the regular service manifests). The DETACH DELETE above
	// misses those nodes because their schedule_name was changed. Reset their status to NULL
	// so stale SUCCEEDED/FAILED values don't pollute subsequent test runs.
	if scheduleName == failureTestScheduleName {
		tableNames := make([]interface{}, 0, len(getDiamondDAG()))
		for _, n := range getDiamondDAG() {
			tableNames = append(tableNames, n.Name)
		}
		_, err = session.Run(ctx, `
			MATCH (t:Table)
			WHERE t.table_name IN $table_names
			REMOVE t.status
		`, map[string]interface{}{"table_names": tableNames})
		if err != nil {
			t.Logf("Warning: Failed to reset Neo4j node statuses for failure DAG: %v", err)
		}
	}
}

// cleanupPostgres removes test data from all PostgreSQL databases
func cleanupPostgres(t *testing.T, ctx context.Context, clients *testClients, scheduleName string) {
	// Get scheduler ID first (needed for outbox cleanup)
	var schedulerID string
	_ = clients.stateDB.Get(&schedulerID, `
		SELECT schedule_id FROM scheduler_tracker WHERE schedule_name = $1
	`, scheduleName)

	// Clean startup_outbox
	if schedulerID != "" {
		_, _ = clients.startupDB.Exec("DELETE FROM startup_outbox WHERE aggregate_id = $1", schedulerID)
	}

	// Clean deployment_outbox
	_, _ = clients.executorDB.Exec("DELETE FROM deployment_outbox WHERE schedule_name = $1", scheduleName)

	// Clean processed_events (deduplication table) - must be cleared so re-runs can process the same outbox IDs
	_, _ = clients.executorDB.Exec("DELETE FROM processed_events")

	// Clean dependency outbox
	if schedulerID != "" {
		_, _ = clients.dependencyDB.Exec("DELETE FROM outbox WHERE aggregate_id = $1", schedulerID)
	}

	// Clean k8s_status_outbox
	_, _ = clients.k8sDB.Exec("DELETE FROM k8s_status_outbox WHERE schedule_name = $1", scheduleName)

	// Clean state_outbox (written by rerun handler)
	if schedulerID != "" {
		_, _ = clients.stateDB.Exec("DELETE FROM state_outbox WHERE aggregate_id = $1", schedulerID)
	}

	// Clean state tables (do last to preserve scheduler_id)
	_, _ = clients.stateDB.Exec("DELETE FROM task_execution WHERE task_id IN (SELECT id FROM task_tracker WHERE schedule_id = $1)", schedulerID)
	_, _ = clients.stateDB.Exec("DELETE FROM task_tracker WHERE schedule_id = $1", schedulerID)
	_, _ = clients.stateDB.Exec("DELETE FROM scheduler_tracker WHERE schedule_name = $1", scheduleName)
}

// cleanupRedis removes test streams from Redis
func cleanupRedis(t *testing.T, ctx context.Context, clients *testClients) {
	streams := []string{
		"scheduler.started:v1",
		"query.model:v1",
		"executor.deployed:v1",
		"k8s.check:v1",
		"task.retry:v1",
		"task.failed:v1",
		"update.table:v1",
		"command.rerun:v1",
	}

	for _, stream := range streams {
		clients.redisClient.Del(ctx, stream)
	}
}

// cleanupK8s removes test jobs from Kubernetes
func cleanupK8s(t *testing.T, ctx context.Context) {
	// Use the schedule-id label to match what executor-controller actually creates
	cmd := exec.CommandContext(ctx, "kubectl", "delete", "jobs",
		"-n", "default",
		"-l", "app=query-executor",
		"--ignore-not-found=true")
	if err := cmd.Run(); err != nil {
		t.Logf("Warning: Failed to cleanup k8s jobs: %v", err)
	}
}
