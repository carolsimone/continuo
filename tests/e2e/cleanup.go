package e2e

import (
	"context"
	"os/exec"
	"testing"

	"github.com/jmoiron/sqlx"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// cleanupTestData removes all test data from all systems
func cleanupTestData(t *testing.T, ctx context.Context, clients *testClients, scheduleName string) {
	t.Log("Cleaning up test data...")

	// Clean Neo4j
	cleanupNeo4j(t, ctx, clients, scheduleName)

	// Clean Redis streams before Postgres dedup tables: deleting consumer-side
	// dedup state (message_processing, processed_events) while the k8s consumer
	// still has pending node.deployed:v1 / check.k8s:v1 messages re-enables
	// those messages and can trigger replays that recreate k8s_outbox rows
	// before the streams are gone.
	cleanupRedis(t, ctx, clients)

	// Clean PostgreSQL databases
	cleanupPostgres(t, ctx, clients, scheduleName)

	// Clean Kubernetes jobs
	cleanupK8s(t, ctx)

	t.Log("✅ Cleanup complete")
}

// cleanupNeo4j removes per-run snapshot data from Neo4j but preserves the Table
// topology. Each schedule run creates its own Run node with EXECUTES edges via
// SnapshotGraph, so the Table nodes (and their DEPENDS_ON edges) are shared
// infrastructure that must persist across runs.
func cleanupNeo4j(t *testing.T, ctx context.Context, clients *testClients, scheduleName string) {
	session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode: neo4jdriver.AccessModeWrite,
	})
	defer session.Close(ctx)

	// Delete Run snapshot nodes (and their EXECUTES edges) created by e2e tests.
	_, err := session.Run(ctx, `
		MATCH (run:Run {schedule_name: $schedule_name})
		DETACH DELETE run
	`, map[string]interface{}{
		"schedule_name": scheduleName,
	})
	if err != nil {
		t.Logf("Warning: Failed to cleanup Neo4j Run nodes: %v", err)
	}

	// Reset stale status properties on Table nodes so previous test results
	// don't pollute subsequent runs.
	_, err = session.Run(ctx, `
		MATCH (t:Table {schedule_name: $schedule_name})
		REMOVE t.status
	`, map[string]interface{}{
		"schedule_name": scheduleName,
	})
	if err != nil {
		t.Logf("Warning: Failed to reset Neo4j node statuses: %v", err)
	}
}

// cleanupPostgres removes test data from all PostgreSQL databases
func cleanupPostgres(t *testing.T, ctx context.Context, clients *testClients, scheduleName string) {
	// Get scheduler ID first (needed for outbox cleanup)
	var schedulerID string
	_ = clients.stateDB.Get(&schedulerID, `
		SELECT schedule_id FROM scheduler_tracker WHERE schedule_name = $1
	`, scheduleName)

	// Clean executor_outbox (renamed from deployment_outbox). The canonical
	// schema stores domain fields in JSONB payload — no schedule_name column —
	// so wipe the whole table. The e2e harness owns it for the duration of
	// the suite; leaving stale pending rows would cause pkg/outbox.Processor
	// to retry them forever, drowning Redis in noise that destabilises
	// manifest-controller's consumer group across subsequent tests.
	_, _ = clients.executorDB.Exec("DELETE FROM executor_outbox")

	// Clean processed_events on k8s (vestigial pre-pkg/messageprocessing dedup;
	// executor's processed_events table was dropped in executor V9).
	_, _ = clients.k8sDB.Exec("DELETE FROM processed_events")

	// Clean per-service message_processing (canonical consumer-side dedup
	// from #57). Must clear so re-runs aren't false-positive-deduped by
	// the new outbox_entry_id secondary uniqueness key.
	for _, db := range []*sqlx.DB{clients.stateDB, clients.orchestratorDB, clients.executorDB, clients.k8sDB} {
		_, _ = db.Exec("DELETE FROM message_processing")
	}

	// Clean orchestrator_outbox (renamed from outbox).
	if schedulerID != "" {
		_, _ = clients.orchestratorDB.Exec("DELETE FROM orchestrator_outbox WHERE aggregate_id = $1", schedulerID)
	}

	// Clean k8s_outbox (renamed from k8s_status_outbox; schedule_name lives
	// in the JSONB payload, not a column).
	_, _ = clients.k8sDB.Exec("DELETE FROM k8s_outbox")

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
		"node.deployed:v1",
		"check.k8s:v1",
		"retry.task:v1",
		"task.failed:v1",
		"node.updated:v1",
		"trigger.rerun:v1",
		"trigger.single_node_run:v1",
		"initialize.run:v1",
		"run.initialized:v1",
		"manifest.loaded:v1",
		"rerun.ready:v1",
		"schedules.loaded:v1",
		"update.graph:v1",
		"schedule.cancelled:v1",
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
		"-l", "app=dbt-job",
		"--ignore-not-found=true")
	if err := cmd.Run(); err != nil {
		t.Logf("Warning: Failed to cleanup k8s jobs: %v", err)
	}
}
