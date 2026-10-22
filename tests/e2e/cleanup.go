package e2e

import (
	"context"
	"os/exec"
	"testing"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/jmoiron/sqlx"
)

// cleanupTestData removes all test data from all systems
func cleanupTestData(t *testing.T, ctx context.Context, clients *testClients, scheduleName string) {
	t.Log("Cleaning up test data...")

	// Clean Redis streams before Postgres dedup tables: deleting message_processing
	// while a consumer still has pending messages re-enables those messages and
	// can trigger replays that recreate execution_outbox rows before the streams
	// are gone.
	cleanupRedis(t, ctx, clients)

	// Clean PostgreSQL databases
	cleanupPostgres(t, ctx, clients, scheduleName)

	// Clean Kubernetes jobs
	cleanupK8s(t, ctx)

	t.Log("✅ Cleanup complete")
}

// cleanupPostgres removes test data from all PostgreSQL databases
func cleanupPostgres(t *testing.T, ctx context.Context, clients *testClients, scheduleName string) {
	// Get scheduler ID first (needed for outbox cleanup)
	var schedulerID string
	_ = clients.stateDB.Get(&schedulerID, `
		SELECT schedule_id FROM scheduler_tracker WHERE schedule_name = $1
	`, scheduleName)

	// Clean the execution database. Order matters: execution_outbox carries a
	// FK to message_processing, so it goes first (deployments' FK clears itself
	// on delete). The harness owns these tables for the suite; leaving pending
	// outbox rows would make pkg/outbox.Processor retry them forever.
	_, _ = clients.executionDB.Exec("DELETE FROM execution_outbox")
	_, _ = clients.executionDB.Exec("DELETE FROM deployments")
	_, _ = clients.executionDB.Exec("DELETE FROM validation_aggregates")
	_, _ = clients.executionDB.Exec("DELETE FROM cancelled_schedules")
	for _, db := range []*sqlx.DB{clients.stateDB, clients.orchestratorDB, clients.executionDB} {
		_, _ = db.Exec("DELETE FROM message_processing")
	}

	// Clean the run's state_outbox rows (its dispatches and cancellation)
	if schedulerID != "" {
		_, _ = clients.stateDB.Exec("DELETE FROM state_outbox WHERE aggregate_id = $1", schedulerID)
	}

	// Clean state tables (do last to preserve scheduler_id)
	_, _ = clients.stateDB.Exec("DELETE FROM task_tracker WHERE schedule_id = $1", schedulerID)
	_, _ = clients.stateDB.Exec("DELETE FROM scheduler_tracker WHERE schedule_name = $1", scheduleName)
}

// cleanupRedis removes test streams from Redis
func cleanupRedis(t *testing.T, ctx context.Context, clients *testClients) {
	testStreams := []string{
		streams.TaskDispatchV1,
		streams.SchedulesLoadedV1,
		streams.ScheduleCancelledV1,
	}

	for _, stream := range testStreams {
		clients.redisClient.Del(ctx, stream)
	}

}

// cleanupK8s removes test jobs from Kubernetes
func cleanupK8s(t *testing.T, ctx context.Context) {
	// Use the schedule-id label to match what execution-controller actually creates
	cmd := exec.CommandContext(ctx, "kubectl", "delete", "jobs",
		"-n", "default",
		"-l", "app=dbt-job",
		"--ignore-not-found=true")
	if err := cmd.Run(); err != nil {
		t.Logf("Warning: Failed to cleanup k8s jobs: %v", err)
	}
}
