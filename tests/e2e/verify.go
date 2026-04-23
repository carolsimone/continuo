package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// verifyExecutorDeployedJobs checks that executor deployed k8s jobs
func verifyExecutorDeployedJobs(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	expectedTables []string,
	scheduleName string,
) {
	// Use app=dbt-job rather than schedule=<name> because seed nodes
	// carry schedule_name="seed" in Neo4j regardless of the parent schedule,
	// so their K8s jobs get label schedule=seed.  Filtering by the executor
	// app label and then checking table_name labels avoids this mismatch.
	pollUntil(t, ctx, 2*time.Minute, 2*time.Second, func() (bool, error) {
		jobList, err := getK8sJobs(ctx, "app=dbt-job")
		if err != nil {
			return false, err
		}

		if len(jobList.Items) < len(expectedTables) {
			return false, nil
		}

		// Extract table names from labels
		deployedTables := []string{}
		for _, job := range jobList.Items {
			if tableName, ok := job.Metadata.Labels["table_name"]; ok {
				deployedTables = append(deployedTables, tableName)
			}
		}

		return containsAll(deployedTables, expectedTables), nil
	}, fmt.Sprintf("Timeout waiting for executor to deploy %d jobs", len(expectedTables)))

	t.Logf("✅ executor-controller deployed %d jobs", len(expectedTables))
}

// verifyJobsCompleted checks that all tasks have reached 'succeeded' status in
// the state DB. Using the state DB (rather than K8s job status) is more reliable:
// K8s jobs may be short-lived or cleaned up before the poll catches them.
func verifyJobsCompleted(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	tables []string,
	scheduleName string,
) {
	pollUntil(t, ctx, 8*time.Minute, 2*time.Second, func() (bool, error) {
		for _, table := range tables {
			var status string
			err := clients.stateDB.QueryRowContext(ctx, `
				SELECT tt.status
				FROM task_tracker tt
				JOIN scheduler_tracker st ON tt.schedule_id = st.schedule_id
				WHERE st.schedule_name = $1 AND tt.table_name = $2
			`, scheduleName, table).Scan(&status)
			if err != nil {
				return false, nil // row not yet present — retry
			}
			if status != "succeeded" {
				return false, nil
			}
		}
		return true, nil
	}, fmt.Sprintf("Timeout waiting for %d jobs to reach 'succeeded' status", len(tables)))

	t.Logf("✅ All %d jobs completed successfully", len(tables))
}

// verifyJobLogs checks that job logs contain expected parameters
func verifyJobLogs(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	tableName string,
	scheduleName string,
) {
	logs := getK8sJobLogs(ctx, t, tableName)

	assert.Contains(t, logs, fmt.Sprintf("schedule_name=%s", scheduleName))
	assert.Contains(t, logs, fmt.Sprintf("table_name=%s", tableName))
	assert.Contains(t, logs, fmt.Sprintf("schema_name=%s", testSchemaName))
	assert.Contains(t, logs, "job_name=")
	assert.Contains(t, logs, fmt.Sprintf("service_name=%s", getServiceNameForTable(tableName)))
}

// verifyDependencyControllerUnlockedNextLevel checks that next level nodes were published
func verifyDependencyControllerUnlockedNextLevel(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	nextLevelTables []string,
	scheduleID uuid.UUID,
) {
	// Wait for query.model:v1 stream to contain messages for next level
	pollUntil(t, ctx, 60*time.Second, 1*time.Second, func() (bool, error) {
		messages, err := clients.redisClient.XRange(ctx, "query.model:v1", "-", "+").Result()
		if err != nil {
			return false, err
		}

		// Count messages matching next level tables for this specific schedule
		matchCount := 0
		for _, msg := range messages {
			if msg.Values["schedule_id"] != scheduleID.String() {
				continue
			}
			if tableName, ok := msg.Values["table_name"].(string); ok {
				for _, expectedTable := range nextLevelTables {
					if tableName == expectedTable {
						matchCount++
						break
					}
				}
			}
		}

		return matchCount >= len(nextLevelTables), nil
	}, fmt.Sprintf("Timeout waiting for orchestrator to unlock %d nodes", len(nextLevelTables)))

	t.Logf("✅ orchestrator unlocked %d nodes", len(nextLevelTables))
}

// verifyFullDAGExecution verifies all 4 levels execute in order
func verifyFullDAGExecution(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	scheduleID uuid.UUID,
) {
	levels := getDAGLevels()

	for i, level := range levels {
		t.Logf("Verifying level %d: %v", i, level)

		// Wait for executor to deploy these jobs
		verifyExecutorDeployedJobs(t, ctx, clients, level, testScheduleName)

		// Wait for jobs to complete
		verifyJobsCompleted(t, ctx, clients, level, testScheduleName)

		// If not last level, verify orchestrator published next level
		if i < len(levels)-1 {
			verifyDependencyControllerUnlockedNextLevel(t, ctx, clients, levels[i+1], scheduleID)
		}
	}

	t.Log("✅ Full DAG execution completed successfully")
}

// verifyOrchestratorPublishedRootNodes verifies that the orchestrator published
// the expected root node messages to query.model:v1 after processing scheduler.started:v1.
func verifyOrchestratorPublishedRootNodes(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	schedulerID uuid.UUID,
	expectedRootNodes []string,
) {
	t.Helper()
	expectedCount := len(expectedRootNodes)

	// Wait for query.model:v1 to contain the expected messages
	pollUntil(t, ctx, 60*time.Second, 500*time.Millisecond, func() (bool, error) {
		messages, err := clients.redisClient.XRange(ctx, "query.model:v1", "-", "+").Result()
		if err != nil {
			return false, err
		}
		count := 0
		for _, msg := range messages {
			if msg.Values["schedule_id"] == schedulerID.String() {
				count++
			}
		}
		return count >= expectedCount, nil
	}, fmt.Sprintf("Timeout waiting for %d messages in query.model:v1", expectedCount))

	// Assert Redis message content and no duplicates
	messages, err := clients.redisClient.XRange(ctx, "query.model:v1", "-", "+").Result()
	require.NoError(t, err)

	var scheduleMessages []goredis.XMessage
	for _, msg := range messages {
		if msg.Values["schedule_id"] == schedulerID.String() {
			scheduleMessages = append(scheduleMessages, msg)
		}
	}
	require.Len(t, scheduleMessages, expectedCount, "Expected exactly %d messages for this schedule in query.model:v1", expectedCount)

	seenStreamTables := make(map[string]bool)
	for _, msg := range scheduleMessages {
		tableName, _ := msg.Values["table_name"].(string)
		assert.NotEmpty(t, msg.Values["task_id"], "task_id must be present")
		assert.NotEmpty(t, msg.Values["job_name"], "job_name must be present")
		assert.NotEmpty(t, msg.Values["outbox_entry_id"], "outbox_entry_id must be present")
		assert.Contains(t, expectedRootNodes, tableName, "stream table_name must be a root node")
		assert.False(t, seenStreamTables[tableName], "Duplicate Redis message for table: %s", tableName)
		seenStreamTables[tableName] = true
	}

	t.Logf("✅ orchestrator published %d root node messages to query.model:v1", expectedCount)
}

// verifyTableEExhaustedRetries polls state DB until table_e has retry_count = 2
// and status = 'failed', confirming all 2 retries were exhausted (3 total attempts).
func verifyTableEExhaustedRetries(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	schedulerID uuid.UUID,
) {
	t.Helper()
	pollUntil(t, ctx, 10*time.Minute, 5*time.Second, func() (bool, error) {
		var retryCount int
		var status string
		err := clients.stateDB.QueryRow(`
			SELECT retry_count, status
			FROM task_tracker
			WHERE schedule_id = $1 AND table_name = 'table_e'
		`, schedulerID).Scan(&retryCount, &status)
		if err != nil {
			return false, err
		}
		return retryCount == 2 && status == "failed", nil
	}, "Timeout waiting for table_e to exhaust 2 retries and reach failed status")

	t.Log("✅ table_e exhausted 2 retries and is permanently failed (3 total attempts)")
}

// verifyNoJobsDeployed asserts that none of the given tables have K8s jobs.
// Called after table_e fails to confirm downstream nodes were never deployed.
func verifyNoJobsDeployed(
	t *testing.T,
	ctx context.Context,
	tables []string,
) {
	t.Helper()
	for _, table := range tables {
		jobList, err := getK8sJobs(ctx, fmt.Sprintf("table_name=%s", table))
		require.NoError(t, err, "Failed to query K8s jobs for table: %s", table)
		assert.Empty(t, jobList.Items, "Expected no K8s jobs for table %s but found %d", table, len(jobList.Items))
	}
	t.Logf("✅ Confirmed no K8s jobs deployed for: %v", tables)
}

// verifySchedulerSucceeded polls scheduler_tracker until the schedule reaches
// 'succeeded' status, confirming orchestrator finalised the run correctly.
func verifySchedulerSucceeded(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	schedulerID uuid.UUID,
) {
	t.Helper()
	pollUntil(t, ctx, 2*time.Minute, 3*time.Second, func() (bool, error) {
		var status string
		err := clients.stateDB.Get(&status, `
			SELECT status FROM scheduler_tracker WHERE schedule_id = $1
		`, schedulerID)
		if err != nil {
			return false, err
		}
		return status == "succeeded", nil
	}, "Timeout waiting for scheduler to reach 'succeeded' status")

	t.Log("✅ Scheduler reached 'succeeded' status")
}

// verifySchedulerFailed polls scheduler_tracker until the schedule reaches
// 'failed' status, confirming orchestrator finalised the run correctly.
func verifySchedulerFailed(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	schedulerID uuid.UUID,
) {
	t.Helper()
	pollUntil(t, ctx, 5*time.Minute, 3*time.Second, func() (bool, error) {
		var status string
		err := clients.stateDB.Get(&status, `
			SELECT status FROM scheduler_tracker WHERE schedule_id = $1
		`, schedulerID)
		if err != nil {
			return false, err
		}
		return status == "failed", nil
	}, "Timeout waiting for scheduler to reach 'failed' status")

	t.Log("✅ Scheduler reached 'failed' status")
}
