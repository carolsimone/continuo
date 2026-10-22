package e2e

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
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

	t.Logf("✅ execution-controller deployed %d jobs", len(expectedTables))
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

// verifyNextLevelDispatched checks that the next level nodes were dispatched on task.dispatch:v1
func verifyNextLevelDispatched(
	t *testing.T,
	ctx context.Context,
	dispatchTap *streamTap,
	nextLevelTables []string,
	scheduleID uuid.UUID,
) {
	// Wait for task.dispatch:v1 to contain the next level's dispatches. The tap
	// saw every entry since before the run was triggered, so a trim of consumed
	// entries cannot hide one.
	pollUntil(t, ctx, 60*time.Second, 1*time.Second, func() (bool, error) {
		matchCount := 0
		for _, d := range dispatchTap.Dispatches(t) {
			if d.RunID == scheduleID.String() && slices.Contains(nextLevelTables, d.TableName) {
				matchCount++
			}
		}
		return matchCount >= len(nextLevelTables), nil
	}, fmt.Sprintf("Timeout waiting for state to dispatch %d nodes", len(nextLevelTables)))

	t.Logf("✅ state dispatched %d nodes", len(nextLevelTables))
}

// getDAGLevels returns happy-path DAG nodes grouped by execution level.
func getDAGLevels() [][]string {
	return [][]string{
		{"seed_table_1", "seed_table_2", "seed_table_3"},
		{"table_a", "table_b", "table_c"},
		{"table_d", "table_e", "table_f"},
		{"table_g", "table_h"},
		{"table_i", "table_j"},
	}
}

// verifyFullDAGExecution verifies all 4 levels execute in order
func verifyFullDAGExecution(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	dispatchTap *streamTap,
	scheduleID uuid.UUID,
) {
	levels := getDAGLevels()

	for i, level := range levels {
		t.Logf("Verifying level %d: %v", i, level)

		// Wait for executor to deploy these jobs
		verifyExecutorDeployedJobs(t, ctx, clients, level, testScheduleName)

		// Wait for jobs to complete
		verifyJobsCompleted(t, ctx, clients, level, testScheduleName)

		// If not last level, verify state dispatched the next level
		if i < len(levels)-1 {
			verifyNextLevelDispatched(t, ctx, dispatchTap, levels[i+1], scheduleID)
		}
	}

	t.Log("✅ Full DAG execution completed successfully")
}

// verifyDialectRouting asserts that production dbt Jobs are actually routed
// through the per-service command dialect configured via the dbt-commands
// ConfigMap, rather than silently falling back to the built-in dialect. The
// e2e cluster maps service-1 to the "customname-dbt" alias (tests/e2e/k8s/
// execution-controller-deployment.yaml) while service-2/3 have no override and
// use built-in plain dbt. Without this assertion, a broken env var, volume
// mount, or ConfigMap wiring would silently fall back to the built-in dialect
// for every service and the suite would stay green.
func verifyDialectRouting(t *testing.T, ctx context.Context) {
	t.Helper()

	// "!mode" excludes seed_build/compile/validation Jobs (which carry a mode
	// label) so only plain production query/seed Jobs — the ones this test's
	// DAG actually deploys — are inspected.
	service1Jobs, err := getK8sJobs(ctx, "app=dbt-job,service_name=service-1,!mode")
	require.NoError(t, err, "Failed to query K8s jobs for service-1")
	require.NotEmpty(t, service1Jobs.Items, "Expected at least one service-1 job to assert dialect routing on")
	for _, job := range service1Jobs.Items {
		require.NotEmpty(t, job.Spec.Template.Spec.Containers, "service-1 job %s has no containers", job.Metadata.Name)
		command := job.Spec.Template.Spec.Containers[0].Command
		require.NotEmpty(t, command, "service-1 job %s has an empty container command", job.Metadata.Name)
		assert.Equal(t, "customname-dbt", command[0],
			"service-1 jobs must run through the customname-dbt dialect (dbt-commands ConfigMap wiring) — job %s ran %v instead",
			job.Metadata.Name, command)
	}

	builtinJobs, err := getK8sJobs(ctx, "app=dbt-job,service_name in (service-2,service-3),!mode")
	require.NoError(t, err, "Failed to query K8s jobs for service-2/3")
	require.NotEmpty(t, builtinJobs.Items, "Expected at least one service-2/3 job to assert built-in dialect fallback on")
	for _, job := range builtinJobs.Items {
		require.NotEmpty(t, job.Spec.Template.Spec.Containers, "job %s has no containers", job.Metadata.Name)
		command := job.Spec.Template.Spec.Containers[0].Command
		require.NotEmpty(t, command, "job %s has an empty container command", job.Metadata.Name)
		assert.Equal(t, "dbt", command[0],
			"service-2/3 jobs must run through the built-in plain dbt dialect — job %s ran %v instead",
			job.Metadata.Name, command)
	}

	t.Log("✅ dialect routing verified: service-1 jobs run customname-dbt, service-2/3 jobs run built-in dbt")
}

// verifyRootNodesDispatched verifies that state dispatched attempt 0 of each
// expected root node to task.dispatch:v1 when it created the run, once.
func verifyRootNodesDispatched(
	t *testing.T,
	ctx context.Context,
	dispatchTap *streamTap,
	schedulerID uuid.UUID,
	expectedRootNodes []string,
) {
	t.Helper()
	expectedCount := len(expectedRootNodes)
	ofRun := func() []tappedDispatch {
		var out []tappedDispatch
		for _, d := range dispatchTap.Dispatches(t) {
			if d.RunID == schedulerID.String() {
				out = append(out, d)
			}
		}
		return out
	}

	// Wait for task.dispatch:v1 to contain the expected dispatches. The tap saw
	// every entry since before the run was triggered, so a trim of consumed
	// entries cannot hide one.
	pollUntil(t, ctx, 60*time.Second, 500*time.Millisecond, func() (bool, error) {
		return len(ofRun()) >= expectedCount, nil
	}, fmt.Sprintf("Timeout waiting for %d dispatches in task.dispatch:v1", expectedCount))

	dispatches := ofRun()
	require.Len(t, dispatches, expectedCount, "Expected exactly %d dispatches of this run in task.dispatch:v1", expectedCount)
	seenStreamTables := make(map[string]bool)
	for _, d := range dispatches {
		assert.NotEmpty(t, d.TaskID, "task_id must be present")
		assert.NotEmpty(t, d.JobName, "job_name must be present")
		assert.NotEmpty(t, d.OutboxEntryID, "outbox_entry_id must be present")
		assert.Equal(t, int32(0), d.Attempt, "a root node's first dispatch is its attempt 0")
		assert.Equal(t, pkgevents.AttemptDispatched, d.KnownStatus)
		assert.Positive(t, d.TimeoutSeconds, "every dispatch carries its task timeout")
		assert.Contains(t, expectedRootNodes, d.TableName, "stream table_name must be a root node")
		assert.False(t, seenStreamTables[d.TableName], "Duplicate dispatch for table: %s", d.TableName)
		seenStreamTables[d.TableName] = true
	}

	t.Logf("✅ state dispatched %d root nodes to task.dispatch:v1", expectedCount)
}

// verifyTasksSkipped polls state DB until every task of schedulerID at one of
// tables is 'skipped': state skipped it when an upstream failed for good.
func verifyTasksSkipped(t *testing.T, ctx context.Context, clients *testClients, schedulerID uuid.UUID, tables []string) {
	t.Helper()
	pollUntil(t, ctx, 2*time.Minute, time.Second, func() (bool, error) {
		var skipped int
		err := clients.stateDB.GetContext(ctx, &skipped, `
			SELECT count(*) FROM task_tracker
			WHERE schedule_id = $1 AND table_name = ANY($2) AND status = 'skipped'`,
			schedulerID, pq.Array(tables))
		if err != nil {
			return false, err
		}
		return skipped == len(tables), nil
	}, fmt.Sprintf("Timeout waiting for %v to be skipped", tables))
	t.Logf("✅ %v skipped", tables)
}

// verifyNodeExhaustedRetries waits until tableName's task of schedulerID has
// failed for good: every attempt its budget allowed (max_retries + 1) failed
// with a failure of class task, each the next attempt state dispatched, and
// the task ended failed at its last attempt.
func verifyNodeExhaustedRetries(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	schedulerID uuid.UUID,
	tableName string,
) {
	t.Helper()
	lastSeen := ""
	var maxRetries int
	pollUntil(t, ctx, 10*time.Minute, 5*time.Second, func() (bool, error) {
		var taskID uuid.UUID
		var attempt int
		var status string
		err := clients.stateDB.QueryRowContext(ctx, `
			SELECT task_id, attempt, max_retries, status
			FROM task_tracker
			WHERE schedule_id = $1 AND table_name = $2
		`, schedulerID, tableName).Scan(&taskID, &attempt, &maxRetries, &status)
		if err != nil {
			return false, err
		}
		var rows []string
		if err := clients.stateDB.SelectContext(ctx, &rows, `
			SELECT attempt::text || ':' || status || ':' || coalesce(failure_class, '')
			FROM task_attempts
			WHERE task_id = $1
			ORDER BY attempt
		`, taskID); err != nil {
			return false, err
		}
		want := make([]string, 0, maxRetries+1)
		for n := 0; n <= maxRetries; n++ {
			want = append(want, fmt.Sprintf("%d:failed:task", n))
		}
		seen := fmt.Sprintf("task_tracker attempt=%d max_retries=%d status=%s; task_attempts [%s]",
			attempt, maxRetries, status, strings.Join(rows, ","))
		if seen != lastSeen {
			t.Logf("%s attempts: %s", tableName, seen)
			lastSeen = seen
		}
		return attempt == maxRetries && status == "failed" && strings.Join(rows, ",") == strings.Join(want, ","), nil
	}, fmt.Sprintf("Timeout waiting for %s to fail every attempt its budget allows with class task (see the last logged attempts line)", tableName))

	t.Logf("✅ %s failed all %d attempts its budget allows", tableName, maxRetries+1)
}

// taskOfRun returns the task_id of tableName's task in runID. state writes
// every task of a run in the transaction that creates the run, so the row is
// there once the trigger has returned.
func taskOfRun(t *testing.T, ctx context.Context, clients *testClients, runID uuid.UUID, tableName string) uuid.UUID {
	t.Helper()
	var taskID uuid.UUID
	require.NoError(t, clients.stateDB.GetContext(ctx, &taskID,
		`SELECT task_id FROM task_tracker WHERE schedule_id = $1 AND table_name = $2`, runID, tableName),
		"run %s holds no task at %s", runID, tableName)
	return taskID
}

// attemptRows lists taskID's task_attempts rows in attempt order, each as
// "attempt:status", followed by ":class:reason" for a failed attempt.
func attemptRows(ctx context.Context, clients *testClients, taskID uuid.UUID) ([]string, error) {
	rows := []string{}
	err := clients.stateDB.SelectContext(ctx, &rows, `
		SELECT attempt::text || ':' || status
		       || coalesce(':' || failure_class || ':' || failure_reason, '')
		  FROM task_attempts
		 WHERE task_id = $1
		 ORDER BY attempt`, taskID)
	return rows, err
}

// endedTask is a task that has ended, with its attempts as attemptRows lists
// them.
type endedTask struct {
	status     string
	attempt    int
	maxRetries int
	attempts   []string
}

// taskEnded reports whether taskID's task has ended: succeeded, failed,
// cancelled or skipped.
func taskEnded(ctx context.Context, clients *testClients, taskID uuid.UUID) (bool, error) {
	var status string
	if err := clients.stateDB.GetContext(ctx, &status,
		`SELECT status FROM task_tracker WHERE task_id = $1`, taskID); err != nil {
		return false, err
	}
	switch status {
	case "succeeded", "failed", "cancelled", "skipped":
		return true, nil
	}
	return false, nil
}

// waitForTaskEnded waits until taskID's task has ended and returns it. The task
// and its attempt rows are logged under label whenever they change, so a
// timeout shows what the poll last saw. state records a failed attempt and
// dispatches the next one in the same transaction, so the rows read once the
// task has ended are all the attempts it will ever have.
func waitForTaskEnded(t *testing.T, ctx context.Context, clients *testClients, taskID uuid.UUID, label string, timeout time.Duration) endedTask {
	t.Helper()
	var ended endedTask
	lastSeen := ""
	pollUntil(t, ctx, timeout, 2*time.Second, func() (bool, error) {
		var cur endedTask
		if err := clients.stateDB.QueryRowContext(ctx,
			`SELECT status, attempt, max_retries FROM task_tracker WHERE task_id = $1`, taskID).
			Scan(&cur.status, &cur.attempt, &cur.maxRetries); err != nil {
			return false, err
		}
		rows, err := attemptRows(ctx, clients, taskID)
		if err != nil {
			return false, err
		}
		cur.attempts = rows
		seen := fmt.Sprintf("task_tracker status=%s attempt=%d max_retries=%d; task_attempts [%s]",
			cur.status, cur.attempt, cur.maxRetries, strings.Join(rows, ","))
		if seen != lastSeen {
			t.Logf("%s: %s", label, seen)
			lastSeen = seen
		}
		switch cur.status {
		case "succeeded", "failed", "cancelled", "skipped":
			ended = cur
			return true, nil
		}
		return false, nil
	}, fmt.Sprintf("Timeout waiting for the task of %s to end (see the last logged line)", label))
	return ended
}

// attemptReportedAt returns when state recorded the last report of attempt n of
// taskID: task_attempts.last_report_at, on the database clock. For an attempt
// that has ended it is when state learned how it ended.
func attemptReportedAt(t *testing.T, ctx context.Context, clients *testClients, taskID uuid.UUID, n int) time.Time {
	t.Helper()
	var at time.Time
	require.NoError(t, clients.stateDB.GetContext(ctx, &at,
		`SELECT last_report_at FROM task_attempts WHERE task_id = $1 AND attempt = $2`, taskID, n),
		"attempt %d of task %s has no report time", n, taskID)
	return at
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
// 'succeeded' status, confirming state finalised the run correctly.
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
// 'failed' status.
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

// verifySchedulerSkipped polls scheduler_tracker until the schedule reaches
// 'skipped' status — the benign terminal for a Test run with no tests to run.
func verifySchedulerSkipped(
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
		return status == "skipped", nil
	}, "Timeout waiting for scheduler to reach 'skipped' status")

	t.Log("✅ Scheduler reached 'skipped' status")
}
