package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_WorkerFailures covers the pool-lifecycle behaviours that are only
// observable end to end: a service rolled back to Jobs stops using its pool, and
// an idle pool retires to zero replicas and cold-starts again on the next task.
//
// The fencing, late-completion (410), retry-increments-attempt, corrupt-artifact,
// parse-context-mismatch, and required-wrapper-cache-rejection behaviours are
// deterministic unit/integration concerns and are proven in executor-controller
// (service/lease, service/routing) and dbt (tests/) rather than raced here.
func TestE2E_WorkerFailures(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	enableWorkerCanary(t, ctx)
	ensureService3WorkerReleased(t, ctx, clients)

	t.Run("mode_switch_to_jobs_is_sufficient_rollback", func(t *testing.T) {
		// With the canary on, the node runs on a worker.
		onWorkers := triggerWorkerNode(t, ctx, clients, "service-3", "e2e_schema", "worker_perf")
		verifySchedulerSucceeded(t, ctx, clients, onWorkers)
		wrow := findDeploymentRow(t, queryDeploymentsByService(t, ctx, clients, "service-3"), "worker_perf")
		require.Equal(t, "workers", wrow.ExecutionMode, "with the canary on the node must run on a worker")

		// Rolling the override back to jobs is the whole rollback: the very next
		// run of the same node takes the Jobs path, with no redeploy or artifact
		// change.
		setExecutorOverrides(t, ctx, "{}")
		t.Cleanup(func() {
			cctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			setExecutorOverrides(t, cctx, workerCanaryOverridesJSON)
		})

		onJobs := triggerWorkerNode(t, ctx, clients, "service-3", "e2e_schema", "worker_perf")
		verifySchedulerSucceeded(t, ctx, clients, onJobs)
		jrow := findDeploymentRow(t, queryDeploymentsByService(t, ctx, clients, "service-3"), "worker_perf")
		assert.Equal(t, "jobs", jrow.ExecutionMode,
			"after the override is cleared the node must run as a Job — mode switch alone is the rollback")
	})

	t.Run("idle_pool_scales_to_zero_then_cold_starts", func(t *testing.T) {
		// A run brings the pool up.
		warm := triggerWorkerNode(t, ctx, clients, "service-3", "e2e_schema", "worker_perf")
		verifySchedulerSucceeded(t, ctx, clients, warm)
		pool, ok := queryWorkerPoolByService(t, ctx, clients, "service-3")
		require.True(t, ok, "a pool must be registered after a worker run")
		require.Positive(t, pool.DesiredReplicas, "an active pool must want at least one worker")

		// With no further work and the idle timeout elapsed, the reconciler
		// retires the pool to zero replicas. WORKER_IDLE_TIMEOUT is 300s, so allow
		// generously beyond it.
		pollUntil(t, ctx, 8*time.Minute, 5*time.Second, func() (bool, error) {
			p, ok := queryWorkerPoolByService(t, ctx, clients, "service-3")
			return ok && p.DesiredReplicas == 0, nil
		}, "idle service-3 pool never scaled to zero replicas")

		// A new task cold-starts the pool again: it scales up, a pod hydrates and
		// turns Ready, and the task runs.
		cold := triggerWorkerNode(t, ctx, clients, "service-3", "e2e_schema", "worker_perf")
		pollUntil(t, ctx, 5*time.Minute, 2*time.Second, func() (bool, error) {
			_, ready := countWorkerPods(t, ctx, clients, "service-3")
			return ready >= 1, nil
		}, "idle-retired pool never cold-started a Ready worker for the next task")
		verifySchedulerSucceeded(t, ctx, clients, cold)
		crow := findDeploymentRow(t, queryDeploymentsByService(t, ctx, clients, "service-3"), "worker_perf")
		assert.Equal(t, "workers", crow.ExecutionMode, "the cold-started task must run on the worker path")
		assert.Equal(t, "succeeded", crow.Status)
	})
}
