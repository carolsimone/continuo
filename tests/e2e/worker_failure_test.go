//go:build e2e_worker

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

	// The idle/cold-start subtest runs first, on an executor that no sibling
	// subtest has restarted: the mode-switch subtest below toggles the shared
	// executor's canary env, which rolls the Deployment, so running it first would
	// leave the pool reconciler churning under the idle subtest's long waits.
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

		// Re-establish the canary immediately before the cold-start so a stray
		// executor rollout earlier in a long suite cannot have left service-3 on
		// the Jobs path: setExecutorOverrides is a no-op when the env already holds
		// this value and re-applies it (with a rollout) if it does not, so the
		// cold-start task is guaranteed to route to the pool rather than silently
		// becoming a Job.
		setExecutorOverrides(t, ctx, workerCanaryOverridesJSON)

		// A new task cold-starts the pool again: it scales up, a pod hydrates and
		// turns Ready, and the task runs. Confirm the task actually took the worker
		// path before waiting on a worker pod, so a mis-route fails with that fact
		// rather than an opaque pod-never-Ready timeout.
		cold := triggerWorkerNode(t, ctx, clients, "service-3", "e2e_schema", "worker_perf")
		var coldMode string
		pollUntil(t, ctx, 1*time.Minute, 2*time.Second, func() (bool, error) {
			coldMode = queryRunExecutionMode(t, ctx, clients, cold)
			return coldMode != "", nil
		}, "cold-start task never produced a deployment row")
		require.Equal(t, "workers", coldMode, "the cold-start task must route to the worker pool")

		// The proof the retired pool cold-started is that this worker task ran to
		// success: a task only settles succeeded on the worker path if the pool
		// scaled a pod up from zero, the pod hydrated its artifact, claimed the
		// lease, and ran dbt. Asserting the task outcome is both stronger and
		// race-free — polling for a live Ready pod would miss the window, since
		// worker_perf finishes in a second or two and the revived pool can begin
		// idling back down before a snapshot catches its pod.
		verifySchedulerSucceeded(t, ctx, clients, cold)
		crow := findDeploymentRow(t, queryDeploymentsByService(t, ctx, clients, "service-3"), "worker_perf")
		assert.Equal(t, "workers", crow.ExecutionMode, "the cold-started task must run on the worker path")
		assert.Equal(t, "succeeded", crow.Status)
	})

	t.Run("mode_switch_to_jobs_is_sufficient_rollback", func(t *testing.T) {
		// With the canary on, the node runs on a worker.
		onWorkers := triggerWorkerNode(t, ctx, clients, "service-3", "e2e_schema", "worker_perf")
		verifySchedulerSucceeded(t, ctx, clients, onWorkers)
		wrow := findDeploymentRow(t, queryDeploymentsByService(t, ctx, clients, "service-3"), "worker_perf")
		require.Equal(t, "workers", wrow.ExecutionMode, "with the canary on the node must run on a worker")

		// Rolling the override back to jobs is the whole rollback: the very next
		// run of the same node takes the Jobs path, with no redeploy or artifact
		// change. The executor rolls when its env changes, so re-establish health
		// before triggering the Jobs-path run.
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
}
