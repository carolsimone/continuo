package e2e

import (
	"context"
	"testing"
	"time"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// workerPerfUniqueID is the dedicated native performance/worker node in
// service-3. It is a self-contained leaf (`SELECT 1 AS id`), so a release that
// changes only it validates without touching any other node — which is what
// keeps this test off the intentionally-failing ftable_* nodes that a full
// service-3 release would reject.
const workerPerfUniqueID = "e2e_schema.worker_perf"

// TestE2E_WorkerExecution proves the native worker path end to end: a real
// release compiles and uploads the runtime artifact, the canary routes the
// promoted node to a reusable worker pool, and the pool's pod hydrates the
// pinned artifact and runs the node — with no per-node Kubernetes Job.
//
// The load-bearing assertion is the cross-pod parse-context equality: the
// parse_context_sha256 the compile pod wrote into the descriptor must equal the
// digest the worker pod recomputes independently, and the only observable proof
// the worker's recomputation matched is that its pod turns Ready (an unhydrated
// worker never passes its readiness probe). A mismatch is proven to fail closed
// by TestE2E_WorkerFailures; this proves the happy path matches.
func TestE2E_WorkerExecution(t *testing.T) {
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

	const changedService = "service-3"

	// Turn the canary on for the length of this test. service-3 is native
	// (plain dbt), so its promoted nodes take the native worker path.
	enableWorkerCanary(t, ctx)

	// Count any worker Deployments already present (a prior run may leave one
	// scaled to zero until it is idle-retired) so the assertions below can reason
	// in deltas: promotion must add none, executable demand must add one.
	beforeDeployments := countWorkerK8sDeployments(t, ctx)

	// Drive a fresh release: its compile job produces the runtime artifact and
	// its promotion carries the runtime reference the worker pool is keyed on.
	releaseID := releaseService3WorkerPerf(t, ctx, clients)

	// The compile job must have uploaded all three runtime objects beside the
	// manifest: the manifest itself, the partial parse a worker hydrates, and the
	// descriptor that binds them to this release.
	for _, name := range []string{"manifest.json", "partial_parse.msgpack", "runtime-manifest.json"} {
		key := changedService + "/" + releaseID + "/" + name
		require.True(t, s3ObjectExists(ctx, clients, key),
			"release compile must upload %s", key)
	}

	descriptor := getRuntimeDescriptor(t, ctx, clients, changedService, releaseID)
	require.Len(t, descriptor.ParseContextSHA256, 64, "descriptor parse_context_sha256 must be a 64-hex digest")
	require.Len(t, descriptor.SHA256, 64, "descriptor artifact sha256 must be a 64-hex digest")
	require.Equal(t, changedService, descriptor.ServiceName)

	// Promotion alone must create no worker Deployment: a pool exists for demand,
	// not for a promoted topology nobody is running yet.
	require.Equal(t, beforeDeployments, countWorkerK8sDeployments(t, ctx),
		"promotion alone must not create a worker Deployment before any executable demand")

	// Run the promoted node. It carries a dbt identity and a complete runtime
	// reference and its service is pinned to workers, so it must take the worker
	// path.
	runID := triggerWorkerNode(t, ctx, clients, changedService, "e2e_schema", "worker_perf")

	// A worker pool registers for the demand, its Deployment appears, and a pod
	// hydrates and turns Ready. Ready is the observable proof the worker's
	// recomputed parse context matched the descriptor's.
	var pool workerPoolRow
	pollUntil(t, ctx, 5*time.Minute, 2*time.Second, func() (bool, error) {
		p, ok := queryWorkerPoolByService(t, ctx, clients, changedService)
		if !ok {
			return false, nil
		}
		pool = p
		_, ready := countWorkerPods(t, ctx, clients, changedService)
		return ready >= 1, nil
	}, "no service-3 worker pod ever became Ready — a permanently unready pool means the worker's recomputed parse context did not match the descriptor (PARSE_CONTEXT_ENV_KEYS admitted a variable that differs between compile and worker pods)")

	require.Greater(t, countWorkerK8sDeployments(t, ctx), beforeDeployments,
		"executable demand must have created a worker Deployment")

	// Cross-pod parse-context EQUALITY (the single riskiest untested assumption).
	// The pool's stored digest is the promoted reference, which came from the
	// compile pod's descriptor; the pod being Ready means a worker recomputed the
	// same digest from the hydrated manifest and its own controller context and
	// accepted the artifact. Both facts together prove a legitimate compile→worker
	// pair matches.
	require.Nil(t, pool.InitializationError,
		"worker pool must have no initialization error: %v", pool.InitializationError)
	require.Equal(t, descriptor.ParseContextSHA256, pool.ParseContextSHA256,
		"the pool's parse_context must equal the compile pod's descriptor value")
	require.Equal(t, descriptor.SHA256, pool.RuntimeSHA256,
		"the pool's runtime manifest sha256 must equal the compile pod's descriptor value")

	// The run settles succeeded on the worker path.
	verifySchedulerSucceeded(t, ctx, clients, runID)

	rows := queryDeploymentsByService(t, ctx, clients, changedService)
	perfRow := findDeploymentRow(t, rows, "worker_perf")
	assert.Equal(t, "workers", perfRow.ExecutionMode, "worker_perf must run on the worker path")
	require.NotNil(t, perfRow.ExecutionPath)
	assert.Equal(t, "native", *perfRow.ExecutionPath, "plain dbt must resolve to the native path")
	require.NotNil(t, perfRow.PoolKey)
	assert.Equal(t, pool.PoolKey, *perfRow.PoolKey, "the task must be claimed from the registered pool")
	assert.Equal(t, "succeeded", perfRow.Status)
	require.NotNil(t, perfRow.LeasePodName)
	assert.NotEmpty(t, *perfRow.LeasePodName, "a claimed task records the pod that held its lease")

	// No per-node production Job is produced for a worker-mode task. The release's
	// own validation job for this node also carries app=dbt-job and
	// table_name=worker_perf, so it is excluded by mode!=validation: only a
	// production run Job (which a worker task must not create) would remain.
	jobs, err := getK8sJobs(ctx, "app=dbt-job,table_name=worker_perf,mode!=validation")
	require.NoError(t, err, "list production dbt-job Jobs for worker_perf")
	assert.Empty(t, jobs.Items, "a worker-mode task must not produce an app=dbt-job production Job")

	firstPod := queryRunLeasePod(t, ctx, clients, runID)
	require.NotEmpty(t, firstPod, "the first run must record its lease pod")

	// Sequential reuse: a second run of the same node is claimed by the same warm
	// pod, proving the process is long-lived rather than one-shot. Scope the
	// comparison to these two runs' own rows so historical runs cannot pollute it.
	runID2 := triggerWorkerNode(t, ctx, clients, changedService, "e2e_schema", "worker_perf")
	verifySchedulerSucceeded(t, ctx, clients, runID2)
	secondPod := queryRunLeasePod(t, ctx, clients, runID2)
	assert.Equal(t, firstPod, secondPod,
		"sequential service-3 runs must reuse a single warm worker pod, not spawn new ones")

	t.Log("native worker execution verified: pool ready, parse-context matched, node ran on a reused pod with no Job")
}

// relProbeUniqueID is a self-contained service-1 leaf (`SELECT 1 AS id`).
// service-1's dbt-commands route every verb through wise-dbt with
// worker.wrapper_cache: required, so running it on a worker exercises the
// wrapper_required path: the team wrapper runs as a child process and must show
// its dbt reused the promoted parse cache.
const relProbeUniqueID = "e2e_schema.rel_probe"

// TestE2E_WorkerWrapper runs the real wise-dbt wrapper on a worker to learn,
// empirically, what the forced DBT_LOG_FORMAT=json / DBT_LOG_LEVEL=debug do to
// it. Those variables are forced because dbt's structured log is the only
// channel that carries the I017/I040 cache-evidence codes a required-cache
// wrapper is held to. If they had no effect, the wrapper's dbt would emit no
// parse-cache codes and the required-cache task would fail
// runtime_manifest_unverified; the task succeeding is the empirical proof the
// forced log format delivered the evidence.
//
// In the e2e image wise-dbt is dbt (a symlink), which is exactly the wrapper
// case the fidelity delta was reasoned about: a wrapper whose child is dbt. The
// genuine third-party wise-dbt lives in the continuo-dbt-demo repo and is out of
// this repo's scope.
func TestE2E_WorkerWrapper(t *testing.T) {
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

	const service = "service-1"
	enableWorkerCanary(t, ctx)
	ensureServiceWorkerReleased(t, ctx, clients, service, "rel_probe", relProbeUniqueID)

	runID := triggerWorkerNode(t, ctx, clients, service, "e2e_schema", "rel_probe")

	// The pool must become Ready before the wrapper task can be claimed.
	pollUntil(t, ctx, 5*time.Minute, 2*time.Second, func() (bool, error) {
		_, ready := countWorkerPods(t, ctx, clients, service)
		return ready >= 1, nil
	}, "no service-1 worker pod became Ready — the wrapper pool never hydrated")

	verifySchedulerSucceeded(t, ctx, clients, runID)

	rows := queryDeploymentsByService(t, ctx, clients, service)
	row := findDeploymentRow(t, rows, "rel_probe")
	require.NotNil(t, row.ExecutionPath)
	require.Equal(t, "wrapper_required", *row.ExecutionPath,
		"service-1 declares wrapper_cache: required, so its worker tasks take the wrapper_required path")
	require.Equal(t, "workers", row.ExecutionMode)
	require.Equal(t, "succeeded", row.Status,
		"a required-cache wrapper task succeeds only if its dbt reported reusing the promoted parse cache")

	// The required-cache wrapper task succeeds only if its dbt reported reusing the
	// promoted parse cache, so cache_status is the worker's own verdict on the
	// forced-log-format evidence.
	term, ok := queryWorkerTerminalResult(t, ctx, clients, runID)
	require.True(t, ok, "the worker must report a terminal result")
	require.Equal(t, "accepted", term.CacheStatus,
		"a required-cache wrapper must observe the promoted parse cache being reused")

	// Read the uploaded task log and observe what the forced log vars produced:
	// well-formed JSON dbt events (DBT_LOG_FORMAT=json), including at least one
	// partial-parse cache-reuse code (I017 or I040) that DBT_LOG_LEVEL=debug
	// surfaces. This is the direct empirical answer to the fidelity-delta question.
	require.NotEmpty(t, term.LogS3URI, "the worker must upload a task log")
	logBody := getS3Object(t, ctx, clients, s3KeyFromURI(t, term.LogS3URI))
	jsonEvents, cacheCodes := scanDBTJSONLog(logBody)
	require.Positive(t, jsonEvents,
		"forced DBT_LOG_FORMAT=json must make the wrapper's dbt emit JSON events; found none in the task log")
	require.NotEmpty(t, cacheCodes,
		"forced json/debug logging must surface a partial-parse cache code (I017/I040); found none")
	t.Logf("wise-dbt wrapper under forced json/debug logging: %d JSON dbt events, cache codes=%v",
		jsonEvents, cacheCodes)
}

// triggerWorkerNode starts a single-node run against the current topology and
// registers cleanup, returning the run ID. The node must already be promoted
// with a runtime reference for it to take the worker path.
func triggerWorkerNode(t *testing.T, ctx context.Context, clients *testClients, service, schema, table string) uuid.UUID {
	t.Helper()
	resp, err := clients.stateClient.TriggerSingleNodeRun(ctx, &statev1.TriggerSingleNodeRunRequest{
		ServiceName:    service,
		SchemaName:     schema,
		TableName:      table,
		MetadataSource: "latest",
	})
	require.NoError(t, err, "TriggerSingleNodeRun %s.%s.%s", service, schema, table)
	require.NotEmpty(t, resp.RunId)
	runID, err := uuid.Parse(resp.RunId)
	require.NoError(t, err)
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()
		cleanupSingleNodeRun(t, cctx, clients, runID, resp.ScheduleName)
	})
	return runID
}

// findDeploymentRow returns the newest deployment row for a table, failing the
// test if none exists.
func findDeploymentRow(t *testing.T, rows []workerDeploymentRow, table string) workerDeploymentRow {
	t.Helper()
	for _, r := range rows {
		if r.TableName == table {
			return r
		}
	}
	require.FailNowf(t, "no deployment row", "no executor_deployments row for table %q", table)
	return workerDeploymentRow{}
}
