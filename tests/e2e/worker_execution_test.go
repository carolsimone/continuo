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
	releaseID := "e2e-worker-" + uuid.NewString()[:8]
	t.Logf("release_id=%s changed_service=%s changed_node=%s", releaseID, changedService, workerPerfUniqueID)

	// Turn the canary on for the length of this test. service-3 is native
	// (plain dbt), so its promoted nodes take the native worker path.
	enableWorkerCanary(t, ctx)

	// Drive a real release whose only changed node is worker_perf, so its
	// compile job runs the runtime exporter and its promotion carries the
	// service's runtime manifest reference — the reference the worker pool is
	// keyed on. Every other service-3 node is carried unchanged.
	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices,
		"no baseline manifests under s3://%s/<service>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)
	changedImageTag := allServices[changedService].imageTag
	require.NotEmpty(t, changedImageTag, "image_tag missing for %s", changedService)

	var prodNodes []map[string]string
	perfFound := false
	for _, si := range allServices {
		for _, n := range si.nodes {
			if n.uniqueID == workerPerfUniqueID {
				perfFound = true
				continue // exclude → worker_perf is the sole changed node
			}
			prodNodes = append(prodNodes, map[string]string{
				"unique_id":    n.uniqueID,
				"content_hash": n.contentHash,
			})
		}
	}
	require.True(t, perfFound,
		"worker_perf not found in any manifest — is dbt/services/service-3/models/worker_perf.sql present and the image rebuilt?")

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProdExcept(t, ctx, clients, allServices, changedService)

	postRelease(t, clients, changedService, releaseID, changedImageTag, false)
	assertValidationRequestedNodes(t, ctx, clients, releaseID, []string{workerPerfUniqueID})
	waitForReleasePromoted(t, ctx, clients, releaseID, 12*time.Minute)
	waitForTopologySwap(t, ctx, clients, releaseID, workerPerfUniqueID, 2*time.Minute)

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
	require.Equal(t, releaseID, descriptor.ReleaseID)

	// Promotion alone must create no worker Deployment: a pool exists for demand,
	// not for a promoted topology nobody is running yet.
	require.Equal(t, 0, countWorkerK8sDeployments(t, ctx),
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

	require.GreaterOrEqual(t, countWorkerK8sDeployments(t, ctx), 1,
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

	// No per-node Job is produced for a worker-mode task.
	jobs, err := getK8sJobs(ctx, "app=dbt-job,table_name=worker_perf")
	require.NoError(t, err, "list dbt-job Jobs for worker_perf")
	assert.Empty(t, jobs.Items, "a worker-mode task must not produce an app=dbt-job production Job")

	firstPod := *perfRow.LeasePodName

	// Sequential reuse: a second run of the same node is claimed by the same
	// warm pod, proving the process is long-lived rather than one-shot.
	runID2 := triggerWorkerNode(t, ctx, clients, changedService, "e2e_schema", "worker_perf")
	verifySchedulerSucceeded(t, ctx, clients, runID2)
	rows2 := queryDeploymentsByService(t, ctx, clients, changedService)
	pods := distinctLeasePods(rows2)
	assert.Contains(t, pods, firstPod, "the reused pod must still be the one that ran")
	assert.Len(t, pods, 1, "sequential service-3 runs must reuse a single worker pod, not spawn new ones")

	t.Log("native worker execution verified: pool ready, parse-context matched, node ran on a reused pod with no Job")
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
