//go:build e2e_worker

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// The execution-mode override the worker canary installs on the executor
// deployment. service-1 is the wrapper path (its dbt-commands route through
// wise-dbt), service-3 is the native path (it inherits the plain-dbt default
// block). service-2 is deliberately absent so it stays on Jobs, which is the
// mixed-mode control every worker test relies on.
const workerCanaryOverridesJSON = `{"service-1":"workers","service-3":"workers"}`

// workerDeploymentName is the executor-controller Deployment the canary patches.
const workerDeploymentName = "executor-controller"

// workerPoolAppLabel is the label every resource of a worker pool carries. It
// matches executor-controller/adapters/k8s worker pool labelling.
const workerPoolAppLabel = "app=continuo-dbt-worker"

// poolKeyLabelBytes is how much of a pool key the pool-key label holds. It
// mirrors executor-controller/adapters/k8s poolKeyNameBytes: a label value is
// bounded, so a pool's pods are selected by the truncated key.
const poolKeyLabelBytes = 16

// poolKeyLabelValue is the pod-selector value for a pool: its key truncated to
// the label's byte budget.
func poolKeyLabelValue(poolKey string) string {
	if len(poolKey) > poolKeyLabelBytes {
		return poolKey[:poolKeyLabelBytes]
	}
	return poolKey
}

// kubectlJSON runs a kubectl command and unmarshals its JSON stdout into out.
func kubectlJSON(t *testing.T, ctx context.Context, out interface{}, args ...string) error {
	t.Helper()
	cmd := exec.CommandContext(ctx, "kubectl", args...) //nolint:gosec // args are fixed literals built by callers below, never external input
	body, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("kubectl %v: %w", args, err)
	}
	return json.Unmarshal(body, out)
}

// setExecutorOverrides patches the executor-controller Deployment's
// EXECUTION_MODE_OVERRIDES_JSON and waits for the rollout to complete, then
// re-establishes health. It is how the worker canary is turned on and off in
// worker-specific test setup rather than in the committed default manifest
// (which the deployment-config guard test pins to "{}").
func setExecutorOverrides(t *testing.T, ctx context.Context, overrides string) {
	t.Helper()
	set := exec.CommandContext(ctx, "kubectl", "set", "env", //nolint:gosec // deployment name and env are fixed literals
		"deployment/"+workerDeploymentName,
		"EXECUTION_MODE_OVERRIDES_JSON="+overrides,
		"-n", "default")
	out, err := set.CombinedOutput()
	require.NoError(t, err, "kubectl set env EXECUTION_MODE_OVERRIDES_JSON: %s", string(out))

	rollout := exec.CommandContext(ctx, "kubectl", "rollout", "status", //nolint:gosec // fixed literals
		"deployment/"+workerDeploymentName, "-n", "default", "--timeout=120s")
	rout, rerr := rollout.CombinedOutput()
	require.NoError(t, rerr, "executor rollout after override change: %s", string(rout))

	// A rollout replaces the pod the health port-forward attached to, so
	// re-establish it against the new pod before the test proceeds.
	portForwardHealthy(t, workerDeploymentName, 8084)
}

// enableWorkerCanary turns the canary on and registers a t.Cleanup that turns it
// back off, so a worker test leaves the shared executor in the jobs-default
// state the rest of the suite expects.
func enableWorkerCanary(t *testing.T, ctx context.Context) {
	t.Helper()
	setExecutorOverrides(t, ctx, workerCanaryOverridesJSON)
	t.Cleanup(func() {
		// Use a fresh context: the test's context may already be cancelled at
		// cleanup time, and leaving the canary on would leak worker routing into
		// later tests.
		cctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		setExecutorOverrides(t, cctx, "{}")
	})
}

// runtimeDescriptor is the runtime-manifest.json a release's compile job wrote
// beside its manifest.json in S3. parse_context_sha256 is the digest the compile
// pod computed; a worker recomputes it independently and refuses the artifact on
// a mismatch, so it is the value the cross-pod equality assertion compares.
type runtimeDescriptor struct {
	Format             string `json:"format"`
	ServiceName        string `json:"service_name"`
	ReleaseID          string `json:"release_id"`
	ImageTag           string `json:"image_tag"`
	ArtifactURI        string `json:"artifact_uri"`
	SHA256             string `json:"sha256"`
	DBTCoreVersion     string `json:"dbt_core_version"`
	AdapterType        string `json:"adapter_type"`
	ParseContextSHA256 string `json:"parse_context_sha256"`
}

// getRuntimeDescriptor fetches and parses the runtime-manifest.json descriptor a
// release compile uploaded for one service. The key is the sibling of the
// canonical manifest key: <service>/<release>/runtime-manifest.json.
func getRuntimeDescriptor(t *testing.T, ctx context.Context, clients *testClients, service, releaseID string) runtimeDescriptor {
	t.Helper()
	key := fmt.Sprintf("%s/%s/runtime-manifest.json", service, releaseID)
	body := getS3Object(t, ctx, clients, key)
	var d runtimeDescriptor
	require.NoError(t, json.Unmarshal(body, &d), "parse runtime descriptor %s", key)
	return d
}

// s3ObjectExists reports whether an object is present in the e2e bucket without
// failing the test when it is absent.
func s3ObjectExists(ctx context.Context, clients *testClients, key string) bool {
	_, err := clients.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(e2eS3Bucket),
		Key:    aws.String(key),
	})
	return err == nil
}

// workerPoolRow is the executor_worker_pools state a test asserts on. A pool is
// registered by the reconciler for the worker-routed work waiting on it; the
// row's parse-context digest is copied from the promoted runtime reference,
// which itself came from the compile pod's descriptor.
type workerPoolRow struct {
	PoolKey             string  `db:"pool_key"`
	ServiceName         string  `db:"service_name"`
	ImageTag            string  `db:"image_tag"`
	RuntimeURI          string  `db:"runtime_manifest_uri"`
	RuntimeSHA256       string  `db:"runtime_manifest_sha256"`
	ParseContextSHA256  string  `db:"runtime_manifest_parse_context_sha256"`
	DesiredReplicas     int     `db:"desired_replicas"`
	InitializationError *string `db:"initialization_error"`
}

// queryWorkerPoolByService returns the executor_worker_pools row for a service,
// or false when no pool is registered for it yet.
func queryWorkerPoolByService(t *testing.T, ctx context.Context, clients *testClients, service string) (workerPoolRow, bool) {
	t.Helper()
	var row workerPoolRow
	err := clients.executorDB.GetContext(ctx, &row,
		`SELECT pool_key, service_name, image_tag, runtime_manifest_uri,
		        runtime_manifest_sha256, runtime_manifest_parse_context_sha256,
		        desired_replicas, initialization_error
		   FROM executor_worker_pools
		  WHERE service_name = $1
		  ORDER BY created_at DESC
		  LIMIT 1`, service)
	if err != nil {
		return workerPoolRow{}, false
	}
	return row, true
}

// workerDeploymentRow is the executor_deployments state a worker task settles
// into. execution_mode distinguishes a pool-claimed task ("workers") from a
// Kubernetes Job ("jobs"); the lease_pod_* fields name the worker pod that held
// it, which is how the test proves two tasks reused one pod or ran in separate
// pods.
type workerDeploymentRow struct {
	Status        string  `db:"status"`
	ExecutionMode string  `db:"execution_mode"`
	ExecutionPath *string `db:"execution_path"`
	PoolKey       *string `db:"pool_key"`
	Attempt       int     `db:"attempt"`
	LeasePodName  *string `db:"lease_pod_name"`
	LeasePodUID   *string `db:"lease_pod_uid"`
	TableName     string  `db:"table_name"`
}

// queryDeploymentsByService returns every executor_deployments row for a
// service's production run work, newest first. Task identity lives in the
// job_params JSON, so service and table are read out of it. mode='production'
// selects the run dispatch and excludes the release's own validation/compile
// rows for the same service.
func queryDeploymentsByService(t *testing.T, ctx context.Context, clients *testClients, service string) []workerDeploymentRow {
	t.Helper()
	var rows []workerDeploymentRow
	err := clients.executorDB.SelectContext(ctx, &rows,
		`SELECT status, execution_mode, execution_path, pool_key, attempt,
		        lease_pod_name, lease_pod_uid,
		        COALESCE(job_params->>'table_name', '') AS table_name
		   FROM executor_deployments
		  WHERE job_params->>'service_name' = $1
		    AND mode = 'production'
		  ORDER BY created_at DESC`, service)
	require.NoError(t, err, "query executor_deployments for service %s", service)
	return rows
}

// countWorkerPods returns the number of pods a worker pool currently has and how
// many report Ready. A pool's pods only turn Ready once they have hydrated their
// pinned artifact — which they can only do when their recomputed parse context
// matches the descriptor — so readyPods rising above zero is the observable
// proof that the cross-pod parse-context check passed.
func countWorkerPods(t *testing.T, ctx context.Context, clients *testClients, service string) (total, ready int) {
	t.Helper()
	pool, ok := queryWorkerPoolByService(t, ctx, clients, service)
	if !ok {
		return 0, 0
	}
	var podList struct {
		Items []struct {
			Metadata struct {
				Labels map[string]string `json:"labels"`
			} `json:"metadata"`
			Status struct {
				Conditions []struct {
					Type   string `json:"type"`
					Status string `json:"status"`
				} `json:"conditions"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := kubectlJSON(t, ctx, &podList,
		"get", "pods", "-n", "default", "-l", workerPoolAppLabel+",pool-key="+poolKeyLabelValue(pool.PoolKey), "-o", "json"); err != nil {
		return 0, 0
	}
	for _, p := range podList.Items {
		total++
		for _, c := range p.Status.Conditions {
			if c.Type == "Ready" && c.Status == "True" {
				ready++
			}
		}
	}
	return total, ready
}

// countWorkerK8sDeployments returns the number of worker-pool Deployments in the
// cluster. Promotion alone must create none; only pending executable demand does.
func countWorkerK8sDeployments(t *testing.T, ctx context.Context) int {
	t.Helper()
	var depList struct {
		Items []json.RawMessage `json:"items"`
	}
	if err := kubectlJSON(t, ctx, &depList,
		"get", "deployments", "-n", "default", "-l", workerPoolAppLabel, "-o", "json"); err != nil {
		return 0
	}
	return len(depList.Items)
}

// waitForValidationRequestedContains polls until the validation.requested:v1
// message for releaseID names node in its ordered node set. It asserts the node
// this test cares about is validated, without over-coupling to the full
// change-detector derivation (which release_promote_test owns): a service
// release validates every node whose content hash differs from prod, which can
// legitimately include sibling nodes of the same or other services.
func waitForValidationRequestedContains(t *testing.T, ctx context.Context, clients *testClients, releaseID, node string) {
	t.Helper()
	var got []string
	pollUntil(t, ctx, 8*time.Minute, 1*time.Second, func() (bool, error) {
		msgs, err := clients.redisClient.XRange(ctx, streams.ValidationRequestedV1, "-", "+").Result()
		if err != nil {
			return false, nil
		}
		for _, msg := range msgs {
			payload, _ := msg.Values["payload"].(string)
			if payload == "" {
				continue
			}
			var p struct {
				ReleaseID      string   `json:"release_id"`
				NodeIDsInOrder []string `json:"node_ids_in_order"`
			}
			if json.Unmarshal([]byte(payload), &p) != nil || p.ReleaseID != releaseID {
				continue
			}
			got = p.NodeIDsInOrder
			for _, n := range got {
				if n == node {
					return true, nil
				}
			}
		}
		return false, nil
	}, fmt.Sprintf("validation.requested:v1 for release %s never named %s (got %v)", releaseID, node, got))
}

// ensureService3WorkerReleased makes service-3 worker-eligible and returns the
// descriptor the compile pod wrote for it. It is idempotent: if the current
// topology already carries a runtime reference for worker_perf (a prior worker
// test in the same run drove the release), it reuses that release rather than
// paying the validation cost again. Only when no reference is present — the
// first worker test, or after a seedTopology-based test overwrote the topology —
// does it drive a fresh release.
//
// The returned descriptor is read from S3, i.e. straight from the compile pod's
// own output, so a later parse-context equality check compares the pool against
// an independent source rather than against the same promotion the pool came
// from.
func ensureService3WorkerReleased(t *testing.T, ctx context.Context, clients *testClients) runtimeDescriptor {
	t.Helper()
	return ensureServiceWorkerReleased(t, ctx, clients, "service-3", "worker_perf", workerPerfUniqueID)
}

// releaseService3WorkerPerf drives a fresh service-3 release changing worker_perf.
func releaseService3WorkerPerf(t *testing.T, ctx context.Context, clients *testClients) string {
	t.Helper()
	return releaseServiceWithChangedLeaf(t, ctx, clients, "service-3", "worker_perf", workerPerfUniqueID)
}

// ensureServiceWorkerReleased makes a service worker-eligible via a leaf node and
// returns the descriptor the compile pod wrote. It reuses an existing release
// when the leaf already carries a runtime reference in the current topology, and
// drives a fresh one otherwise. See ensureService3WorkerReleased for the rationale.
func ensureServiceWorkerReleased(t *testing.T, ctx context.Context, clients *testClients, service, leafTable, leafUniqueID string) runtimeDescriptor {
	t.Helper()
	uri := neo4jScalarString(ctx, clients,
		`MATCH (t:Table {table_name:$name}) RETURN COALESCE(t.runtime_manifest_uri, '') AS v`,
		map[string]any{"name": leafTable})
	if uri != "" {
		releaseID := releaseIDFromArtifactURI(t, uri)
		t.Logf("%s already worker-eligible (release %s) — reusing", service, releaseID)
		return getRuntimeDescriptor(t, ctx, clients, service, releaseID)
	}
	releaseID := releaseServiceWithChangedLeaf(t, ctx, clients, service, leafTable, leafUniqueID)
	return getRuntimeDescriptor(t, ctx, clients, service, releaseID)
}

// releaseServiceWithChangedLeaf drives a real release whose changed set includes
// a self-contained leaf, so its compile job produces the runtime artifact and
// its promotion carries the runtime reference. Sibling nodes whose content hash
// differs from prod may also validate; the fixtures this is used with are all
// self-contained, so the release still promotes. Returns the release ID.
func releaseServiceWithChangedLeaf(t *testing.T, ctx context.Context, clients *testClients, service, leafTable, leafUniqueID string) string {
	t.Helper()
	releaseID := "e2e-worker-" + shortID()

	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices,
		"no baseline manifests under s3://%s/<service>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)
	imageTag := allServices[service].imageTag
	require.NotEmpty(t, imageTag, "image_tag missing for %s", service)

	var prodNodes []map[string]string
	leafFound := false
	for _, si := range allServices {
		for _, n := range si.nodes {
			if n.uniqueID == leafUniqueID {
				leafFound = true
				continue // exclude → the leaf is a changed node
			}
			prodNodes = append(prodNodes, map[string]string{
				"unique_id":    n.uniqueID,
				"content_hash": n.contentHash,
			})
		}
	}
	require.True(t, leafFound,
		"%s not found in any manifest — is the model present and the image rebuilt?", leafUniqueID)

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProdExcept(t, ctx, clients, allServices, service)

	postRelease(t, clients, service, releaseID, imageTag, false)
	waitForValidationRequestedContains(t, ctx, clients, releaseID, leafUniqueID)
	waitForReleasePromoted(t, ctx, clients, releaseID, 12*time.Minute)
	waitForTopologySwap(t, ctx, clients, releaseID, leafUniqueID, 2*time.Minute)
	return releaseID
}

// releaseIDFromArtifactURI parses the release segment out of a runtime artifact
// URI of the form s3://<bucket>/<service>/<release>/partial_parse.msgpack.
func releaseIDFromArtifactURI(t *testing.T, uri string) string {
	t.Helper()
	parts := strings.Split(strings.TrimPrefix(uri, "s3://"), "/")
	require.GreaterOrEqual(t, len(parts), 3, "unexpected artifact URI %q", uri)
	return parts[len(parts)-2]
}

// shortID returns a short random suffix for a release ID.
func shortID() string { return uuid.NewString()[:8] }

// workerTerminalResult is the JSON a worker reported on Complete, stored in
// executor_deployments.terminal_result. The uploaded log URI and the parse-cache
// verdict live here rather than in dedicated columns.
type workerTerminalResult struct {
	Succeeded   bool   `json:"succeeded"`
	LogS3URI    string `json:"log_s3_uri"`
	CacheStatus string `json:"cache_status"`
}

// queryWorkerTerminalResult returns the terminal result a run's worker reported.
func queryWorkerTerminalResult(t *testing.T, ctx context.Context, clients *testClients, runID uuid.UUID) (workerTerminalResult, bool) {
	t.Helper()
	var raw []byte
	err := clients.executorDB.GetContext(ctx, &raw,
		`SELECT terminal_result FROM executor_deployments
		  WHERE schedule_id = $1 AND mode = 'production'
		  ORDER BY created_at DESC LIMIT 1`, runID)
	if err != nil || len(raw) == 0 {
		return workerTerminalResult{}, false
	}
	var res workerTerminalResult
	if json.Unmarshal(raw, &res) != nil {
		return workerTerminalResult{}, false
	}
	return res, true
}

// s3KeyFromURI strips the s3://<bucket>/ prefix from a URI, returning the key.
func s3KeyFromURI(t *testing.T, uri string) string {
	t.Helper()
	rest := strings.TrimPrefix(uri, "s3://")
	i := strings.IndexByte(rest, '/')
	require.Positive(t, i, "malformed s3 URI %q", uri)
	return rest[i+1:]
}

// scanDBTJSONLog counts well-formed JSON dbt event lines in a task log and
// collects the partial-parse cache-evidence codes among them (I017/I040 accepted,
// I016/I024/I028 rejected). A wrapper writes its child's stdout verbatim, so with
// DBT_LOG_FORMAT=json the log is JSON lines; a plain-text log yields zero.
func scanDBTJSONLog(body []byte) (jsonEvents int, cacheCodes []string) {
	accepted := map[string]bool{"I017": true, "I040": true}
	rejected := map[string]bool{"I016": true, "I024": true, "I028": true}
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var event struct {
			Info struct {
				Code string `json:"code"`
			} `json:"info"`
		}
		if json.Unmarshal([]byte(line), &event) != nil {
			continue
		}
		jsonEvents++
		if accepted[event.Info.Code] || rejected[event.Info.Code] {
			cacheCodes = append(cacheCodes, event.Info.Code)
		}
	}
	return jsonEvents, cacheCodes
}

// queryRunExecutionMode returns the execution_mode ("workers"/"jobs") of a
// single run's production deployment, or "" if none exists yet. Scoping by
// schedule_id keeps it from reading a prior run's row.
func queryRunExecutionMode(t *testing.T, ctx context.Context, clients *testClients, runID uuid.UUID) string {
	t.Helper()
	var mode string
	err := clients.executorDB.GetContext(ctx, &mode,
		`SELECT execution_mode FROM executor_deployments
		  WHERE schedule_id = $1 AND mode = 'production'
		  ORDER BY created_at DESC LIMIT 1`, runID)
	if err != nil {
		return ""
	}
	return mode
}

// queryRunLeasePod returns the worker pod name that held the lease for a single
// run's production task, or "" if none. Scoping by schedule_id keeps one run's
// pod from being confused with another's.
func queryRunLeasePod(t *testing.T, ctx context.Context, clients *testClients, runID uuid.UUID) string {
	t.Helper()
	var pod *string
	err := clients.executorDB.GetContext(ctx, &pod,
		`SELECT lease_pod_name FROM executor_deployments
		  WHERE schedule_id = $1 AND mode = 'production'
		  ORDER BY created_at DESC LIMIT 1`, runID)
	if err != nil || pod == nil {
		return ""
	}
	return *pod
}
