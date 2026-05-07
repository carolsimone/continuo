package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"testing"
	"time"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// verifyServicesHealthy checks all service health endpoints
func verifyServicesHealthy(t *testing.T) {
	dockerComposeServices := []string{
		"http://state:8082/health",
		"http://orchestrator:8087/health",
	}

	for _, url := range dockerComposeServices {
		resp, err := http.Get(url)
		require.NoError(t, err, "Failed to reach service: %s", url)
		require.Equal(t, http.StatusOK, resp.StatusCode, "Service unhealthy: %s", url)
		resp.Body.Close()
	}

	// k8s controllers
	k8sControllers := map[string]int{
		"executor-controller": 8084,
		"k8s-controller":      8085,
	}

	for deployment, port := range k8sControllers {
		portForwardCmd := exec.Command("kubectl", "port-forward",
			fmt.Sprintf("deployment/%s", deployment),
			fmt.Sprintf("%d:%d", port, port),
			"-n", "default",
		)
		err := portForwardCmd.Start()
		require.NoError(t, err, "Failed to start port-forward for %s", deployment)
		defer func() {
			portForwardCmd.Process.Kill() //nolint:errcheck
			portForwardCmd.Wait()        //nolint:errcheck
		}()

		// Poll until health endpoint responds or times out
		pollUntil(t, context.Background(), 30*time.Second, time.Second, func() (bool, error) {
			resp, err := http.Get(fmt.Sprintf("http://localhost:%d/health", port))
			if err != nil {
				return false, nil // transient — retry
			}
			resp.Body.Close()
			return resp.StatusCode == http.StatusOK, nil
		}, fmt.Sprintf("K8s service unhealthy after port-forward: %s", deployment))
	}
	t.Log("✅ All services healthy (docker-compose + K8s)")
}

// verifyK8sAvailable checks if kubectl can reach the cluster
func verifyK8sAvailable(t *testing.T, ctx context.Context) {
	cmd := exec.CommandContext(ctx, "kubectl", "cluster-info")
	err := cmd.Run()
	require.NoError(t, err, "kubectl cannot reach cluster - is kind running?")
	t.Log("✅ Kubernetes cluster available")
}

// pollUntil polls a condition until it succeeds or times out.
// It logs a progress message every 30 seconds so long-running polls are visible.
func pollUntil(
	t *testing.T,
	ctx context.Context,
	timeout time.Duration,
	interval time.Duration,
	condition func() (bool, error),
	timeoutMsg string,
) {
	deadline := time.After(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	start := time.Now()
	progressInterval := 30 * time.Second
	lastProgress := time.Now()

	for {
		select {
		case <-deadline:
			t.Fatal(timeoutMsg)
		case <-ticker.C:
			success, err := condition()
			if err != nil {
				t.Logf("Poll condition error (will retry): %v", err)
				continue
			}
			if success {
				return
			}
			if time.Since(lastProgress) >= progressInterval {
				t.Logf("Still waiting... (%.0fs elapsed) — %s", time.Since(start).Seconds(), timeoutMsg)
				lastProgress = time.Now()
			}
		}
	}
}

// containsAll checks if slice contains all elements
func containsAll(slice, elements []string) bool {
	elementMap := make(map[string]bool)
	for _, elem := range elements {
		elementMap[elem] = false
	}
	for _, item := range slice {
		if _, exists := elementMap[item]; exists {
			elementMap[item] = true
		}
	}
	for _, found := range elementMap {
		if !found {
			return false
		}
	}
	return true
}

// k8sJobList represents kubectl get jobs JSON output
type k8sJobList struct {
	Items []struct {
		Metadata struct {
			Name   string            `json:"name"`
			Labels map[string]string `json:"labels"`
		} `json:"metadata"`
		Status struct {
			Succeeded int `json:"succeeded"`
		} `json:"status"`
	} `json:"items"`
}

// getK8sJobs retrieves jobs from k8s cluster
func getK8sJobs(ctx context.Context, labelSelector string) (*k8sJobList, error) {
	cmd := exec.CommandContext(ctx, "kubectl", "get", "jobs",
		"-n", "default",
		"-l", labelSelector,
		"-o", "json")
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	var jobList k8sJobList
	if err := json.Unmarshal(output, &jobList); err != nil {
		return nil, err
	}
	return &jobList, nil
}

// getK8sJobLogs retrieves logs from a k8s job's pod
func getK8sJobLogs(ctx context.Context, t *testing.T, tableName string) string {
	// Get pod name
	cmd := exec.CommandContext(ctx, "kubectl", "get", "pods",
		"-l", fmt.Sprintf("table_name=%s", tableName),
		"-o", "jsonpath={.items[0].metadata.name}")
	podName, err := cmd.Output()
	require.NoError(t, err, "Failed to get pod name for table: %s", tableName)

	// Get logs
	cmd = exec.CommandContext(ctx, "kubectl", "logs", string(podName))
	logs, err := cmd.Output()
	require.NoError(t, err, "Failed to get logs for pod: %s", string(podName))

	return string(logs)
}

// verifyRedisStreamHasMessages checks Redis stream message count
func verifyRedisStreamHasMessages(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	streamName string,
	minCount int64,
) {
	pollUntil(t, ctx, 30*time.Second, 500*time.Millisecond, func() (bool, error) {
		messages, err := clients.redisClient.XRange(ctx, streamName, "-", "+").Result()
		if err != nil {
			return false, err
		}
		return int64(len(messages)) >= minCount, nil
	}, fmt.Sprintf("Timeout waiting for %d messages in Redis stream %s", minCount, streamName))

	t.Logf("✅ Redis stream %s has at least %d messages", streamName, minCount)
}

// xpending returns the count of unacked messages in a Redis stream's consumer group.
// Returns 0 if the consumer group doesn't exist (treats NOGROUP as "no pending").
func xpending(t *testing.T, ctx context.Context, clients *testClients, stream, group string) int64 {
	t.Helper()
	res, err := clients.redisClient.XPending(ctx, stream, group).Result()
	if err != nil {
		// NOGROUP variants vary across Redis versions; broaden the substring match.
		if errStr := err.Error(); len(errStr) >= 7 && errStr[:7] == "NOGROUP" {
			return 0
		}
		require.NoError(t, err, "XPENDING %s %s failed", stream, group)
	}
	return res.Count
}

// readTopologyGeneration returns the current topology_generation counter from
// the orchestrator's Postgres state. Used by tests that assert no topology
// commit happened (counter unchanged).
func readTopologyGeneration(t *testing.T, ctx context.Context, clients *testClients) int64 {
	t.Helper()
	var gen int64
	err := clients.orchestratorDB.GetContext(ctx, &gen,
		`SELECT topology_generation FROM topology_state WHERE id = TRUE`)
	require.NoError(t, err, "SELECT topology_generation failed")
	return gen
}

// countRejectedTopologyMessages returns the number of rows in the orchestrator's
// rejected_topology_messages forensics table.
func countRejectedTopologyMessages(t *testing.T, ctx context.Context, clients *testClients) int {
	t.Helper()
	var n int
	err := clients.orchestratorDB.GetContext(ctx, &n,
		`SELECT count(*) FROM rejected_topology_messages`)
	require.NoError(t, err, "SELECT count(*) FROM rejected_topology_messages failed")
	return n
}

// manifestControllerLogContains tails the manifest-controller container's log
// file (where its main loop writes via start-services.sh's bash launch line).
func manifestControllerLogContains(t *testing.T, substr string) bool {
	t.Helper()
	cmd := exec.Command("docker", "exec", "manifest-controller",
		"cat", "/tmp/mc.log")
	out, err := cmd.Output()
	if err != nil {
		t.Logf("docker exec manifest-controller cat /tmp/mc.log failed: %v", err)
		return false
	}
	return contains(string(out), substr)
}

// contains is a tiny string-substring shim so we don't add a `strings` import
// solely for this one call site.
func contains(haystack, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// scheduleState captures the fields the watchdog test asserts on.
type scheduleState struct {
	Status             string `db:"status"`
	CancellationReason string `db:"cancellation_reason"`
	CancelledBy        string `db:"cancelled_by"`
}

// readScheduleState returns the most-recently-created scheduler_tracker row
// for a schedule matched by name. Returns an empty scheduleState if no row
// matches (so callers can poll without distinguishing "not found" from "found
// in non-terminal state").
func readScheduleState(t *testing.T, ctx context.Context, clients *testClients, scheduleName string) scheduleState {
	t.Helper()
	var st scheduleState
	err := clients.stateDB.GetContext(ctx, &st,
		`SELECT
		    coalesce(status, '') AS status,
		    coalesce(cancellation_reason, '') AS cancellation_reason,
		    coalesce(cancelled_by, '') AS cancelled_by
		   FROM scheduler_tracker
		  WHERE schedule_name = $1
		  ORDER BY created_at DESC
		  LIMIT 1`, scheduleName)
	if err != nil {
		return scheduleState{}
	}
	return st
}

// triggerSchedule calls state.TriggerSchedule via gRPC and returns the new run's
// schedule_id (UUID string).
func triggerSchedule(t *testing.T, ctx context.Context, clients *testClients, scheduleName string) string {
	t.Helper()
	resp, err := clients.stateClient.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{
		ScheduleName: scheduleName,
	})
	require.NoError(t, err, "TriggerSchedule(%q) failed", scheduleName)
	return resp.GetScheduleId()
}

// queryNeo4jRunKind returns the :Run.kind property for a run, or "" if the node
// is not found. Used by PR0 audit assertions to verify the kind is stamped on
// the Neo4j run snapshot.
func queryNeo4jRunKind(t *testing.T, clients *testClients, runID uuid.UUID) string {
	t.Helper()
	session := clients.neo4jDriver.NewSession(context.Background(), neo4jdriver.SessionConfig{
		AccessMode: neo4jdriver.AccessModeRead,
	})
	defer session.Close(context.Background())
	result, err := session.Run(context.Background(),
		`MATCH (r:Run {run_id: $run_id}) RETURN COALESCE(r.kind, "") AS kind`,
		map[string]interface{}{"run_id": runID.String()})
	require.NoError(t, err)
	if !result.Next(context.Background()) {
		return ""
	}
	raw, _ := result.Record().Get("kind")
	s, _ := raw.(string)
	return s
}

// queryPostgresTrackerKind returns scheduler_tracker.kind for a run identified by
// its schedule_id. Used by PR0 audit assertions.
func queryPostgresTrackerKind(t *testing.T, db *sqlx.DB, scheduleID uuid.UUID) string {
	t.Helper()
	var kind string
	require.NoError(t, db.GetContext(context.Background(), &kind,
		`SELECT kind FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID))
	return kind
}

// queryFirstTaskTrackerMetadata returns the manifest_version and image_tag of
// any task_tracker row for the given schedule_id. Used by PR0 audit assertions to
// verify per-task metadata is populated after a successful run.
func queryFirstTaskTrackerMetadata(t *testing.T, db *sqlx.DB, scheduleID uuid.UUID) (manifestVersion, imageTag string) {
	t.Helper()
	var sample struct {
		ManifestVersion string `db:"manifest_version"`
		ImageTag        string `db:"image_tag"`
	}
	require.NoError(t, db.GetContext(context.Background(), &sample,
		`SELECT manifest_version, image_tag FROM task_tracker WHERE schedule_id = $1 LIMIT 1`, scheduleID))
	return sample.ManifestVersion, sample.ImageTag
}

// deleteS3Sidecar removes service_metadata.json from localstack S3 for a
// service. Used by E2E.1 to create the "no sidecar" condition that A1's
// publish-boundary validator must reject. Shells out to dbt-compile-and-load
// (which already has boto3 + the localstack endpoint configured) so we don't
// need to add aws-sdk-go to tests/e2e/go.mod.
func deleteS3Sidecar(t *testing.T, serviceName string) {
	t.Helper()
	pyScript := fmt.Sprintf(`
import boto3, os
s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["S3_ENDPOINT_URL"],
    region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
)
key = os.environ["S3_ENV"] + "/manifest/%s/service_metadata.json"
s3.delete_object(Bucket=os.environ["S3_BUCKET"], Key=key)
print("deleted", key)
`, serviceName)
	cmd := exec.Command("docker", "exec",
		"-e", "S3_ENDPOINT_URL=http://localstack:4566",
		"-e", "S3_BUCKET=continuo",
		"-e", "S3_ENV=local",
		"-e", "AWS_ACCESS_KEY_ID=test",
		"-e", "AWS_SECRET_ACCESS_KEY=test",
		"-e", "AWS_DEFAULT_REGION=us-east-1",
		"dbt-compile-and-load",
		"uv", "run", "python", "-c", pyScript)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "deleteS3Sidecar(%q) failed: %s", serviceName, string(out))
	t.Logf("deleted sidecar for %s: %s", serviceName, string(out))
}
