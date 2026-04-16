package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// verifyServicesHealthy checks all service health endpoints
func verifyServicesHealthy(t *testing.T) {
	dockerComposeServices := []string{
		"http://state:8082/health",
		"http://startup-controller:8083/health",
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

// verifyStartupOutboxProcessed checks startup_outbox table
func verifyStartupOutboxProcessed(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	schedulerID uuid.UUID,
	expectedCount int,
) {
	pollUntil(t, ctx, 60*time.Second, 1*time.Second, func() (bool, error) {
		var processedCount int
		err := clients.startupDB.Get(&processedCount, `
			SELECT COUNT(*)
			FROM startup_outbox
			WHERE aggregate_id = $1 AND status = 'processed'
		`, schedulerID)

		if err != nil {
			return false, err
		}

		return processedCount == expectedCount, nil
	}, fmt.Sprintf("Timeout waiting for startup-controller to process %d outbox entries", expectedCount))

	t.Logf("✅ startup-controller processed %d outbox entries", expectedCount)
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
