package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// verifyServicesHealthy checks all service health endpoints
func verifyServicesHealthy(t *testing.T) {
	dockerComposeServices := []string{
		"http://state:8082/health",
		"http://orchestrator:8087/health",
	}

	for _, url := range dockerComposeServices {
		resp, err := http.Get(url) //nolint:gosec // url is one of the fixed dockerComposeServices literals above, not external input
		require.NoError(t, err, "Failed to reach service: %s", url)
		require.Equal(t, http.StatusOK, resp.StatusCode, "Service unhealthy: %s", url)
		_ = resp.Body.Close()
	}

	// k8s controllers
	k8sControllers := map[string]int{"execution-controller": 8084}

	for deployment, port := range k8sControllers {
		// `kubectl port-forward deployment/X` attaches to whatever pod the API
		// server picks — which during/after a rolling restart can be a dying
		// pod. If /health keeps failing on the same port-forward, the pod is
		// likely terminating; tear it down and retry with a fresh port-forward
		// (which re-resolves to a currently-healthy pod). The outer attempt
		// loop owns the port-forward lifecycle so each retry is clean.
		portForwardHealthy(t, deployment, port)
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

// portForwardHealthy ensures `deployment` is serving 200 OK on /health via a
// kubectl port-forward. If a single port-forward keeps failing /health (e.g.
// it landed on a terminating pod after a rolling restart), the port-forward
// is killed and re-established to re-resolve to a currently-healthy pod.
//
// Total wall-clock budget: up to 6 attempts × 20 s probe window = 120 s.
func portForwardHealthy(t *testing.T, deployment string, port int) {
	t.Helper()

	const maxAttempts = 6
	const probeWindow = 20 * time.Second
	const probeInterval = 1 * time.Second

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		pf := exec.Command("kubectl", "port-forward", //nolint:gosec // deployment/port come from the fixed k8sControllers map in verifyServicesHealthy, not external input
			fmt.Sprintf("deployment/%s", deployment),
			fmt.Sprintf("%d:%d", port, port),
			"-n", "default",
		)
		if err := pf.Start(); err != nil {
			lastErr = fmt.Errorf("attempt %d: failed to start port-forward: %w", attempt, err)
			continue
		}

		// Probe /health for up to probeWindow on this port-forward.
		deadline := time.After(probeWindow)
		ticker := time.NewTicker(probeInterval)
		ok := false
	probeLoop:
		for {
			select {
			case <-deadline:
				break probeLoop
			case <-ticker.C:
				resp, err := http.Get(fmt.Sprintf("http://localhost:%d/health", port))
				if err != nil {
					lastErr = err
					continue
				}
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					ok = true
					break probeLoop
				}
				lastErr = fmt.Errorf("health returned status %d", resp.StatusCode)
			}
		}
		ticker.Stop()

		if ok {
			// Keep this port-forward running for the rest of the test; tear
			// it down only when the test completes.
			t.Cleanup(func() {
				pf.Process.Kill() //nolint:errcheck
				pf.Wait()         //nolint:errcheck
			})
			return
		}

		// This attempt failed — kill the port-forward and try again with a
		// fresh one (which re-resolves the deployment to a healthy pod).
		pf.Process.Kill() //nolint:errcheck
		pf.Wait()         //nolint:errcheck
		t.Logf("port-forward attempt %d for %s failed (last error: %v) — retrying with fresh port-forward", attempt, deployment, lastErr)
	}

	require.FailNow(t,
		fmt.Sprintf("K8s service unhealthy after port-forward: %s (after %d attempts; last error: %v)", deployment, maxAttempts, lastErr))
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
	t.Helper()
	pollUntilDescribed(t, ctx, timeout, interval, condition, func() string { return timeoutMsg })
}

// pollUntilDescribed is pollUntil with a message built when it is printed, so
// a timeout can name what the condition last observed.
func pollUntilDescribed(
	t *testing.T,
	ctx context.Context,
	timeout time.Duration,
	interval time.Duration,
	condition func() (bool, error),
	describe func() string,
) {
	t.Helper()
	deadline := time.After(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	start := time.Now()
	progressInterval := 30 * time.Second
	lastProgress := time.Now()

	for {
		select {
		case <-deadline:
			t.Fatal(describe())
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
				t.Logf("Still waiting... (%.0fs elapsed) — %s", time.Since(start).Seconds(), describe())
				lastProgress = time.Now()
			}
		}
	}
}

// waitForParseCache polls state's task_attempts rows for the given
// schedule's most recently reported attempt until parse_cache equals want
// ("hydrated" or "degraded"). task_attempts has no schedule_id column of its
// own — it joins through task_tracker (task_attempts.task_id ->
// task_tracker.task_id -> task_tracker.schedule_id) to scope the query to a
// single triggered run.
// parse_cache_reason (only ever populated alongside "degraded") is polled
// alongside parse_cache and logged on every change and in the timeout failure
// message, so a degrade's cause is visible in CI output without a manual DB
// query. This does not use the shared pollUntil helper: pollUntil's
// timeoutMsg is a plain string evaluated once at the call site, before any
// polling happens, so it cannot carry the last-observed reason — that is
// only known once polling has run.
func waitForParseCache(t *testing.T, ctx context.Context, db *sqlx.DB, scheduleID uuid.UUID, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	start := time.Now()
	progressInterval := 30 * time.Second
	lastProgress := time.Now()
	var last, lastReason sql.NullString

	for {
		select {
		case <-deadline:
			t.Fatalf("parse_cache never became %q for schedule %s (last observed: parse_cache=%q parse_cache_reason=%q)",
				want, scheduleID, last.String, lastReason.String)
		case <-ticker.C:
			var got, reason sql.NullString
			err := db.QueryRowx(`
				SELECT a.parse_cache, a.parse_cache_reason
				  FROM task_attempts a
				  JOIN task_tracker t ON t.task_id = a.task_id
				 WHERE t.schedule_id = $1
				 ORDER BY a.last_report_at DESC NULLS LAST
				 LIMIT 1`, scheduleID).Scan(&got, &reason)
			if err != nil {
				if err == sql.ErrNoRows {
					// No task_attempts row written yet — keep polling.
					continue
				}
				t.Logf("Poll condition error (will retry): %v", err)
				continue
			}
			if got != last || reason != lastReason {
				t.Logf("parse_cache for schedule %s: %q (reason=%q)", scheduleID, got.String, reason.String)
				last = got
				lastReason = reason
			}
			if got.Valid && got.String == want {
				t.Logf("✅ parse_cache=%q for schedule %s", want, scheduleID)
				return
			}
			if time.Since(lastProgress) >= progressInterval {
				t.Logf("Still waiting... (%.0fs elapsed) — parse_cache never became %q for schedule %s (last observed: parse_cache=%q parse_cache_reason=%q)",
					time.Since(start).Seconds(), want, scheduleID, last.String, lastReason.String)
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

// k8sJob is one Job of kubectl get jobs JSON output.
type k8sJob struct {
	Metadata struct {
		Name              string            `json:"name"`
		Labels            map[string]string `json:"labels"`
		CreationTimestamp time.Time         `json:"creationTimestamp"`
		DeletionTimestamp *time.Time        `json:"deletionTimestamp"`
	} `json:"metadata"`
	Status struct {
		Succeeded  int `json:"succeeded"`
		Conditions []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"conditions"`
	} `json:"status"`
	Spec struct {
		ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds"`
		Template              struct {
			Spec struct {
				Containers []struct {
					Command []string `json:"command"`
				} `json:"containers"`
			} `json:"spec"`
		} `json:"template"`
	} `json:"spec"`
}

// finished reports whether the Job is terminal: Kubernetes marked it Complete
// or Failed, so it runs no further pod.
func (j k8sJob) finished() bool {
	for _, c := range j.Status.Conditions {
		if (c.Type == "Complete" || c.Type == "Failed") && c.Status == "True" {
			return true
		}
	}
	return false
}

// k8sJobList represents kubectl get jobs JSON output
type k8sJobList struct {
	Items []k8sJob `json:"items"`
}

// getK8sJobs retrieves jobs from k8s cluster
func getK8sJobs(ctx context.Context, labelSelector string) (*k8sJobList, error) {
	cmd := exec.CommandContext(ctx, "kubectl", "get", "jobs", //nolint:gosec // labelSelector is built by the tests from fixed label keys and values they generate or read from state, not external input
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

// k8sPodList is the part of kubectl get pods JSON output the tests read.
type k8sPodList struct {
	Items []struct {
		Metadata struct {
			Name   string            `json:"name"`
			Labels map[string]string `json:"labels"`
		} `json:"metadata"`
		Status struct {
			Phase string `json:"phase"`
			// StartTime is when the kubelet accepted the pod, before any of its
			// containers started; nil until then.
			StartTime             *time.Time           `json:"startTime"`
			InitContainerStatuses []k8sContainerStatus `json:"initContainerStatuses"`
			ContainerStatuses     []k8sContainerStatus `json:"containerStatuses"`
		} `json:"status"`
	} `json:"items"`
}

// k8sContainerStatus is the part of a pod's container status the tests read:
// when the container ran, once it has terminated.
type k8sContainerStatus struct {
	Name  string `json:"name"`
	State struct {
		Terminated *struct {
			StartedAt  time.Time `json:"startedAt"`
			FinishedAt time.Time `json:"finishedAt"`
		} `json:"terminated"`
	} `json:"state"`
}

// getK8sPods lists the pods matching labelSelector in the namespace the Jobs
// run in, those being deleted included.
func getK8sPods(ctx context.Context, labelSelector string) (*k8sPodList, error) {
	cmd := exec.CommandContext(ctx, "kubectl", "get", "pods", //nolint:gosec // labelSelector is built by the tests from fixed label keys and values they read from state, not external input
		"-n", "default",
		"-l", labelSelector,
		"-o", "json")
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	var podList k8sPodList
	if err := json.Unmarshal(output, &podList); err != nil {
		return nil, err
	}
	return &podList, nil
}

// attemptLabel returns the attempt number execution-controller labels a Job
// and its pods with, or -1 when the label is absent or not a number.
func attemptLabel(labels map[string]string) int {
	n, err := strconv.Atoi(labels["attempt"])
	if err != nil {
		return -1
	}
	return n
}

// waitForAttemptJob waits until the Job of attempt n of taskID exists and
// returns it.
func waitForAttemptJob(t *testing.T, ctx context.Context, taskID uuid.UUID, n int, timeout time.Duration) k8sJob {
	t.Helper()
	selector := fmt.Sprintf("task-id=%s,attempt=%d", taskID, n)
	var job k8sJob
	pollUntil(t, ctx, timeout, time.Second, func() (bool, error) {
		jobs, err := getK8sJobs(ctx, selector)
		if err != nil {
			return false, err
		}
		if len(jobs.Items) == 0 {
			return false, nil
		}
		job = jobs.Items[0]
		return true, nil
	}, fmt.Sprintf("Timeout waiting for the Job of attempt %d of task %s", n, taskID))
	t.Logf("Job %s runs attempt %d of task %s (created %s)", job.Metadata.Name, n, taskID, job.Metadata.CreationTimestamp.Format(time.RFC3339))
	return job
}

// jobStartTimeout returns the start timeout the cluster's execution-controller
// runs with: how long a Job's pod may take, after the Job is created, to start
// a container. It is the Deployment's JOB_START_TIMEOUT_SECONDS
// (tests/e2e/k8s/execution-controller-deployment.yaml); a Deployment without it
// fails the test rather than leave it to wait out the service's default.
func jobStartTimeout(t *testing.T, ctx context.Context) time.Duration {
	t.Helper()
	out, err := exec.CommandContext(ctx, "kubectl", "get", "deployment", "execution-controller",
		"-n", "default", "-o", "json").Output()
	require.NoError(t, err, "read the execution-controller Deployment")
	var deployment struct {
		Spec struct {
			Template struct {
				Spec struct {
					Containers []struct {
						Env []struct {
							Name  string `json:"name"`
							Value string `json:"value"`
						} `json:"env"`
					} `json:"containers"`
				} `json:"spec"`
			} `json:"template"`
		} `json:"spec"`
	}
	require.NoError(t, json.Unmarshal(out, &deployment), "decode the execution-controller Deployment")
	for _, c := range deployment.Spec.Template.Spec.Containers {
		for _, e := range c.Env {
			if e.Name != "JOB_START_TIMEOUT_SECONDS" {
				continue
			}
			seconds, err := strconv.Atoi(e.Value)
			require.NoError(t, err, "JOB_START_TIMEOUT_SECONDS=%q is not a whole number of seconds", e.Value)
			return time.Duration(seconds) * time.Second
		}
	}
	require.FailNow(t, "the execution-controller Deployment sets no JOB_START_TIMEOUT_SECONDS")
	return 0
}

// queryPostgresTrackerKind returns scheduler_tracker.kind for a run identified by
// its schedule_id.
func queryPostgresTrackerKind(t *testing.T, db *sqlx.DB, scheduleID uuid.UUID) string {
	t.Helper()
	var kind string
	require.NoError(t, db.GetContext(context.Background(), &kind,
		`SELECT kind FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID))
	return kind
}

// queryRunOperation returns scheduler_tracker.operation for a run: run, test,
// build or full_refresh.
func queryRunOperation(t *testing.T, clients *testClients, runID uuid.UUID) string {
	t.Helper()
	var operation string
	require.NoError(t, clients.stateDB.GetContext(context.Background(), &operation,
		`SELECT operation FROM scheduler_tracker WHERE schedule_id = $1`, runID))
	return operation
}

// queryRunSourceRunID returns scheduler_tracker.source_run_id for a run, or ""
// when the run has no source run.
func queryRunSourceRunID(t *testing.T, clients *testClients, runID uuid.UUID) string {
	t.Helper()
	var source *string
	require.NoError(t, clients.stateDB.GetContext(context.Background(), &source,
		`SELECT source_run_id::text FROM scheduler_tracker WHERE schedule_id = $1`, runID))
	if source == nil {
		return ""
	}
	return *source
}

// countRunsOfKind counts the scheduler_tracker rows of kind.
func countRunsOfKind(t *testing.T, clients *testClients, kind string) int {
	t.Helper()
	var n int
	require.NoError(t, clients.stateDB.GetContext(context.Background(), &n,
		`SELECT count(*) FROM scheduler_tracker WHERE kind = $1`, kind))
	return n
}

// queryFirstTaskTrackerImageTag returns the image_tag of any task_tracker row
// for the given schedule_id, so assertions can verify the per-task image is
// populated after a successful run.
func queryFirstTaskTrackerImageTag(t *testing.T, db *sqlx.DB, scheduleID uuid.UUID) string {
	t.Helper()
	var imageTag string
	require.NoError(t, db.GetContext(context.Background(), &imageTag,
		`SELECT image_tag FROM task_tracker WHERE schedule_id = $1 LIMIT 1`, scheduleID))
	return imageTag
}
