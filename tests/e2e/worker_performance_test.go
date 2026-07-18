//go:build e2e_worker

package e2e

import (
	"context"
	"encoding/json"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// perfReport is written to /tmp/continuo-worker-performance.json and echoed on
// failure so a perf regression is diagnosable from the numbers.
type perfReport struct {
	WarmWorkerSamplesMS []float64 `json:"warm_worker_samples_ms"`
	JobSamplesMS        []float64 `json:"job_samples_ms"`
	WorkerP50MS         float64   `json:"warm_worker_p50_ms"`
	WorkerP95MS         float64   `json:"warm_worker_p95_ms"`
	JobP50MS            float64   `json:"job_p50_ms"`
	JobP95MS            float64   `json:"job_p95_ms"`
	Ratio               float64   `json:"worker_over_job_ratio"`
}

const perfReportPath = "/tmp/continuo-worker-performance.json"

// TestE2E_WorkerPerformance is the non-flaky performance gate. It runs the same
// native fixture (service-3 worker_perf) on the same image against the same
// warehouse, first on warm worker pods and then as Kubernetes Jobs, and proves
// the warm worker reaches its first dbt invocation both in bounded absolute time
// and far faster than a Job.
//
// The worker measurement is single-clock: lease grant and first-dbt-invocation
// are both executor-controller timestamps, so no cross-service clock skew enters
// a sub-second number. The Job measurement spans executor (reservation) to state
// (dbt start), which is a seconds-scale number where tens of milliseconds of
// skew are immaterial.
func TestE2E_WorkerPerformance(t *testing.T) {
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

	// Warm the pool: the first run pays cold-start (pod schedule + hydrate); the
	// measured samples must all be warm.
	warm := triggerWorkerNode(t, ctx, clients, "service-3", "e2e_schema", "worker_perf")
	verifySchedulerSucceeded(t, ctx, clients, warm)

	// Collect 20 warm native worker samples. Both timestamps are executor-clock
	// and immutable: slot_reserved_at is stamped when the claim grants the lease
	// (the worker path reserves its slot at claim), and started_at when the
	// worker acknowledges its dbt process started. Neither is touched by a
	// heartbeat, so a task of any duration yields a valid claim→dbt-start sample.
	const wantWorker = 20
	var workerMS []float64
	for i := 0; i < wantWorker; i++ {
		run := triggerWorkerNode(t, ctx, clients, "service-3", "e2e_schema", "worker_perf")
		verifySchedulerSucceeded(t, ctx, clients, run)
		started, reserved, mode, ok := workerTiming(t, ctx, clients, run)
		require.True(t, ok, "no worker deployment row with both timestamps for run %s", run)
		require.Equal(t, "workers", mode, "warm sample must have run on the worker path")
		workerMS = append(workerMS, started.Sub(reserved).Seconds()*1000)
	}

	// Roll service-3 back to Jobs — which is also the sufficient rollback path —
	// and measure the same fixture as a Kubernetes Job.
	setExecutorOverrides(t, ctx, "{}")

	const wantJob = 10
	var jobMS []float64
	for attempt := 0; attempt < wantJob+8 && len(jobMS) < wantJob; attempt++ {
		run := triggerWorkerNode(t, ctx, clients, "service-3", "e2e_schema", "worker_perf")
		verifySchedulerSucceeded(t, ctx, clients, run)
		reserved, jobName, mode, ok := jobReservationTiming(t, ctx, clients, run)
		require.True(t, ok, "no job deployment row for run %s", run)
		require.Equal(t, "jobs", mode, "rollback sample must have run on the Jobs path")
		// The Job's dbt process start is the container's kubelet-observed start
		// time. Reservation (executor clock) to container start (kubelet clock)
		// crosses clocks, but a Job's reservation→dbt-start is seconds (pod
		// schedule + container boot), so the tens-of-milliseconds host clock skew
		// that made a worker's sub-20ms cross-clock number meaningless is immaterial
		// here.
		started, ok := jobContainerStart(t, ctx, jobName)
		if !ok {
			t.Logf("run %s job pod start not yet observable — skipping as a timing sample", run)
			continue
		}
		jobMS = append(jobMS, started.Sub(reserved).Seconds()*1000)
	}
	require.GreaterOrEqual(t, len(jobMS), wantJob, "could not collect %d Job samples", wantJob)

	workerP95 := time.Duration(nearestRankP95(workerMS) * float64(time.Millisecond))
	jobP95 := time.Duration(nearestRankP95(jobMS) * float64(time.Millisecond))
	report := perfReport{
		WarmWorkerSamplesMS: workerMS,
		JobSamplesMS:        jobMS,
		WorkerP50MS:         percentile(workerMS, 50),
		WorkerP95MS:         nearestRankP95(workerMS),
		JobP50MS:            percentile(jobMS, 50),
		JobP95MS:            nearestRankP95(jobMS),
		Ratio:               nearestRankP95(workerMS) / nearestRankP95(jobMS),
	}
	writePerfReport(t, report)
	t.Logf("perf: worker p50=%.1fms p95=%.1fms | job p50=%.1fms p95=%.1fms | ratio=%.3f",
		report.WorkerP50MS, report.WorkerP95MS, report.JobP50MS, report.JobP95MS, report.Ratio)

	require.LessOrEqual(t, workerP95, time.Second,
		"warm worker p95 lease→dbt-start must be within one second (see %s)", perfReportPath)
	require.LessOrEqual(t, float64(workerP95), float64(jobP95)*0.20,
		"warm worker p95 must be at most a fifth of the Job p95 (see %s)", perfReportPath)
}

// workerTiming returns the dbt-start (started_at) and lease-grant
// (slot_reserved_at) timestamps of the worker deployment for a run, plus its
// execution mode. Both are immutable executor-clock timestamps.
func workerTiming(t *testing.T, ctx context.Context, clients *testClients, runID uuid.UUID) (startedAt, reservedAt time.Time, mode string, found bool) {
	t.Helper()
	var row struct {
		StartedAt      *time.Time `db:"started_at"`
		SlotReservedAt *time.Time `db:"slot_reserved_at"`
		ExecutionMode  string     `db:"execution_mode"`
	}
	err := clients.executorDB.GetContext(ctx, &row,
		`SELECT started_at, slot_reserved_at, execution_mode
		   FROM executor_deployments
		  WHERE schedule_id = $1 AND mode = 'production'
		  ORDER BY created_at DESC LIMIT 1`, runID)
	if err != nil || row.StartedAt == nil || row.SlotReservedAt == nil {
		return time.Time{}, time.Time{}, "", false
	}
	return *row.StartedAt, *row.SlotReservedAt, row.ExecutionMode, true
}

// jobReservationTiming returns the slot reservation time and job name of the Job
// deployment for a run, plus its execution mode.
func jobReservationTiming(t *testing.T, ctx context.Context, clients *testClients, runID uuid.UUID) (reservedAt time.Time, jobName string, mode string, found bool) {
	t.Helper()
	var row struct {
		SlotReservedAt *time.Time `db:"slot_reserved_at"`
		JobName        string     `db:"job_name"`
		ExecutionMode  string     `db:"execution_mode"`
	}
	err := clients.executorDB.GetContext(ctx, &row,
		`SELECT slot_reserved_at, COALESCE(job_params->>'job_name','') AS job_name, execution_mode
		   FROM executor_deployments
		  WHERE schedule_id = $1 AND mode = 'production'
		  ORDER BY created_at DESC LIMIT 1`, runID)
	if err != nil || row.SlotReservedAt == nil {
		return time.Time{}, "", "", false
	}
	return *row.SlotReservedAt, row.JobName, row.ExecutionMode, true
}

// jobContainerStart returns the kubelet-observed start time of a Job's dbt-job
// container. The Job completes quickly, so the container is read from either its
// running or terminated state.
func jobContainerStart(t *testing.T, ctx context.Context, jobName string) (time.Time, bool) {
	t.Helper()
	if jobName == "" {
		return time.Time{}, false
	}
	var podList struct {
		Items []struct {
			Status struct {
				ContainerStatuses []struct {
					Name  string `json:"name"`
					State struct {
						Running    *struct {
							StartedAt time.Time `json:"startedAt"`
						} `json:"running"`
						Terminated *struct {
							StartedAt time.Time `json:"startedAt"`
						} `json:"terminated"`
					} `json:"state"`
				} `json:"containerStatuses"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := kubectlJSON(t, ctx, &podList,
		"get", "pods", "-n", "default", "-l", "job-name="+jobName, "-o", "json"); err != nil {
		return time.Time{}, false
	}
	for _, p := range podList.Items {
		for _, cs := range p.Status.ContainerStatuses {
			if cs.Name != "dbt-job" {
				continue
			}
			if cs.State.Terminated != nil && !cs.State.Terminated.StartedAt.IsZero() {
				return cs.State.Terminated.StartedAt, true
			}
			if cs.State.Running != nil && !cs.State.Running.StartedAt.IsZero() {
				return cs.State.Running.StartedAt, true
			}
		}
	}
	return time.Time{}, false
}

// nearestRankP95 returns the nearest-rank 95th percentile of the samples in
// milliseconds. The samples are copied before sorting so the caller's slice
// order (the collection order) is preserved for the report.
func nearestRankP95(samples []float64) float64 { return percentile(samples, 95) }

// percentile returns the nearest-rank p-th percentile of samples.
func percentile(samples []float64, p int) float64 {
	if len(samples) == 0 {
		return 0
	}
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)
	rank := (p*len(sorted) + 99) / 100 // ceil(p/100 * n)
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

// writePerfReport persists the samples and percentiles for post-run inspection.
func writePerfReport(t *testing.T, report perfReport) {
	t.Helper()
	body, err := json.MarshalIndent(report, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(perfReportPath, body, 0o644)) //nolint:gosec // diagnostic artifact, not sensitive
	t.Logf("wrote perf samples to %s", perfReportPath)
}
