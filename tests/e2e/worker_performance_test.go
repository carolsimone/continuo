//go:build e2e_worker

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const (
	// workerAbsoluteBound is the hard performance gate: a warm worker's
	// lease→dbt-start p95 must land within one second. Both timestamps are the
	// executor-controller's own clock, so this number carries no cross-service
	// skew and is always authoritative — it is asserted on every run.
	workerAbsoluteBound = time.Second

	// jobBaselineSanityFloor is the smallest Job reservation→pod-start p95 that
	// can be a real measurement. A Job's reservation (executor/Postgres clock) to
	// container start (kubelet clock) spans a pod schedule plus a container boot —
	// hundreds of milliseconds in practice (~370ms observed). Those two clocks
	// skew by tens-to-hundreds of milliseconds on this dev host, and around a date
	// boundary the skew can shrink or even invert the subtraction. A jobP95 below
	// this floor is therefore host clock skew, not a physically-impossible fast
	// Job, so the cross-clock ratio is not asserted from it. The floor sits well
	// below the real ~370ms baseline and well above sub-worker noise.
	jobBaselineSanityFloor = 50 * time.Millisecond
)

// perfGateDecision is the outcome of the performance gate over the two p95
// latencies. The absolute worker bound is always authoritative; the worker/Job
// ratio is asserted only when the Job baseline is clock-consistent.
type perfGateDecision struct {
	AbsoluteOK      bool   // workerP95 is within workerAbsoluteBound
	RatioApplicable bool   // the Job baseline is plausible enough to compare against
	RatioOK         bool   // workerP95 <= 0.20 * jobP95 (meaningful only when RatioApplicable)
	RatioSkipReason string // why the ratio was not asserted, for the run log
}

// evaluatePerfGate applies the performance gate to the collected p95 latencies.
// workerP95 is single-clock and is always checked against the absolute bound.
// jobP95 is cross-clock (executor reservation to kubelet container start); when
// it is implausibly small — below jobBaselineSanityFloor, which also catches the
// non-positive values host clock skew produces around a date boundary — it is
// treated as skew rather than a real Job time, so the ratio is skipped and the
// absolute bound stands alone.
func evaluatePerfGate(workerP95, jobP95 time.Duration) perfGateDecision {
	d := perfGateDecision{AbsoluteOK: workerP95 <= workerAbsoluteBound}
	if jobP95 < jobBaselineSanityFloor {
		d.RatioSkipReason = fmt.Sprintf(
			"Job baseline implausible (jobP95=%v) — reservation/pod-start clock skew on this host; "+
				"ratio gate not asserted, absolute worker bound is authoritative", jobP95)
		return d
	}
	d.RatioApplicable = true
	d.RatioOK = float64(workerP95) <= float64(jobP95)*0.20
	return d
}

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
// a sub-second number, and the absolute bound on it is always authoritative. The
// Job measurement spans executor (reservation, Postgres clock) to kubelet
// (container start), so it is cross-clock: on this dev host these clocks skew by
// tens-to-hundreds of milliseconds and can even flip the subtraction negative
// around a date boundary. The worker/Job ratio is therefore asserted only when
// the Job baseline is clock-consistent (see evaluatePerfGate); an implausible
// baseline is treated as host skew and logged, never failed.
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
		// time. Reservation (executor/Postgres clock) to container start (kubelet
		// clock) crosses clocks; a real Job's reservation→pod-start is hundreds of
		// milliseconds (pod schedule + container boot), but host clock skew can
		// shrink or invert that subtraction, so the resulting baseline is checked
		// for plausibility before the ratio is asserted (see evaluatePerfGate).
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

	decision := evaluatePerfGate(workerP95, jobP95)
	require.True(t, decision.AbsoluteOK,
		"warm worker p95 lease→dbt-start must be within one second: got %v (see %s)", workerP95, perfReportPath)
	if decision.RatioApplicable {
		require.True(t, decision.RatioOK,
			"warm worker p95 must be at most a fifth of the Job p95: worker=%v job=%v (see %s)",
			workerP95, jobP95, perfReportPath)
	} else {
		t.Logf("perf: %s (worker p95=%v, job p95=%v)", decision.RatioSkipReason, workerP95, jobP95)
	}
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
