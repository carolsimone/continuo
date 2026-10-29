package e2e

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const (
	// missingImageTable is a node the missing-image test adds to the default
	// DAG. Its image tag names an image no registry holds, so no attempt of it
	// reaches a container and no dbt project needs a model of it.
	missingImageTable = "missing_image_probe"
	// missingImageSchedule is that node's schedule and nothing else's.
	missingImageSchedule = "e2e-missing-image"

	// startTimeoutTable is the default DAG's node the start-timeout test runs:
	// a model that succeeds whenever its pod starts.
	startTimeoutTable = "ftable_a"

	// noPodsQuota is the ResourceQuota the start-timeout test applies to the
	// namespace the Jobs run in. deploy-k8s-controllers.sh,
	// provision-k8s-test-env.sh and cleanup-k8s-controllers.sh delete it too,
	// in case a killed test run left it behind.
	noPodsQuota = "e2e-no-pods"
)

// TestE2E_StartFailure_AMissingImageIsRetriedWithoutOverlap proves that a start
// failure is retried until the task's budget is spent, and that an attempt's
// Job is created only once no pod of the attempt before it can run:
//
//	missing_image_probe's image tag names an image that does not exist
//	→ TriggerSingleNodeRun → state dispatches attempt 0
//	→ its pod cannot pull the image (ErrImagePull, ImagePullBackOff)
//	→ execution-controller deletes the Job, waits for its pod to stop and
//	  reports attempt 0 failed (start, image_pull)
//	→ state dispatches attempt 1, and so on up to max_retries
//	→ the last failure fails the task and the run.
//
// While the run goes on, the test lists the task's Jobs and then its pods once
// or twice a second: once the Job, or a pod, of an attempt has been listed, no
// pod of an earlier attempt may be listed unless it has stopped for good.
func TestE2E_StartFailure_AMissingImageIsRetriedWithoutOverlap(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// Topology (2m) + max_retries + 1 failed pulls on a cold stack (12m) + the
	// run's finalization, with headroom.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	cleanupTestData(t, ctx, clients, "single-node-run")

	// The default DAG plus the node whose image does not exist. The default DAG
	// is announced again at the end, so no later test sees the node.
	nodes, schedules := e2eTopology(t, ctx, clients)
	missingTag := "e2e-missing-" + uuid.NewString()[:8]
	nodes = append(nodes, topologyartifact.Node{
		UniqueID:           seedSchemaName + "." + missingImageTable,
		SchemaName:         seedSchemaName,
		TableName:          missingImageTable,
		ResolvedRelationID: seedSchemaName + "." + missingImageTable,
		ServiceName:        "service-1",
		NodeType:           "dbt-model",
		ImageTag:           missingTag,
		Schedule:           missingImageSchedule,
		UpstreamUniqueIDs:  []string{},
	})
	announced := announceTopology(t, ctx, "e2e-missing-image-"+uuid.NewString()[:8], nodes)
	defer func() {
		restoreCtx, restoreCancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer restoreCancel()
		waitForStateLiveRelease(t, restoreCtx, clients, seedTopology(t, restoreCtx, clients))
	}()
	waitForAnnouncedTopology(t, ctx, clients, announced, append(schedules, missingImageSchedule))
	waitForStateLiveRelease(t, ctx, clients, announced)

	resp, err := clients.runClient.TriggerSingleNodeRun(ctx, &runv1.TriggerSingleNodeRunRequest{RequestId: uuid.NewString(),
		ServiceName:    "service-1",
		SchemaName:     seedSchemaName,
		TableName:      missingImageTable,
		MetadataSource: "latest",
	})
	require.NoError(t, err, "TriggerSingleNodeRun(%s)", missingImageTable)
	runID, err := uuid.Parse(resp.RunId)
	require.NoError(t, err, "TriggerSingleNodeRun must return the run's id")
	defer cleanupSingleNodeRun(t, ctx, clients, runID, resp.ScheduleName)
	t.Logf("run %s of %s (image tag %s) triggered", runID, missingImageTable, missingTag)
	taskID := taskOfRun(t, ctx, clients, runID, missingImageTable)

	// latest is the highest attempt whose Job or pod has been listed so far; a
	// pod exists only once its attempt's Job does. The Jobs are listed before
	// the pods, so a pod of an earlier attempt listed in the same pass, or in
	// any later one, existed after that attempt's Job did. Such a pod overlaps
	// the later attempt unless it can never run again: its phase is Succeeded
	// or Failed, which is how the kubelet leaves a stopped pod until its object
	// is removed.
	selector := "task-id=" + taskID.String()
	latest := -1
	observed := map[int]bool{}
	var overlaps []string
	pollUntil(t, ctx, 12*time.Minute, 500*time.Millisecond, func() (bool, error) {
		jobs, err := getK8sJobs(ctx, selector)
		if err != nil {
			return false, err
		}
		pods, err := getK8sPods(ctx, selector)
		if err != nil {
			return false, err
		}
		for _, job := range jobs.Items {
			n := attemptLabel(job.Metadata.Labels)
			observed[n] = true
			latest = max(latest, n)
		}
		for _, pod := range pods.Items {
			n := attemptLabel(pod.Metadata.Labels)
			observed[n] = true
			latest = max(latest, n)
		}
		for _, pod := range pods.Items {
			n := attemptLabel(pod.Metadata.Labels)
			if n < latest && pod.Status.Phase != "Succeeded" && pod.Status.Phase != "Failed" {
				overlaps = append(overlaps, fmt.Sprintf("pod %s of attempt %d (phase %s) exists after attempt %d's Job was created",
					pod.Metadata.Name, n, pod.Status.Phase, latest))
			}
		}
		return taskEnded(ctx, clients, taskID)
	}, fmt.Sprintf("Timeout waiting for the task of %s to end", missingImageTable))

	ended := waitForTaskEnded(t, ctx, clients, taskID, missingImageTable, time.Minute)
	require.Positive(t, ended.maxRetries, "the e2e stack gives every task retries, so attempts can follow one another")
	want := make([]string, 0, ended.maxRetries+1)
	for n := 0; n <= ended.maxRetries; n++ {
		want = append(want, fmt.Sprintf("%d:failed:%s:%s", n, model.FailureClassStart, model.FailureReasonImagePull))
	}
	require.Equal(t, want, ended.attempts, "every attempt the budget allows fails to pull its image")
	require.Equal(t, "failed", ended.status)
	require.Equal(t, ended.maxRetries, ended.attempt, "the task fails at its last allowed attempt")

	require.Empty(t, overlaps, "no pod of an attempt that can still run may exist once the next attempt's Job does")
	for n := 1; n <= ended.maxRetries; n++ {
		require.True(t, observed[n],
			"neither attempt %d's Job nor a pod of it was ever listed, so its overlap with attempt %d went unchecked", n, n-1)
	}
	verifySchedulerFailed(t, ctx, clients, runID)
	t.Logf("✅ %s failed all %d attempts on its missing image, none overlapping the one before",
		missingImageTable, ended.maxRetries+1)
}

// TestE2E_StartFailure_APodThatNeverStartsTimesOutWithoutARetry proves the
// start timeout: an attempt whose pod never starts a container ends once the
// start timeout has passed since its Job was created, and is not retried.
//
//	a ResourceQuota of zero pods in the Jobs' namespace keeps every new pod
//	from being created
//	→ TriggerSingleNodeRun(ftable_a) → its attempt 0's Job is created but gets
//	  no pod
//	→ at the start timeout execution-controller deletes the Job and reports
//	  attempt 0 failed (timeout, start_timeout)
//	→ state fails the task without a retry and fails the run.
//
// The quota blocks the creation of every pod in the namespace while it stands,
// so this test must never run in parallel with another; it removes the quota
// as soon as the attempt has ended, and again when the test ends however it
// ends.
func TestE2E_StartFailure_APodThatNeverStartsTimesOutWithoutARetry(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// Topology (2m) + the start timeout + the report (5m) + the run's
	// finalization, with headroom.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	cleanupTestData(t, ctx, clients, "single-node-run")
	startTimeout := jobStartTimeout(t, ctx)

	waitForStateLiveRelease(t, ctx, clients, seedTopology(t, ctx, clients))

	blockPodCreation(t, ctx)

	resp, err := clients.runClient.TriggerSingleNodeRun(ctx, &runv1.TriggerSingleNodeRunRequest{RequestId: uuid.NewString(),
		ServiceName:    "service-1",
		SchemaName:     seedSchemaName,
		TableName:      startTimeoutTable,
		MetadataSource: "latest",
	})
	require.NoError(t, err, "TriggerSingleNodeRun(%s)", startTimeoutTable)
	runID, err := uuid.Parse(resp.RunId)
	require.NoError(t, err, "TriggerSingleNodeRun must return the run's id")
	defer cleanupSingleNodeRun(t, ctx, clients, runID, resp.ScheduleName)
	t.Logf("run %s of %s triggered with pod creation blocked", runID, startTimeoutTable)
	taskID := taskOfRun(t, ctx, clients, runID, startTimeoutTable)

	job := waitForAttemptJob(t, ctx, taskID, 0, 2*time.Minute)

	// No pod of the task may appear while the quota stands: one would mean the
	// attempt could start, and the test would prove nothing about the timeout.
	selector := "task-id=" + taskID.String()
	var pods []string
	pollUntil(t, ctx, startTimeout+5*time.Minute, 2*time.Second, func() (bool, error) {
		list, err := getK8sPods(ctx, selector)
		if err != nil {
			return false, err
		}
		for _, pod := range list.Items {
			pods = append(pods, pod.Metadata.Name)
		}
		return taskEnded(ctx, clients, taskID)
	}, fmt.Sprintf("Timeout waiting for the task of %s to end", startTimeoutTable))
	unblockPodCreation(t)
	require.Empty(t, pods, "the quota keeps the attempt's pod from being created")

	ended := waitForTaskEnded(t, ctx, clients, taskID, startTimeoutTable, time.Minute)
	require.Equal(t, []string{
		fmt.Sprintf("0:failed:%s:%s", model.FailureClassTimeout, model.FailureReasonStartTimeout),
	}, ended.attempts, "attempt 0 times out before starting and no attempt follows it")
	require.Equal(t, "failed", ended.status)
	require.Equal(t, 0, ended.attempt)

	// It failed once the start timeout had passed since its Job was created.
	elapsed := attemptReportedAt(t, ctx, clients, taskID, 0).Sub(job.Metadata.CreationTimestamp)
	t.Logf("attempt 0 reported failed %s after its Job was created (start timeout %s)", elapsed.Round(time.Second), startTimeout)
	require.GreaterOrEqual(t, elapsed, startTimeout-clockTolerance,
		"the attempt must not time out before the start timeout")
	require.Less(t, elapsed, startTimeout+2*time.Minute,
		"the attempt ends soon after the start timeout")

	verifySchedulerFailed(t, ctx, clients, runID)
	t.Logf("✅ %s timed out before starting, after %s, without a retry", startTimeoutTable, elapsed.Round(time.Second))
}

// blockPodCreation applies a ResourceQuota of zero pods to the namespace the
// Jobs run in, so no new pod can be created there, and waits until the quota
// controller has recorded its limit. The quota is removed when the test ends.
func blockPodCreation(t *testing.T, ctx context.Context) {
	t.Helper()
	t.Cleanup(func() { unblockPodCreation(t) })
	quota := fmt.Sprintf(`apiVersion: v1
kind: ResourceQuota
metadata:
  name: %s
  namespace: default
spec:
  hard:
    pods: "0"
`, noPodsQuota)
	apply := exec.CommandContext(ctx, "kubectl", "apply", "-f", "-")
	apply.Stdin = strings.NewReader(quota)
	out, err := apply.CombinedOutput()
	require.NoError(t, err, "apply ResourceQuota %s: %s", noPodsQuota, out)
	pollUntil(t, ctx, time.Minute, time.Second, func() (bool, error) {
		hard, err := exec.CommandContext(ctx, "kubectl", "get", "resourcequota", noPodsQuota,
			"-n", "default", "-o", "jsonpath={.status.hard.pods}").Output()
		if err != nil {
			return false, err
		}
		return strings.TrimSpace(string(hard)) == "0", nil
	}, fmt.Sprintf("Timeout waiting for ResourceQuota %s to take effect", noPodsQuota))
	t.Logf("ResourceQuota %s forbids new pods in the default namespace", noPodsQuota)
}

// unblockPodCreation deletes the ResourceQuota blockPodCreation applied; an
// absent quota is not an error. A quota left behind would keep every later
// test's pods from being created, so a failure to delete it fails the test.
func unblockPodCreation(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", "delete", "resourcequota", noPodsQuota,
		"-n", "default", "--ignore-not-found").CombinedOutput()
	if err != nil {
		t.Errorf("delete ResourceQuota %s: %v: %s", noPodsQuota, err, out)
	}
}
