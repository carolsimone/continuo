package watch

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/adapters/commandcfg"
	"github.com/carolsimone/continuo/execution-controller/adapters/k8s"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestObservation_MapsTheJobAndItsPods(t *testing.T) {
	j := job("j")
	deleting := metav1.NewTime(time.Unix(300, 0))
	j.DeletionTimestamp = &deleting
	j.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded"},
		{Type: batchv1.JobComplete, Status: corev1.ConditionFalse},
	}
	p := pod("p", "j")
	p.Status.Phase = corev1.PodFailed
	p.Status.Reason = "Evicted"
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "dbt-job", State: corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled", Message: "oom",
			StartedAt: metav1.NewTime(time.Unix(100, 0)), FinishedAt: metav1.NewTime(time.Unix(130, 0))}}}}
	p.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "hydrate-parse-cache", State: corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "back-off"}}}}

	obs := observation(j, []*corev1.Pod{p})

	assert.True(t, obs.JobPresent)
	assert.Equal(t, "t1", obs.JobTaskID, "the Job's task-id label")
	assert.True(t, obs.JobDeleting)
	assert.True(t, obs.JobFailed, "FailureTarget counts as the Job's failure")
	assert.Equal(t, "DeadlineExceeded", obs.JobFailureReason)
	assert.False(t, obs.JobSucceeded, "a condition that is not true counts for nothing")
	require.Len(t, obs.Pods, 1)
	got := obs.Pods[0]
	assert.Equal(t, model.PodFailed, got.Phase)
	assert.Equal(t, "Evicted", got.Reason)
	assert.True(t, got.Main.Terminated)
	assert.Equal(t, int32(137), got.Main.ExitCode)
	assert.Equal(t, "OOMKilled", got.Main.TerminatedReason)
	assert.Equal(t, time.Unix(100, 0).UTC(), got.Main.StartedAt.UTC())
	assert.Equal(t, time.Unix(130, 0).UTC(), got.Main.FinishedAt.UTC())
	require.Len(t, got.Inits, 1)
	assert.Equal(t, "ImagePullBackOff", got.Inits[0].WaitingReason)
}

func TestObservation_NoJobAndPodsInNameOrder(t *testing.T) {
	obs := observation(nil, []*corev1.Pod{pod("b", "j"), pod("a", "j")})
	assert.False(t, obs.JobPresent)
	require.Len(t, obs.Pods, 2)
	assert.Equal(t, "a", obs.Pods[0].Name)
	assert.Equal(t, "b", obs.Pods[1].Name)
}

// A Job runs the attempt its task-id and attempt labels name.
func TestObservation_TheJobsLabelsNameItsAttempt(t *testing.T) {
	j := job("j")
	j.Labels["attempt"] = "2"

	obs := observation(j, nil)

	assert.True(t, obs.RunsAttempt("t1", 2))
	assert.False(t, obs.RunsAttempt("t1", 1))
	assert.False(t, obs.RunsAttempt("t2", 2))
	assert.False(t, observation(nil, nil).RunsAttempt("t1", 2), "no Job runs no attempt")
}

// The Job the Kubernetes adapter creates for an attempt is observed as
// running that attempt: the labels the adapter writes are the ones the watch
// reads.
func TestObservation_TheJobCreatedForAnAttemptRunsIt(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "warehouse-conn")
	t.Setenv("DOCKERHUB_USERNAME", "")
	cs := fake.NewSimpleClientset()
	client := k8s.NewK8sClientFor(cs, slog.New(slog.NewTextHandler(os.Stderr, nil)), commandcfg.Defaults(), 30*time.Minute)
	taskID := "3e8d7c6b-5a49-4382-9170-6f5e4d3c2b1a"
	require.NoError(t, client.CreateQueryJob(context.Background(), k8s.JobParams{
		JobName: "service-1-e2e-schema-ftable-e-7f3e1a2b-r2", TaskID: taskID,
		RunID: "7f3e1a2b-4c5d-4e6f-8a9b-0c1d2e3f4a5b", ScheduleName: "e2e-schedule-failure",
		ServiceName: "service-1", SchemaName: "e2e_schema", TableName: "ftable_e", Namespace: ns,
		NodeType: pkgmodel.NodeTypeDbtModel, ImageTag: "v42", Attempt: 2, ActiveDeadlineSeconds: 4500,
	}))
	created, err := cs.BatchV1().Jobs(ns).Get(context.Background(), "service-1-e2e-schema-ftable-e-7f3e1a2b-r2", metav1.GetOptions{})
	require.NoError(t, err)

	obs := observation(created, nil)

	assert.True(t, obs.RunsAttempt(taskID, 2))
	assert.False(t, obs.RunsAttempt(taskID, 1))
}

// A task's Job names the run it belongs to in its run-id label, the one
// the Kubernetes adapter writes; the observer reads it to tell a Job of a
// fenced run.
func TestObservation_ReadsTheRunIDLabel(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "warehouse-conn")
	t.Setenv("DOCKERHUB_USERNAME", "")
	cs := fake.NewSimpleClientset()
	client := k8s.NewK8sClientFor(cs, slog.New(slog.NewTextHandler(os.Stderr, nil)), commandcfg.Defaults(), 30*time.Minute)
	runID := "7f3e1a2b-4c5d-4e6f-8a9b-0c1d2e3f4a5b"
	require.NoError(t, client.CreateQueryJob(context.Background(), k8s.JobParams{
		JobName: "service-1-e2e-schema-ftable-e-7f3e1a2b", TaskID: "3e8d7c6b-5a49-4382-9170-6f5e4d3c2b1a",
		RunID: runID, ScheduleName: "daily", ServiceName: "service-1", SchemaName: "e2e_schema",
		TableName: "ftable_e", Namespace: ns, NodeType: pkgmodel.NodeTypeDbtModel, ImageTag: "v42",
		ActiveDeadlineSeconds: 4500,
	}))
	created, err := cs.BatchV1().Jobs(ns).Get(context.Background(), "service-1-e2e-schema-ftable-e-7f3e1a2b", metav1.GetOptions{})
	require.NoError(t, err)

	assert.Equal(t, runID, created.Labels["run-id"], "the adapter writes the run under the run-id label")
	assert.Equal(t, runID, observation(created, nil).JobRunID)
	assert.Empty(t, observation(job("j"), nil).JobRunID, "a Job with no run label names no run")
	assert.Empty(t, observation(nil, nil).JobRunID, "no Job names no run")
}
