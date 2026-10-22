package watch

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
