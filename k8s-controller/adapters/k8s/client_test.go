package k8s

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/carolsimone/continuo/k8s-controller/domain/model"
)

func TestGetJobStatus_StartedAt_FromJobWhenPodsGone(t *testing.T) {
	startTime := metav1.NewTime(time.Date(2026, 4, 21, 11, 23, 0, 0, time.UTC))
	completedAt := metav1.NewTime(time.Date(2026, 4, 21, 11, 23, 30, 0, time.UTC))

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "default"},
		Status: batchv1.JobStatus{
			Succeeded:      1,
			StartTime:      &startTime,
			CompletionTime: &completedAt,
		},
	}

	fakeClient := fake.NewSimpleClientset(job) // no pods registered
	client := &K8sClient{clientset: fakeClient, logger: slog.Default()}

	result, err := client.GetJobStatus(context.Background(), "default", "test-job")
	require.NoError(t, err)
	assert.Equal(t, model.JobStatusSucceeded, result.Status)
	require.NotNil(t, result.StartedAt, "StartedAt must be set from job.Status.StartTime even when no pods exist")
	assert.Equal(t, startTime.Time.UTC(), result.StartedAt.UTC())
}

func TestGetJobStatus_ImagePullBackOff_ReturnsJobStatusFailed(t *testing.T) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "default"},
		Status: batchv1.JobStatus{
			Active: 1, // pod is "active" — k8s Job never increments Status.Failed
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job-abc",
			Namespace: "default",
			Labels:    map[string]string{"job-name": "test-job"},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "dbt-job",
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason:  "ImagePullBackOff",
							Message: "Back-off pulling image \"carolsimone/service-1:latest\"",
						},
					},
				},
			},
		},
	}

	fakeClient := fake.NewSimpleClientset(job, pod)
	client := &K8sClient{clientset: fakeClient, logger: slog.Default()}

	result, err := client.GetJobStatus(context.Background(), "default", "test-job")
	require.NoError(t, err)
	assert.Equal(t, model.JobStatusFailed, result.Status)
	assert.Contains(t, result.TerminationMsg, "ImagePullBackOff")
}
