package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

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

func TestGetJobMeta_ReturnsLabelsAndAnnotations(t *testing.T) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-job", Namespace: "default",
			Labels:      map[string]string{"mode": "validation", "node-id": "public-orders"},
			Annotations: map[string]string{"continuo.dev/node-id": "public.orders:long+raw"},
		},
	}
	client := &K8sClient{clientset: fake.NewSimpleClientset(job), logger: slog.Default()}

	labels, annotations, err := client.GetJobMeta(context.Background(), "default", "test-job")
	require.NoError(t, err)
	assert.Equal(t, "validation", labels["mode"])
	assert.Equal(t, "public.orders:long+raw", annotations["continuo.dev/node-id"], "raw id from annotation")
}

func TestGetJobMeta_MissingJob_ReturnsEmptyMetaNoError(t *testing.T) {
	// A deleted/TTL-reaped Job must not error here: GetJobStatus already maps
	// NotFound to Failed, and absent metadata routes to the production failure
	// path so the retry/permanent-failure handlers still emit. Erroring would
	// strand the check message as a transient failure with no outbox row.
	client := &K8sClient{clientset: fake.NewSimpleClientset(), logger: slog.Default()}

	labels, annotations, err := client.GetJobMeta(context.Background(), "default", "gone-job")
	require.NoError(t, err, "missing Job is not an error for metadata lookup")
	assert.Empty(t, labels)
	assert.Empty(t, annotations)
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

// TestGetJobStatus_DbtNoop_ReturnsJobStatusFailed verifies that a job which exits 0
// but whose pod logs contain "Nothing to do" is detected as failed, not succeeded.
func TestGetJobStatus_DbtNoop_ReturnsJobStatusFailed(t *testing.T) {
	startTime := metav1.NewTime(time.Date(2026, 4, 21, 11, 23, 0, 0, time.UTC))
	completedAt := metav1.NewTime(time.Date(2026, 4, 21, 11, 24, 0, 0, time.UTC))

	job := &batchv1.Job{
		TypeMeta:   metav1.TypeMeta{Kind: "Job", APIVersion: "batch/v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "default"},
		Status: batchv1.JobStatus{
			Succeeded:      1,
			StartTime:      &startTime,
			CompletionTime: &completedAt,
		},
	}
	pod := &corev1.Pod{
		TypeMeta:   metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job-pod",
			Namespace: "default",
			Labels:    map[string]string{"job-name": "test-job"},
		},
	}
	noopLog := "12:00:00  Running with dbt=1.9.0\n12:00:01  Nothing to do. Try checking your model configs.\n"

	client := newClientServingLogs(t, job, pod, noopLog)

	result, err := client.GetJobStatus(context.Background(), "default", "test-job")
	require.NoError(t, err)
	assert.Equal(t, model.JobStatusFailed, result.Status)
	assert.Contains(t, result.TerminationMsg, "dbt matched no models")
}

// TestGetJobStatus_DbtSuccess_ReturnsJobStatusSucceeded ensures a normally-completed
// job (exits 0, logs show model ran) is not incorrectly flagged by the noop check.
func TestGetJobStatus_DbtSuccess_ReturnsJobStatusSucceeded(t *testing.T) {
	startTime := metav1.NewTime(time.Date(2026, 4, 21, 11, 23, 0, 0, time.UTC))
	completedAt := metav1.NewTime(time.Date(2026, 4, 21, 11, 24, 0, 0, time.UTC))

	job := &batchv1.Job{
		TypeMeta:   metav1.TypeMeta{Kind: "Job", APIVersion: "batch/v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "default"},
		Status: batchv1.JobStatus{
			Succeeded:      1,
			StartTime:      &startTime,
			CompletionTime: &completedAt,
		},
	}
	pod := &corev1.Pod{
		TypeMeta:   metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job-pod",
			Namespace: "default",
			Labels:    map[string]string{"job-name": "test-job"},
		},
	}
	successLog := "12:00:00  Running with dbt=1.9.0\n" +
		"12:00:05  1 of 1 START sql table model e2e_schema.table_a [RUN]\n" +
		"12:00:10  1 of 1 OK created sql table model e2e_schema.table_a [SELECT 1 in 5.00s]\n" +
		"12:00:10  Finished running 1 table model in 0 hours 0 minutes and 5.00 seconds.\n"

	client := newClientServingLogs(t, job, pod, successLog)

	result, err := client.GetJobStatus(context.Background(), "default", "test-job")
	require.NoError(t, err)
	assert.Equal(t, model.JobStatusSucceeded, result.Status)
}

// newClientServingLogs creates a K8sClient backed by an httptest.Server that serves
// canned responses for job get, pod list, and pod log endpoints. This is needed because
// fake.NewSimpleClientset does not support the streaming log API.
func newClientServingLogs(t *testing.T, job *batchv1.Job, pod *corev1.Pod, logBody string) *K8sClient {
	t.Helper()

	mux := http.NewServeMux()

	mux.HandleFunc(fmt.Sprintf("/apis/batch/v1/namespaces/%s/jobs/%s", job.Namespace, job.Name),
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			data, _ := json.Marshal(job)
			_, _ = w.Write(data)
		})

	mux.HandleFunc(fmt.Sprintf("/api/v1/namespaces/%s/pods", pod.Namespace),
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			list := &corev1.PodList{
				TypeMeta: metav1.TypeMeta{Kind: "PodList", APIVersion: "v1"},
				Items:    []corev1.Pod{*pod},
			}
			data, _ := json.Marshal(list)
			_, _ = w.Write(data)
		})

	mux.HandleFunc(fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/log", pod.Namespace, pod.Name),
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte(logBody))
		})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cfg := &rest.Config{Host: srv.URL}
	cs, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)

	return &K8sClient{clientset: cs, logger: slog.Default()}
}
