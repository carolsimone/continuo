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

	result, err := client.GetJobStatus(context.Background(), "default", "test-job", "")
	require.NoError(t, err)
	assert.Equal(t, model.JobStatusSucceeded, result.Status)
	require.NotNil(t, result.StartedAt, "StartedAt must be set from job.Status.StartTime even when no pods exist")
	assert.Equal(t, startTime.UTC(), result.StartedAt.UTC())
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

	result, err := client.GetJobStatus(context.Background(), "default", "test-job", "")
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

	result, err := client.GetJobStatus(context.Background(), "default", "test-job", "")
	require.NoError(t, err)
	assert.Equal(t, model.JobStatusFailed, result.Status)
	assert.Contains(t, result.TerminationMsg, "dbt matched no models")
}

// newDbtNoopJob builds a Job/pod pair that exited 0 with a "Nothing to do" log tail.
func newDbtNoopJob() (*batchv1.Job, *corev1.Pod, string) {
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
		TypeMeta: metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job-pod",
			Namespace: "default",
			Labels:    map[string]string{"job-name": "test-job"},
		},
	}
	noopLog := "12:00:00  Running with dbt=1.9.0\n12:00:01  Nothing to do. Try checking your model configs.\n"
	return job, pod, noopLog
}

// TestGetJobStatus_DbtNoop_TestOperation_ReturnsJobStatusSucceeded verifies that a
// `dbt test` Job which exits 0 with "Nothing to do" (the target node has no tests)
// is a legitimate no-op, not a failure. Unlike a materializing verb, `dbt test`
// producing nothing means "no tests to run", which is success.
func TestGetJobStatus_DbtNoop_TestOperation_ReturnsJobStatusSucceeded(t *testing.T) {
	job, pod, noopLog := newDbtNoopJob()
	client := newClientServingLogs(t, job, pod, noopLog)

	result, err := client.GetJobStatus(context.Background(), "default", "test-job", "test")
	require.NoError(t, err)
	assert.Equal(t, model.JobStatusSucceeded, result.Status)
}

// TestGetJobStatus_DbtNoop_BuildOperation_ReturnsJobStatusFailed guards that the
// no-op exemption is test-only: a materializing verb ("build") that matches no
// models still means the model is missing from the image, which is a failure.
func TestGetJobStatus_DbtNoop_BuildOperation_ReturnsJobStatusFailed(t *testing.T) {
	job, pod, noopLog := newDbtNoopJob()
	client := newClientServingLogs(t, job, pod, noopLog)

	result, err := client.GetJobStatus(context.Background(), "default", "test-job", "build")
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

	result, err := client.GetJobStatus(context.Background(), "default", "test-job", "")
	require.NoError(t, err)
	assert.Equal(t, model.JobStatusSucceeded, result.Status)
}

// TestGetJobStatus_InitContainerImagePullBackOff_ReturnsJobStatusFailed verifies that
// an ImagePullBackOff on an init container (e.g. the compile Job's `compile` init
// container) is correctly surfaced as a failure. The k8s Job controller never
// increments Status.Failed for image pull loops regardless of whether the image pull
// failure is on a main container or an init container.
func TestGetJobStatus_InitContainerImagePullBackOff_ReturnsJobStatusFailed(t *testing.T) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "default"},
		Status: batchv1.JobStatus{
			Active: 1, // k8s never increments Status.Failed for image pull loops
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job-abc",
			Namespace: "default",
			Labels:    map[string]string{"job-name": "test-job"},
		},
		Status: corev1.PodStatus{
			// Main container never starts — only InitContainerStatuses is populated.
			InitContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "fetch",
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason:  "ImagePullBackOff",
							Message: "Back-off pulling image \"carolsimone/s3-sidecar:latest\"",
						},
					},
				},
			},
		},
	}

	fakeClient := fake.NewSimpleClientset(job, pod)
	client := &K8sClient{clientset: fakeClient, logger: slog.Default()}

	result, err := client.GetJobStatus(context.Background(), "default", "test-job", "")
	require.NoError(t, err)
	assert.Equal(t, model.JobStatusFailed, result.Status)
	assert.Contains(t, result.TerminationMsg, "ImagePullBackOff")
}

// TestPickInitContainerLog_ReturnsFailedInitContainerName verifies that the helper
// returns the name of the first init container that terminated with a non-zero exit
// code, so GetPodLogs knows which container log to read as a fallback.
func TestPickInitContainerLog_ReturnsFailedInitContainerName(t *testing.T) {
	pod := corev1.Pod{
		Status: corev1.PodStatus{
			InitContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "fetch",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{ExitCode: 1},
					},
				},
			},
		},
	}
	assert.Equal(t, "fetch", pickInitContainerLog(pod))
}

// TestPickInitContainerLog_ReturnsEmptyWhenNoneFailed verifies that the helper
// returns empty string when all init containers exited cleanly, so GetPodLogs
// does not attempt a spurious fallback on successful validation Jobs.
func TestPickInitContainerLog_ReturnsEmptyWhenNoneFailed(t *testing.T) {
	pod := corev1.Pod{
		Status: corev1.PodStatus{
			InitContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "fetch",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{ExitCode: 0},
					},
				},
			},
		},
	}
	assert.Equal(t, "", pickInitContainerLog(pod))
}

// TestGetPodLogs_FallsBackToInitContainerWhenMainLogEmpty verifies that when the main
// dbt-job container never started (because a preceding init container failed), GetPodLogs
// falls back to reading the failed init container's log instead of returning empty. This
// surfaces error messages such as "compile: S3 upload ... failed" to the classifier
// rather than silently swallowing them.
func TestGetPodLogs_FallsBackToInitContainerWhenMainLogEmpty(t *testing.T) {
	job := &batchv1.Job{
		TypeMeta:   metav1.TypeMeta{Kind: "Job", APIVersion: "batch/v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "default"},
		Status:     batchv1.JobStatus{Failed: 1},
	}

	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job-pod",
			Namespace: "default",
			Labels:    map[string]string{"job-name": "test-job"},
		},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{{Name: "fetch"}},
		},
		Status: corev1.PodStatus{
			InitContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "fetch",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{ExitCode: 1},
					},
				},
			},
		},
	}

	initLog := "compile: S3 upload s3://bucket/manifest.json failed: NoSuchBucket"
	logsByContainer := map[string]string{
		"":      "", // main container: empty (never started)
		"fetch": initLog,
	}

	client := newClientServingLogsPerContainer(t, job, pod, logsByContainer)

	fullLog, _, err := client.GetPodLogs(context.Background(), "default", "test-job", 10)
	require.NoError(t, err)
	assert.Contains(t, fullLog, "S3 upload")
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

// newClientServingLogsPerContainer is like newClientServingLogs but lets callers
// specify different log bodies per container name. The key "" addresses the default
// (main) container; any other key matches the ?container=<name> query parameter set
// by streamPodLogs when reading a named init container. This is required to test
// the init-container log fallback path in GetPodLogs.
func newClientServingLogsPerContainer(t *testing.T, job *batchv1.Job, pod *corev1.Pod, logsByContainer map[string]string) *K8sClient {
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
			container := r.URL.Query().Get("container")
			w.Header().Set("Content-Type", "text/plain")
			if logBody, ok := logsByContainer[container]; ok {
				_, _ = w.Write([]byte(logBody))
			}
			// Unregistered container key: return empty body.
		})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cfg := &rest.Config{Host: srv.URL}
	cs, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)

	return &K8sClient{clientset: cs, logger: slog.Default()}
}
