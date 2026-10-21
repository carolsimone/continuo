package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

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
