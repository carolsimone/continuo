package k8s

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/carolsimone/continuo/k8s-controller/domain/model"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// K8sClient provides methods to interact with Kubernetes
type K8sClient struct {
	clientset kubernetes.Interface
	logger    *slog.Logger
}

// NewK8sClient creates a new K8sClient.
// Uses KUBECONFIG when set (local/docker-compose), otherwise falls back to
// in-cluster config (K8s pod with a ServiceAccount).
func NewK8sClient(logger *slog.Logger) (*K8sClient, error) {
	var config *rest.Config
	var err error

	if kubeconfigPath := os.Getenv("KUBECONFIG"); kubeconfigPath != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return nil, fmt.Errorf("failed to build config from kubeconfig: %w", err)
		}
	} else {
		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to build in-cluster config: %w", err)
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		logger.Error("Failed to create K8s clientset", "error", err)
		return nil, fmt.Errorf("failed to create k8s clientset: %w", err)
	}

	logger.Info("K8s client initialized successfully")

	return &K8sClient{
		clientset: clientset,
		logger:    logger,
	}, nil
}

// GetJobStatus queries K8s API for job/pod status and returns detailed status information
func (c *K8sClient) GetJobStatus(ctx context.Context, namespace, jobName string) (*model.K8sPodResult, error) {
	// Step 1: Get Job
	job, err := c.clientset.BatchV1().Jobs(namespace).Get(ctx, jobName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			c.logger.Warn("Job not found",
				"namespace", namespace,
				"job_name", jobName,
			)
			return &model.K8sPodResult{
				Status:         model.JobStatusUnknown,
				TerminationMsg: "Job not found in Kubernetes",
			}, nil
		}
		c.logger.Error("Failed to get job",
			"namespace", namespace,
			"job_name", jobName,
			"error", err,
		)
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	// Step 2: Determine job status from job.Status
	result := &model.K8sPodResult{}

	if job.Status.Succeeded > 0 {
		result.Status = model.JobStatusSucceeded
	} else if job.Status.Failed > 0 || c.hasFailedCondition(job) {
		result.Status = model.JobStatusFailed
	} else if reason, msg := c.checkImagePullError(ctx, namespace, jobName); reason != "" {
		// Pod is stuck waiting for an image that will never arrive. The k8s Job
		// controller never increments Status.Failed for image pull loops — detect
		// it via pod ContainerStatus.Waiting.Reason so the run can fail cleanly.
		result.Status = model.JobStatusFailed
		result.TerminationMsg = fmt.Sprintf("%s: %s", reason, msg)
	} else {
		// Active > 0 means running; Active == 0 with no success/failure means
		// the job is still pending (being scheduled) — treat as running so a
		// delayed re-check is scheduled rather than treating it as unknown/failed.
		result.Status = model.JobStatusRunning
	}

	// Step 3: For failed or succeeded jobs, get pod details for timing and error info
	if result.Status == model.JobStatusFailed || result.Status == model.JobStatusSucceeded {
		// Use job-level times as the baseline — they persist on the Job object
		// even after pods are garbage-collected.
		if job.Status.StartTime != nil {
			t := job.Status.StartTime.Time
			result.StartedAt = &t
		}
		if job.Status.CompletionTime != nil {
			t := job.Status.CompletionTime.Time
			result.CompletedAt = &t
		}

		pods, err := c.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("job-name=%s", jobName),
		})
		if err != nil {
			c.logger.Error("Failed to list pods for job",
				"namespace", namespace,
				"job_name", jobName,
				"error", err,
			)
			return result, nil
		}

		if len(pods.Items) > 0 {
			pod := pods.Items[0]

			// Pod-level start time overrides job-level when available.
			if pod.Status.StartTime != nil {
				t := pod.Status.StartTime.Time
				result.StartedAt = &t
			}

			for _, cs := range pod.Status.ContainerStatuses {
				if cs.State.Terminated != nil {
					terminated := cs.State.Terminated
					result.ExitCode = &terminated.ExitCode
					result.TerminationMsg = terminated.Message
					if terminated.Reason != "" {
						if result.TerminationMsg == "" {
							result.TerminationMsg = terminated.Reason
						} else {
							result.TerminationMsg = fmt.Sprintf("%s: %s", terminated.Reason, result.TerminationMsg)
						}
					}

					completedTime := terminated.FinishedAt.Time
					result.CompletedAt = &completedTime

					// Container-level StartedAt is the most precise; use it when set.
					if !terminated.StartedAt.IsZero() {
						t := terminated.StartedAt.Time
						result.StartedAt = &t
					}

					if result.StartedAt != nil {
						result.ExecutionSeconds = completedTime.Sub(*result.StartedAt).Seconds()
					}
					break
				}
			}
		}

		// When pods were GC'd before polling, compute duration from the job-level
		// timestamps that were set as the baseline above.
		if result.ExecutionSeconds == 0 && result.StartedAt != nil && result.CompletedAt != nil {
			result.ExecutionSeconds = result.CompletedAt.Sub(*result.StartedAt).Seconds()
		}
	}

	c.logger.Debug("Got job status",
		"namespace", namespace,
		"job_name", jobName,
		"status", result.Status,
	)

	return result, nil
}

// GetPodLogs fetches logs for the first pod of a completed job.
// Returns both the full log and the last tailLines lines.
// Returns empty strings if no pod is found or no logs are available.
func (c *K8sClient) GetPodLogs(ctx context.Context, namespace, jobName string, tailLines int64) (fullLog, tail string, err error) {
	pods, err := c.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", jobName),
	})
	if err != nil {
		return "", "", fmt.Errorf("failed to list pods for job %s: %w", jobName, err)
	}
	if len(pods.Items) == 0 {
		c.logger.Warn("No pods found for job", "job_name", jobName)
		return "", "", nil
	}

	podName := pods.Items[0].Name

	// Fetch full log
	fullLog, err = c.streamPodLogs(ctx, namespace, podName, nil)
	if err != nil {
		c.logger.Warn("Failed to stream full pod log", "pod", podName, "error", err)
		fullLog = ""
	}

	// Fetch tail
	tail, err = c.streamPodLogs(ctx, namespace, podName, &tailLines)
	if err != nil {
		c.logger.Warn("Failed to stream pod log tail", "pod", podName, "error", err)
		tail = ""
	}

	return fullLog, tail, nil
}

func (c *K8sClient) streamPodLogs(ctx context.Context, namespace, podName string, tailLines *int64) (string, error) {
	opts := &corev1.PodLogOptions{}
	if tailLines != nil {
		opts.TailLines = tailLines
	}
	req := c.clientset.CoreV1().Pods(namespace).GetLogs(podName, opts)
	stream, err := req.Stream(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to open log stream: %w", err)
	}
	defer stream.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, stream); err != nil {
		return "", fmt.Errorf("failed to read log stream: %w", err)
	}
	return strings.TrimRight(buf.String(), "\n"), nil
}

// hasFailedCondition checks if the job has a failed condition
func (c *K8sClient) hasFailedCondition(job *batchv1.Job) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// checkImagePullError inspects pod ContainerStatus.Waiting for image pull failure reasons.
// The k8s Job controller never marks Status.Failed for stuck image pulls, so we detect
// them directly from pod state. Returns reason+message if found, empty strings otherwise.
func (c *K8sClient) checkImagePullError(ctx context.Context, namespace, jobName string) (reason, message string) {
	pods, err := c.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", jobName),
	})
	if err != nil || len(pods.Items) == 0 {
		return "", ""
	}
	for _, pod := range pods.Items {
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.State.Waiting != nil {
				switch cs.State.Waiting.Reason {
				case "ImagePullBackOff", "ErrImagePull", "InvalidImageName":
					msg := cs.State.Waiting.Message
					if msg == "" {
						msg = cs.State.Waiting.Reason
					}
					return cs.State.Waiting.Reason, msg
				}
			}
		}
	}
	return "", ""
}
