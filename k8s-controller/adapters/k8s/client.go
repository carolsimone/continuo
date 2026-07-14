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
				Status:         model.JobStatusFailed,
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
		// dbt exits 0 when no models match the selector ("Nothing to do").
		// The k8s Job controller marks the pod Succeeded, but nothing was actually
		// executed — detect it via log tail so the run fails cleanly instead of
		// silently reporting a false success.
		if reason := c.checkDbtNoop(ctx, namespace, jobName); reason != "" {
			result.Status = model.JobStatusFailed
			result.TerminationMsg = reason
		} else {
			result.Status = model.JobStatusSucceeded
		}
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

// GetJobMeta returns the labels and annotations stamped on a Job in one API call.
// The terminal-status handler reads the `mode` label to decide whether a Job is a
// validation Job (emitting a single validation_node_completed row) or a production
// task Job (emitting the three production task-status rows). For validation Jobs it
// reads the RAW release/node identity from the annotations — labels are sanitized
// (charset + 63-char limit) and would desync the executor's outcome lookup.
func (c *K8sClient) GetJobMeta(ctx context.Context, namespace, jobName string) (labels, annotations map[string]string, err error) {
	job, err := c.clientset.BatchV1().Jobs(namespace).Get(ctx, jobName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			// The Job is gone (deleted, or TTL-reaped). There is no metadata to
			// route on, so return empty maps rather than an error: GetJobStatus
			// has already mapped NotFound to JobStatusFailed, and absent labels
			// route to the production failure path so the existing retry/
			// permanent-failure handlers still emit their outbox rows. Failing
			// here instead would strand the check message as a transient error.
			return map[string]string{}, map[string]string{}, nil
		}
		return nil, nil, fmt.Errorf("get job meta: %w", err)
	}
	return job.Labels, job.Annotations, nil
}

// GetPodLogs fetches logs for the first pod of a completed job.
// Returns both the full log and the last tailLines lines.
// Returns empty strings if no pod is found or no logs are available.
//
// When the main container produced no output because a preceding init container
// failed (e.g. the compile Job's `compile` init container), the function falls
// back to reading the first failed init container's logs so the error message
// reaches the classifier instead of being silently swallowed. Validation Jobs
// have no init container; this fallback applies only to the compile leg.
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

	pod := pods.Items[0]
	podName := pod.Name

	// Fetch full log from the main (default) container.
	fullLog, err = c.streamPodLogs(ctx, namespace, podName, nil, "")
	if err != nil {
		c.logger.Warn("Failed to stream full pod log", "pod", podName, "error", err)
		fullLog = ""
	}

	// Fetch tail from the main container.
	tail, err = c.streamPodLogs(ctx, namespace, podName, &tailLines, "")
	if err != nil {
		c.logger.Warn("Failed to stream pod log tail", "pod", podName, "error", err)
		tail = ""
	}

	// When the main container produced no output (it never started because an init
	// container failed), fall back to that init container's logs. This surfaces error
	// messages such as "compile: S3 upload ... failed" to the classifier.
	// Normal runs are unaffected because fullLog is non-empty for them.
	if fullLog == "" {
		if initName := pickInitContainerLog(pod); initName != "" {
			if initFull, ferr := c.streamPodLogs(ctx, namespace, podName, nil, initName); ferr == nil && initFull != "" {
				fullLog = initFull
				if initTail, terr := c.streamPodLogs(ctx, namespace, podName, &tailLines, initName); terr == nil {
					tail = initTail
				}
			}
		}
	}

	return fullLog, tail, nil
}

// pickInitContainerLog returns the name of the first init container that terminated
// with a non-zero exit code, or empty string if none failed. Used by GetPodLogs to
// select the fallback log source when the main container never ran.
func pickInitContainerLog(pod corev1.Pod) string {
	for _, ics := range pod.Status.InitContainerStatuses {
		if ics.State.Terminated != nil && ics.State.Terminated.ExitCode != 0 {
			return ics.Name
		}
	}
	return ""
}

// streamPodLogs reads logs for a single container in a pod. When containerName is
// empty, Kubernetes returns the default (first) container's logs. Pass a non-empty
// containerName to read a specific init or sidecar container.
func (c *K8sClient) streamPodLogs(ctx context.Context, namespace, podName string, tailLines *int64, containerName string) (string, error) {
	opts := &corev1.PodLogOptions{}
	if tailLines != nil {
		opts.TailLines = tailLines
	}
	if containerName != "" {
		opts.Container = containerName
	}
	req := c.clientset.CoreV1().Pods(namespace).GetLogs(podName, opts)
	stream, err := req.Stream(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to open log stream: %w", err)
	}
	defer func() { _ = stream.Close() }()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, stream); err != nil {
		return "", fmt.Errorf("failed to read log stream: %w", err)
	}
	return strings.TrimRight(buf.String(), "\n"), nil
}

// checkDbtNoop detects when dbt exits 0 having matched no models.
// dbt emits "Nothing to do" when the selector finds no enabled nodes — the
// pod exits 0 so the k8s Job marks it Succeeded, but no model was run and
// no table was created. Returns a non-empty reason string when detected.
func (c *K8sClient) checkDbtNoop(ctx context.Context, namespace, jobName string) string {
	_, tail, err := c.GetPodLogs(ctx, namespace, jobName, 5)
	if err != nil || tail == "" {
		return ""
	}
	if strings.Contains(tail, "Nothing to do") {
		return "dbt matched no models for this selector — model may be missing from the service image"
	}
	return ""
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

// checkImagePullError inspects pod container statuses for image pull failure reasons.
// The k8s Job controller never marks Status.Failed for stuck image pulls, so we detect
// them directly from pod state. Both main containers and init containers are checked —
// a broken init-container image (e.g. the compile Job's `compile` init container) would
// otherwise hang a release in "validating" forever because Job.Status.Failed stays zero.
// Returns reason+message if found, empty strings otherwise.
func (c *K8sClient) checkImagePullError(ctx context.Context, namespace, jobName string) (reason, message string) {
	pods, err := c.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", jobName),
	})
	if err != nil || len(pods.Items) == 0 {
		return "", ""
	}
	for _, pod := range pods.Items {
		// Check both regular containers and init containers: an unpullable image on
		// either kind keeps Job.Status.Failed at zero while the pod loops indefinitely.
		allStatuses := append(pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses...) //nolint:gocritic
		for _, cs := range allStatuses {
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
