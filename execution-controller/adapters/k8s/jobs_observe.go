package k8s

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GetPodLogs fetches the log of the first pod of jobName, finished or still
// terminating. Returns both the full log and the last tailLines lines.
// Returns empty strings if no pod is found or no logs are available.
//
// When the main container produced no output because a preceding init container
// failed (e.g. the compile Job's `compile` init container), the function falls
// back to reading the first failed init container's logs so the error message
// reaches the attempt's facts instead of being silently swallowed.
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
	// messages such as "compile: S3 upload ... failed" in the attempt's error message.
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
