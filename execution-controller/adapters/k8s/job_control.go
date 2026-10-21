package k8s

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/execution-controller/service/ports"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// JobControl reads, deletes and reads the pod logs of Jobs in one namespace,
// straight from the Kubernetes API.
type JobControl struct {
	client    *K8sClient
	namespace string
}

var (
	_ ports.JobTerminator = (*JobControl)(nil)
	_ ports.PodLogReader  = (*JobControl)(nil)
)

// NewJobControl binds a JobControl to client and namespace.
func NewJobControl(client *K8sClient, namespace string) *JobControl {
	return &JobControl{client: client, namespace: namespace}
}

// DeleteJob deletes jobName with foreground propagation, so the Job object
// stays until every pod of it is gone. A Job that is gone is not an error.
func (j *JobControl) DeleteJob(ctx context.Context, jobName string) error {
	policy := metav1.DeletePropagationForeground
	err := j.client.clientset.BatchV1().Jobs(j.namespace).Delete(ctx, jobName, metav1.DeleteOptions{PropagationPolicy: &policy})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("delete job %s: %w", jobName, err)
	}
	return nil
}

// GetPodLogs returns jobName's pod log and its last tailLines lines.
func (j *JobControl) GetPodLogs(ctx context.Context, jobName string, tailLines int64) (string, string, error) {
	return j.client.GetPodLogs(ctx, j.namespace, jobName, tailLines)
}
