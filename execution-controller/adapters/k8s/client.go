// Package k8s is the Kubernetes adapter: one clientset creates dbt, python, validation, seed-build, compile and schema-op Jobs (jobs_create.go, schema_ops.go) and observes their status, metadata and pod logs (jobs_observe.go).
package k8s

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"

	"github.com/carolsimone/continuo/execution-controller/service/ports"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// jobTTLSecondsAfterFinished bounds how long a terminal (Succeeded or Failed) dbt
// Job — and its pod — stays on the cluster before Kubernetes garbage-collects it.
// k8s-controller reads and uploads a Job's pod logs to S3 synchronously the moment it
// observes the Job go terminal, so this window only needs to cover ad-hoc
// kubectl-level debugging of a still-present pod, not log retention: the S3 copy is
// durable long before this elapses. 24h matches the other Job TTL backstops in this
// repo (db-init-migrate-job.yaml, minio/bucket-init-job.yaml).
const jobTTLSecondsAfterFinished = int32(86400)

// JobParams represents the parameters needed to create a K8s Job
type JobParams struct {
	JobName      string
	TaskID       string
	ScheduleID   string
	ScheduleName string // used for the schedule label
	ServiceName  string
	SchemaName   string
	TableName    string
	Namespace    string
	NodeType     pkg_model.NodeType
	ImageTag     string
	// Operation selects the dbt verb the executor runs for this node.
	// pkg_model.OperationRun (empty) is the default: dbt run/seed/snapshot by
	// NodeType. pkg_model.OperationTest runs `dbt test --select <node>`.
	Operation pkg_model.Operation
	// Mode is the legacy promote-seed dispatch mode carried by queued work only;
	// when non-empty it is stamped as a "mode" label so k8s-controller routes the
	// Job away from the production lifecycle. Empty for everything current.
	Mode string
}

// K8sClient provides methods to interact with Kubernetes
type K8sClient struct {
	clientset kubernetes.Interface
	logger    *slog.Logger
	commands  ports.CommandResolver
}

// NewK8sClient creates a new K8sClient.
// Uses KUBECONFIG when set (local/docker-compose), otherwise falls back to
// in-cluster config (K8s pod with a ServiceAccount). commands resolves the
// per-service dbt command dialect for every Job this client builds.
func NewK8sClient(logger *slog.Logger, commands ports.CommandResolver) (*K8sClient, error) {
	var restConfig *rest.Config
	var err error

	if kubeconfigPath := os.Getenv("KUBECONFIG"); kubeconfigPath != "" {
		restConfig, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return nil, fmt.Errorf("failed to build config from kubeconfig: %w", err)
		}
	} else {
		restConfig, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to build in-cluster config: %w", err)
		}
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		logger.Error("Failed to create K8s clientset", "error", err)
		return nil, fmt.Errorf("failed to create k8s clientset: %w", err)
	}

	logger.Info("K8s client initialized successfully")

	return &K8sClient{
		clientset: clientset,
		logger:    logger,
		commands:  commands,
	}, nil
}

// JobExists checks if a K8s Job with the given name exists in the namespace
func (c *K8sClient) JobExists(ctx context.Context, namespace, jobName string) (bool, error) {
	_, err := c.clientset.BatchV1().Jobs(namespace).Get(ctx, jobName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		c.logger.Error("Failed to check if job exists",
			"namespace", namespace,
			"job_name", jobName,
			"error", err,
		)
		return false, fmt.Errorf("failed to check if job exists: %w", err)
	}

	return true, nil
}

// CreateJob creates a K8s Job
func (c *K8sClient) CreateJob(ctx context.Context, job *batchv1.Job) error {
	_, err := c.clientset.BatchV1().Jobs(job.Namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		c.logger.Error("Failed to create K8s job",
			"namespace", job.Namespace,
			"job_name", job.Name,
			"error", err,
		)
		return fmt.Errorf("failed to create k8s job: %w", err)
	}

	c.logger.Info("Created K8s job",
		"namespace", job.Namespace,
		"job_name", job.Name,
	)

	return nil
}

func (c *K8sClient) deleteJob(ctx context.Context, namespace, jobName string) error {
	policy := metav1.DeletePropagationBackground
	err := c.clientset.BatchV1().Jobs(namespace).Delete(ctx, jobName, metav1.DeleteOptions{PropagationPolicy: &policy})
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

// CountActiveJobs returns the number of Jobs in the namespace matching
// labelSelector that currently have a running pod (.status.active > 0). Jobs
// that are created but whose pod is still Pending/unscheduled (active == 0) do
// not count. Used by deployer.Dispatcher to enforce the concurrent-Job cap.
func (c *K8sClient) CountActiveJobs(ctx context.Context, namespace, labelSelector string) (int, error) {
	list, err := c.clientset.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return 0, fmt.Errorf("list jobs for active count: %w", err)
	}
	active := 0
	for i := range list.Items {
		if list.Items[i].Status.Active > 0 {
			active++
		}
	}
	return active, nil
}

// setClientsetForTest swaps the clientset for a fake in unit tests.
func (c *K8sClient) setClientsetForTest(cs kubernetes.Interface) { c.clientset = cs }

// nonK8sLabel matches characters not allowed in a Kubernetes label value.
var nonK8sLabel = regexp.MustCompile(`[^a-zA-Z0-9._-]`)

// sanitizeK8sLabel coerces an arbitrary string into a valid Kubernetes label
// value: disallowed characters become "-", the result is truncated to the
// 63-character limit, and leading/trailing non-alphanumerics are trimmed —
// a label value must START and END alphanumeric, not merely contain allowed
// characters. Candidate schemas begin with "_", which the API server would
// otherwise reject (rejecting the whole Job at creation).
func sanitizeK8sLabel(s string) string {
	out := nonK8sLabel.ReplaceAllString(s, "-")
	if len(out) > 63 {
		out = out[:63]
	}
	return strings.Trim(out, "-_.")
}
