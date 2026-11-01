// Package k8s is the Kubernetes adapter: one clientset creates dbt, python, validation, seed-build, compile and schema-op Jobs (jobs_create.go, schema_ops.go), reads their pod logs (jobs_observe.go) and deletes a Job whose attempt execution-controller ends (job_control.go). The watch package observes every Job.
package k8s

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/carolsimone/continuo/execution-controller/service/ports"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/num"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// JobParams represents the parameters needed to create a K8s Job
type JobParams struct {
	JobName      string
	TaskID       string
	RunID        string
	ScheduleName string // used for the schedule label
	ServiceName  string
	SchemaName   string
	TableName    string
	Namespace    string
	NodeType     pkg_model.NodeType
	ImageTag     string
	// SecretRef names the continuo-api-* Secret a python-api node's pod
	// receives as env vars; empty for every other node.
	SecretRef string
	// Operation selects the dbt verb execution-controller runs for this node.
	// pkg_model.OperationRun (empty) is the default: dbt run/seed/snapshot by
	// NodeType — a seed load is plain, non-destructive `dbt seed`.
	// pkg_model.OperationTest runs `dbt test --select <node>`.
	// pkg_model.OperationFullRefresh rebuilds a model (full_refresh command)
	// or seed (seed_full_refresh command) from scratch.
	Operation pkg_model.Operation
	// Attempt is which attempt of the task the Job runs; it labels the Job
	// and its pod and reaches the container as TASK_ATTEMPT.
	Attempt int
	// ActiveDeadlineSeconds is how long Kubernetes lets the Job run, counted
	// from the Job's start.
	ActiveDeadlineSeconds int64
}

// K8sClient provides methods to interact with Kubernetes
type K8sClient struct {
	clientset kubernetes.Interface
	logger    *slog.Logger
	commands  ports.CommandResolver
	// jobTTLSeconds is the TTLSecondsAfterFinished of every task, validation,
	// seed-build and compile Job the client creates: how long a finished
	// (Succeeded or Failed) Job and its pod stay on the cluster before
	// Kubernetes deletes them. The observer uploads an attempt's pod log and
	// results to S3 as soon as the attempt ends, so the window only serves
	// debugging a finished pod with kubectl.
	jobTTLSeconds int32
}

// RateLimit is the request rate limit of the Kubernetes client: QPS requests
// per second once it runs steadily, and Burst requests at once in a spike.
// Requests beyond it wait in the client until the Kubernetes API server is
// allowed another.
type RateLimit struct {
	QPS   int
	Burst int
}

func (l RateLimit) apply(rc *rest.Config) {
	rc.QPS = float32(l.QPS)
	rc.Burst = l.Burst
}

// NewK8sClient creates a new K8sClient limited to limit.
// Uses KUBECONFIG when set (local/docker-compose), otherwise falls back to
// in-cluster config (K8s pod with a ServiceAccount). commands resolves the
// per-service dbt command dialect for every Job this client builds, and every
// task, validation, seed-build and compile Job it creates is deleted by
// Kubernetes jobTTL after it finishes.
func NewK8sClient(logger *slog.Logger, commands ports.CommandResolver, limit RateLimit, jobTTL time.Duration) (*K8sClient, error) {
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

	limit.apply(restConfig)

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		logger.Error("Failed to create K8s clientset", "error", err)
		return nil, fmt.Errorf("failed to create k8s clientset: %w", err)
	}

	logger.Info("K8s client initialized successfully")

	return NewK8sClientFor(clientset, logger, commands, jobTTL), nil
}

// NewK8sClientFor creates a K8sClient over clientset; commands resolves the
// per-service dbt command dialect for every Job it builds, and every task,
// validation, seed-build and compile Job it creates carries jobTTL, in whole
// seconds, as its TTLSecondsAfterFinished.
func NewK8sClientFor(clientset kubernetes.Interface, logger *slog.Logger, commands ports.CommandResolver, jobTTL time.Duration) *K8sClient {
	return &K8sClient{clientset: clientset, logger: logger, commands: commands,
		jobTTLSeconds: num.ClampInt32(jobTTL / time.Second)}
}

// finishedJobTTL is the TTLSecondsAfterFinished of a Job the client creates.
func (c *K8sClient) finishedJobTTL() *int32 {
	ttl := c.jobTTLSeconds
	return &ttl
}

// Clientset is the Kubernetes clientset the client uses, for the Job watch.
func (c *K8sClient) Clientset() kubernetes.Interface { return c.clientset }

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

// CreateJob creates a K8s Job; a Job of the same name that already exists
// counts as created.
func (c *K8sClient) CreateJob(ctx context.Context, job *batchv1.Job) error {
	_, err := c.createJob(ctx, job)
	return err
}

// createJob creates a K8s Job and reports whether a Job of its name already
// existed, in which case it created nothing.
func (c *K8sClient) createJob(ctx context.Context, job *batchv1.Job) (existed bool, err error) {
	_, err = c.clientset.BatchV1().Jobs(job.Namespace).Create(ctx, job, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		c.logger.Info("K8s job already exists, creating nothing",
			"namespace", job.Namespace, "job_name", job.Name)
		return true, nil
	}
	if err != nil {
		c.logger.Error("Failed to create K8s job",
			"namespace", job.Namespace,
			"job_name", job.Name,
			"error", err,
		)
		return false, fmt.Errorf("failed to create k8s job: %w", err)
	}

	c.logger.Info("Created K8s job",
		"namespace", job.Namespace,
		"job_name", job.Name,
	)

	return false, nil
}

func (c *K8sClient) deleteJob(ctx context.Context, namespace, jobName string) error {
	policy := metav1.DeletePropagationBackground
	err := c.clientset.BatchV1().Jobs(namespace).Delete(ctx, jobName, metav1.DeleteOptions{PropagationPolicy: &policy})
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
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
