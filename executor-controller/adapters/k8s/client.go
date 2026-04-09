package k8s

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// JobParams represents the parameters needed to create a K8s Job
type JobParams struct {
	JobName      string
	TaskID       string
	ScheduleID   string
	ScheduleName string // used for the schedule label
	ServiceName  string
	Schema       string
	TableName    string
	Namespace    string
	NodeType     pkg_model.NodeType
}

// K8sClient provides methods to interact with Kubernetes
type K8sClient struct {
	clientset *kubernetes.Clientset
	logger    *slog.Logger
}

// NewK8sClient creates a new K8sClient using in-cluster configuration
func NewK8sClient(logger *slog.Logger) (*K8sClient, error) {
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		logger.Error("Failed to get in-cluster K8s config", "error", err)
		return nil, fmt.Errorf("failed to get in-cluster config: %w", err)
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

// CreateQueryJob builds and creates a K8s Job for query execution (idempotent)
func (c *K8sClient) CreateQueryJob(ctx context.Context, params JobParams) error {
	// Step 1: Check if job already exists (idempotent operation)
	exists, err := c.JobExists(ctx, params.Namespace, params.JobName)
	if err != nil {
		return err
	}

	if exists {
		c.logger.Info("K8s job already exists, skipping creation",
			"namespace", params.Namespace,
			"job_name", params.JobName,
		)
		return nil
	}

	// Step 2: Build Job spec
	backoffLimit := int32(0)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      params.JobName,
			Namespace: params.Namespace,
			Labels: map[string]string{
				"app":          "query-executor",
				"task-id":      params.TaskID,
				"schedule-id":  params.ScheduleID,
				"schedule":     params.ScheduleName,
				"table_name":   params.TableName,
				"schema_name":  params.Schema,
				"service_name": params.ServiceName,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":          "query-executor",
						"task-id":      params.TaskID,
						"schedule-id":  params.ScheduleID,
						"schedule":     params.ScheduleName,
						"table_name":   params.TableName,
						"schema_name":  params.Schema,
						"service_name": params.ServiceName,
					},
				},
				Spec: buildPodSpec(params),
			},
		},
	}

	// Step 3: Create the job
	return c.CreateJob(ctx, job)
}

// buildPodSpec constructs the PodSpec for a query executor job.
// PostgreSQL connection env vars are forwarded from the executor-controller's
// own environment so dbt pods can reach the same database.
func buildPodSpec(params JobParams) corev1.PodSpec {
	envVars := []corev1.EnvVar{
		{Name: "TASK_ID", Value: params.TaskID},
		{Name: "SCHEDULE_ID", Value: params.ScheduleID},
		{Name: "SCHEDULE_NAME", Value: params.ScheduleName},
		{Name: "SERVICE_NAME", Value: params.ServiceName},
		{Name: "SCHEMA", Value: params.Schema},
		{Name: "TABLE_NAME", Value: params.TableName},
		{Name: "JOB_NAME", Value: params.JobName},
		// dbt profile connection — forwarded from executor-controller environment
		{Name: "DBT_POSTGRES_HOST", Value: os.Getenv("POSTGRES_HOST")},
		{Name: "DBT_POSTGRES_PORT", Value: os.Getenv("POSTGRES_PORT")},
		{Name: "DBT_POSTGRES_DB", Value: os.Getenv("DBT_POSTGRES_DB")},
		{Name: "DBT_POSTGRES_USER", Value: os.Getenv("POSTGRES_USER")},
		{Name: "DBT_POSTGRES_PASSWORD", Value: os.Getenv("POSTGRES_PASSWORD")},
	}

	return corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever,
		Containers: []corev1.Container{
			{
				Name:            "query-executor",
				Image:           params.ServiceName,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         params.NodeType.Command(params.TableName),
				Env:             envVars,
			},
		},
	}
}
