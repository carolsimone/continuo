package k8s

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"regexp"

	validationmodel "github.com/carolsimone/continuo/executor-controller/domain/model"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
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
	ScheduleID   string
	ScheduleName string // used for the schedule label
	ServiceName  string
	SchemaName   string
	TableName    string
	Namespace    string
	NodeType     pkg_model.NodeType
	ImageTag     string
}

// K8sClient provides methods to interact with Kubernetes
type K8sClient struct {
	clientset kubernetes.Interface
	logger    *slog.Logger
}

// NewK8sClient creates a new K8sClient.
// Uses KUBECONFIG when set (local/docker-compose), otherwise falls back to
// in-cluster config (K8s pod with a ServiceAccount).
func NewK8sClient(logger *slog.Logger) (*K8sClient, error) {
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
	podSpec, err := buildPodSpec(params)
	if err != nil {
		return fmt.Errorf("failed to build pod spec: %w", err)
	}

	backoffLimit := int32(0)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      params.JobName,
			Namespace: params.Namespace,
			Labels: map[string]string{
				"app":          "dbt-job",
				"task-id":      params.TaskID,
				"schedule-id":  params.ScheduleID,
				"schedule":     params.ScheduleName,
				"table_name":   params.TableName,
				"schema_name":  params.SchemaName,
				"service_name": params.ServiceName,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":          "dbt-job",
						"task-id":      params.TaskID,
						"schedule-id":  params.ScheduleID,
						"schedule":     params.ScheduleName,
						"table_name":   params.TableName,
						"schema_name":  params.SchemaName,
						"service_name": params.ServiceName,
					},
				},
				Spec: podSpec,
			},
		},
	}

	// Step 3: Create the job
	return c.CreateJob(ctx, job)
}

// ValidationJobParams represents the parameters needed to create a
// mode=validation K8s Job. It mirrors JobParams for the production fields a
// validation node still needs and adds the validation-only fields (release/node
// identity, candidate schema, candidate SQL URI).
type ValidationJobParams struct {
	JobName     string
	ReleaseID   string
	NodeID      string
	ServiceName string
	SchemaName  string
	TableName   string
	NodeType    pkg_model.NodeType
	ImageTag    string

	// ValidationOp selects the runner operation: "build_from_sql" (default) or
	// "clone_from_prod". ProdSchema is the source schema for clone_from_prod.
	// Both are set per node by release-controller (Plan 3); empty here defaults
	// VALIDATION_OP to build_from_sql.
	ValidationOp string
	ProdSchema   string

	CandidateSchema string
	CandidateSQLURI string

	// ManifestS3URI is the S3 destination where the compile Job uploads the
	// compiled manifest.json. Populated only for mode=compile Jobs.
	ManifestS3URI string

	Namespace string
}

// CreateValidationJob builds and creates a mode=validation K8s Job
// (idempotent by job name). The Job carries app=dbt-job so existing watchers
// stay correct, plus the mode=validation label so k8s-controller routes its
// terminal status to validation.node.completed:v1. release-id/node-id are stored
// twice: as sanitized labels (for selection/observability) and as raw
// annotations (the authoritative identity k8s-controller echoes into the payload).
func (c *K8sClient) CreateValidationJob(ctx context.Context, params ValidationJobParams) error {
	exists, err := c.JobExists(ctx, params.Namespace, params.JobName)
	if err != nil {
		return err
	}
	if exists {
		c.logger.Info("Validation K8s job already exists, skipping creation",
			"namespace", params.Namespace,
			"job_name", params.JobName,
		)
		return nil
	}

	podSpec, err := buildValidationPodSpec(params)
	if err != nil {
		return fmt.Errorf("failed to build validation pod spec: %w", err)
	}

	backoffLimit := int32(0)
	labels := map[string]string{
		"app":          "dbt-job",
		"mode":         "validation",
		"release-id":   sanitizeK8sLabel(params.ReleaseID),
		"node-id":      sanitizeK8sLabel(params.NodeID),
		"service_name": params.ServiceName,
		"schema_name":  params.SchemaName,
		"table_name":   params.TableName,
	}
	annotations := map[string]string{
		pkg_model.AnnotationReleaseID: params.ReleaseID,
		pkg_model.AnnotationNodeID:    params.NodeID,
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        params.JobName,
			Namespace:   params.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations},
				Spec:       podSpec,
			},
		},
	}

	return c.CreateJob(ctx, job)
}

// buildValidationPodSpec constructs the PodSpec for a validation node job.
// Validation runs the continuo-owned validation image (which carries
// validation_runner.py + the warehouse adapter), NOT the per-service team
// image — so the per-service ImageTag is unused here and an empty tag is no
// longer an error. VALIDATION_IMAGE overrides verbatim; otherwise default to
// dbt-base:latest, prefixed with DOCKERHUB_USERNAME when set.
// The container command comes from model.ValidationCommand, the single source
// of truth for the validation CLI. VALIDATION_OP selects the runner operation
// (default "build_from_sql"); PROD_SCHEMA is the clone source for Plan 3.
func buildValidationPodSpec(p ValidationJobParams) (corev1.PodSpec, error) {
	image := os.Getenv("VALIDATION_IMAGE")
	if image == "" {
		image = "dbt-base:latest"
		if user := os.Getenv("DOCKERHUB_USERNAME"); user != "" {
			image = user + "/" + image
		}
	}

	op := p.ValidationOp
	if op == "" {
		op = "build_from_sql"
	}

	envVars := []corev1.EnvVar{
		{Name: "RELEASE_ID", Value: p.ReleaseID},
		{Name: "NODE_ID", Value: p.NodeID},
		{Name: "SERVICE_NAME", Value: p.ServiceName},
		{Name: "SCHEMA", Value: p.SchemaName},
		{Name: "TABLE_NAME", Value: p.TableName},
		{Name: "JOB_NAME", Value: p.JobName},
		{Name: "DBT_TARGET_SCHEMA", Value: p.CandidateSchema},
		// CANDIDATE_SQL_URI is an S3 URI pointing to the node's compiled SQL with
		// schema refs already rewritten to the candidate schema. validation_runner.py
		// fetches this object from S3 and builds the node as an empty CTAS table.
		{Name: "CANDIDATE_SQL_URI", Value: p.CandidateSQLURI},
		// S3 credentials — forwarded from executor-controller environment so the
		// validation runner can boto3-GET the candidate SQL object.
		{Name: "S3_ENDPOINT_URL", Value: os.Getenv("S3_ENDPOINT_URL")},
		{Name: "S3_BUCKET", Value: os.Getenv("S3_BUCKET")},
		{Name: "AWS_ACCESS_KEY_ID", Value: os.Getenv("AWS_ACCESS_KEY_ID")},
		{Name: "AWS_SECRET_ACCESS_KEY", Value: os.Getenv("AWS_SECRET_ACCESS_KEY")},
		{Name: "AWS_DEFAULT_REGION", Value: os.Getenv("AWS_DEFAULT_REGION")},
		// dbt profile connection — forwarded from executor-controller environment
		{Name: "DBT_POSTGRES_HOST", Value: os.Getenv("POSTGRES_HOST")},
		{Name: "DBT_POSTGRES_PORT", Value: os.Getenv("POSTGRES_PORT")},
		{Name: "DBT_POSTGRES_DB", Value: os.Getenv("DBT_POSTGRES_DB")},
		{Name: "DBT_POSTGRES_USER", Value: os.Getenv("POSTGRES_USER")},
		{Name: "DBT_POSTGRES_PASSWORD", Value: os.Getenv("POSTGRES_PASSWORD")},
		// Selects the runner operation; PROD_SCHEMA is the clone source (Plan 3).
		{Name: "VALIDATION_OP", Value: op},
		{Name: "PROD_SCHEMA", Value: p.ProdSchema},
	}

	return corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever,
		Containers: []corev1.Container{
			{
				Name:            "dbt-job",
				Image:           image,
				ImagePullPolicy: validationImagePullPolicy(),
				Command:         validationmodel.ValidationCommand(p.NodeType, p.TableName),
				Env:             envVars,
			},
		},
	}, nil
}

// validationImagePullPolicy resolves the pull policy for validation Job pods.
//
// Validation runs a continuo-owned image pinned to a mutable tag (dbt-base:latest
// by default), which is re-pushed when the validator changes. PullAlways ensures
// the node fetches the freshly pushed image rather than a stale cached layer.
//
// Environments that side-load images directly into the node's image cache and
// have no registry to pull from (the kind-based e2e suite, local clusters) set
// VALIDATION_IMAGE_PULL_POLICY=IfNotPresent (or Never) so the locally-loaded
// image is used; PullAlways there would fail with ErrImagePull because the
// mutable tag exists only in the node cache, not in any registry.
func validationImagePullPolicy() corev1.PullPolicy {
	switch os.Getenv("VALIDATION_IMAGE_PULL_POLICY") {
	case string(corev1.PullIfNotPresent):
		return corev1.PullIfNotPresent
	case string(corev1.PullNever):
		return corev1.PullNever
	default:
		return corev1.PullAlways
	}
}

// nonK8sLabel matches characters not allowed in a Kubernetes label value.
var nonK8sLabel = regexp.MustCompile(`[^a-zA-Z0-9._-]`)

// sanitizeK8sLabel coerces an arbitrary string into a valid Kubernetes label
// value: disallowed characters become "-" and the result is truncated to the
// 63-character limit.
func sanitizeK8sLabel(s string) string {
	out := nonK8sLabel.ReplaceAllString(s, "-")
	if len(out) > 63 {
		out = out[:63]
	}
	return out
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

// CreateSeedBuildJob builds and creates a mode=seed_build K8s Job (idempotent
// by job name). The Job uses the team image (same as production) and runs
// `dbt seed --select <TableName>`, materializing into the candidate schema via
// DBT_TARGET_SCHEMA so the generate_schema_name macro routes the output there.
// The mode=seed_build label lets k8s-controller route its terminal status to
// seed.build.node.completed:v1.
func (c *K8sClient) CreateSeedBuildJob(ctx context.Context, params ValidationJobParams) error {
	exists, err := c.JobExists(ctx, params.Namespace, params.JobName)
	if err != nil {
		return err
	}
	if exists {
		c.logger.Info("Seed-build K8s job already exists, skipping creation",
			"namespace", params.Namespace,
			"job_name", params.JobName,
		)
		return nil
	}

	podSpec, err := buildSeedBuildPodSpec(params)
	if err != nil {
		return fmt.Errorf("failed to build seed-build pod spec: %w", err)
	}

	backoffLimit := int32(0)
	labels := map[string]string{
		"app":          "dbt-job",
		"mode":         "seed_build",
		"release-id":   sanitizeK8sLabel(params.ReleaseID),
		"node-id":      sanitizeK8sLabel(params.NodeID),
		"service_name": params.ServiceName,
		"schema_name":  params.SchemaName,
		"table_name":   params.TableName,
	}
	annotations := map[string]string{
		pkg_model.AnnotationReleaseID: params.ReleaseID,
		pkg_model.AnnotationNodeID:    params.NodeID,
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        params.JobName,
			Namespace:   params.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations},
				Spec:       podSpec,
			},
		},
	}

	return c.CreateJob(ctx, job)
}

// buildSeedBuildPodSpec constructs the PodSpec for a seed-build Job.
// It mirrors buildPodSpec (production team image + standard DBT_POSTGRES_* +
// SCHEMA/TABLE_NAME env) and adds DBT_TARGET_SCHEMA=<CandidateSchema> so the
// generate_schema_name macro materializes the seed into the candidate schema.
// ImageTag must be non-empty — the team image must be explicitly versioned.
func buildSeedBuildPodSpec(p ValidationJobParams) (corev1.PodSpec, error) {
	if p.ImageTag == "" {
		return corev1.PodSpec{}, fmt.Errorf("%w: image_tag missing from seed-build job params for service %s",
			events.ErrPermanent, p.ServiceName)
	}

	image := p.ServiceName + ":" + p.ImageTag
	if user := os.Getenv("DOCKERHUB_USERNAME"); user != "" {
		image = user + "/" + image
	}

	envVars := []corev1.EnvVar{
		{Name: "RELEASE_ID", Value: p.ReleaseID},
		{Name: "NODE_ID", Value: p.NodeID},
		{Name: "SERVICE_NAME", Value: p.ServiceName},
		{Name: "SCHEMA", Value: p.SchemaName},
		{Name: "TABLE_NAME", Value: p.TableName},
		{Name: "JOB_NAME", Value: p.JobName},
		{Name: "DBT_TARGET_SCHEMA", Value: p.CandidateSchema},
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
				Name:            "dbt-job",
				Image:           image,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         p.NodeType.Command(p.TableName),
				Env:             envVars,
			},
		},
	}, nil
}

// CreateCompileJob builds and creates a mode=compile K8s Job (idempotent by
// job name). The Job pod has a shared emptyDir volume "shared" mounted at
// /shared in both containers:
//   - initContainer "compile": team image runs `dbt compile --profiles-dir
//     /project && cp .../manifest.json /shared/manifest.json` with the
//     standard DBT_POSTGRES_* warehouse envs.
//   - main container "upload": validation image (VALIDATION_IMAGE or
//     <DOCKERHUB_USERNAME>/dbt-base:latest) runs `python /compile_uploader.py`
//     with COMPILE_MANIFEST_PATH + MANIFEST_S3_URI + the S3 credential envs.
//
// The mode=compile label lets k8s-controller route its terminal status to
// compile.node.completed:v1. release-id/node-id annotations carry the
// authoritative identity. ImageTag must be non-empty — the team image must be
// explicitly versioned.
func (c *K8sClient) CreateCompileJob(ctx context.Context, params ValidationJobParams) error {
	exists, err := c.JobExists(ctx, params.Namespace, params.JobName)
	if err != nil {
		return err
	}
	if exists {
		c.logger.Info("Compile K8s job already exists, skipping creation",
			"namespace", params.Namespace,
			"job_name", params.JobName,
		)
		return nil
	}

	podSpec, err := buildCompilePodSpec(params)
	if err != nil {
		return fmt.Errorf("failed to build compile pod spec: %w", err)
	}

	backoffLimit := int32(0)
	labels := map[string]string{
		"app":          "dbt-job",
		"mode":         "compile",
		"release-id":   sanitizeK8sLabel(params.ReleaseID),
		"node-id":      sanitizeK8sLabel(params.NodeID),
		"service_name": params.ServiceName,
	}
	annotations := map[string]string{
		pkg_model.AnnotationReleaseID: params.ReleaseID,
		pkg_model.AnnotationNodeID:    params.NodeID,
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        params.JobName,
			Namespace:   params.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations},
				Spec:       podSpec,
			},
		},
	}

	return c.CreateJob(ctx, job)
}

// buildCompilePodSpec constructs the PodSpec for a compile Job. The pod has:
//   - a shared emptyDir volume "shared" mounted at /shared in both containers;
//   - an initContainer "compile" using the team image (ImageTag must be
//     non-empty) that runs `dbt compile --profiles-dir /project` and copies
//     the resulting manifest.json into /shared/manifest.json;
//   - a main container "upload" using a minimal python+boto3 image (no dbt;
//     COMPILE_UPLOAD_IMAGE env, else <DOCKERHUB_USERNAME>/manifest-uploader:latest)
//     that runs `python /compile_uploader.py` with COMPILE_MANIFEST_PATH,
//     MANIFEST_S3_URI, and the S3 credential envs forwarded from the executor-controller env.
func buildCompilePodSpec(p ValidationJobParams) (corev1.PodSpec, error) {
	if p.ImageTag == "" {
		return corev1.PodSpec{}, fmt.Errorf("%w: image_tag missing from compile job params for service %s",
			events.ErrPermanent, p.ServiceName)
	}

	// Team image for the init (compile) container.
	teamImage := p.ServiceName + ":" + p.ImageTag
	if user := os.Getenv("DOCKERHUB_USERNAME"); user != "" {
		teamImage = user + "/" + teamImage
	}

	// Upload container image: a minimal python+boto3 image (NO dbt). The compile leg keeps S3
	// access out of any dbt image. Defaults to manifest-uploader:latest (DOCKERHUB_USERNAME-prefixed).
	uploadImage := os.Getenv("COMPILE_UPLOAD_IMAGE")
	if uploadImage == "" {
		uploadImage = "manifest-uploader:latest"
		if user := os.Getenv("DOCKERHUB_USERNAME"); user != "" {
			uploadImage = user + "/" + uploadImage
		}
	}

	sharedMount := corev1.VolumeMount{Name: "shared", MountPath: "/shared"}

	initEnvVars := []corev1.EnvVar{
		// dbt profile connection — forwarded from executor-controller environment
		{Name: "DBT_POSTGRES_HOST", Value: os.Getenv("POSTGRES_HOST")},
		{Name: "DBT_POSTGRES_PORT", Value: os.Getenv("POSTGRES_PORT")},
		{Name: "DBT_POSTGRES_DB", Value: os.Getenv("DBT_POSTGRES_DB")},
		{Name: "DBT_POSTGRES_USER", Value: os.Getenv("POSTGRES_USER")},
		{Name: "DBT_POSTGRES_PASSWORD", Value: os.Getenv("POSTGRES_PASSWORD")},
	}

	uploadEnvVars := []corev1.EnvVar{
		{Name: "COMPILE_MANIFEST_PATH", Value: "/shared/manifest.json"},
		{Name: "MANIFEST_S3_URI", Value: p.ManifestS3URI},
		// S3 credentials — forwarded from executor-controller environment.
		{Name: "S3_ENDPOINT_URL", Value: os.Getenv("S3_ENDPOINT_URL")},
		{Name: "S3_BUCKET", Value: os.Getenv("S3_BUCKET")},
		{Name: "AWS_ACCESS_KEY_ID", Value: os.Getenv("AWS_ACCESS_KEY_ID")},
		{Name: "AWS_SECRET_ACCESS_KEY", Value: os.Getenv("AWS_SECRET_ACCESS_KEY")},
		{Name: "AWS_DEFAULT_REGION", Value: os.Getenv("AWS_DEFAULT_REGION")},
	}

	return corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever,
		Volumes: []corev1.Volume{
			{
				Name: "shared",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			},
		},
		InitContainers: []corev1.Container{
			{
				Name:            "compile",
				Image:           teamImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command: []string{
					"sh", "-c",
					"dbt compile --profiles-dir /project && cp /project/target/manifest.json /shared/manifest.json",
				},
				Env:          initEnvVars,
				VolumeMounts: []corev1.VolumeMount{sharedMount},
			},
		},
		Containers: []corev1.Container{
			{
				Name:            "upload",
				Image:           uploadImage,
				ImagePullPolicy: validationImagePullPolicy(),
				Command:         []string{"python", "/compile_uploader.py"},
				Env:             uploadEnvVars,
				VolumeMounts:    []corev1.VolumeMount{sharedMount},
			},
		},
	}, nil
}

// setClientsetForTest swaps the clientset for a fake in unit tests.
func (c *K8sClient) setClientsetForTest(cs kubernetes.Interface) { c.clientset = cs }

// buildPodSpec constructs the PodSpec for a query executor job.
// PostgreSQL connection env vars are forwarded from the executor-controller's
// own environment so dbt pods can reach the same database.
// Returns an error if ImageTag is empty — content-addressed tags must be explicit;
// falling back to "latest" is intentionally refused.
func buildPodSpec(params JobParams) (corev1.PodSpec, error) {
	if params.ImageTag == "" {
		return corev1.PodSpec{}, fmt.Errorf("%w: image_tag missing from job params for service %s",
			events.ErrPermanent, params.ServiceName)
	}

	image := params.ServiceName + ":" + params.ImageTag
	if user := os.Getenv("DOCKERHUB_USERNAME"); user != "" {
		image = user + "/" + image
	}

	envVars := []corev1.EnvVar{
		{Name: "TASK_ID", Value: params.TaskID},
		{Name: "SCHEDULE_ID", Value: params.ScheduleID},
		{Name: "SCHEDULE_NAME", Value: params.ScheduleName},
		{Name: "SERVICE_NAME", Value: params.ServiceName},
		{Name: "SCHEMA", Value: params.SchemaName},
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
				Name:            "dbt-job",
				Image:           image,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         params.NodeType.Command(params.TableName),
				Env:             envVars,
			},
		},
	}, nil
}
