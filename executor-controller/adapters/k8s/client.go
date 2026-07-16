package k8s

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"

	validationmodel "github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/ports"
	"github.com/carolsimone/continuo/executor-controller/service/runtimecontext"
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
	// Operation selects the dbt verb the executor runs for this node.
	// pkg_model.OperationRun (empty) is the default: dbt run/seed/snapshot by
	// NodeType. pkg_model.OperationTest runs `dbt test --select <node>`.
	Operation pkg_model.Operation
	// Mode is the optional dispatch mode. Empty for normal production jobs;
	// set to events.ModePromoteSeed for promote-seed jobs. When non-empty the
	// value is stamped as a "mode" label on the Job and its pod template so
	// k8s-controller can suppress the production lifecycle events for jobs
	// that have no real state run. Normal production jobs (empty Mode) get no
	// mode label — the wire format is unchanged.
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
	podSpec, err := buildPodSpec(params,
		c.commands.NodeCommand(params.ServiceName, params.Operation, params.NodeType, params.TableName))
	if err != nil {
		return fmt.Errorf("failed to build pod spec: %w", err)
	}

	backoffLimit := int32(0)
	jobLabels := map[string]string{
		"app":          "dbt-job",
		"task-id":      params.TaskID,
		"schedule-id":  params.ScheduleID,
		"schedule":     params.ScheduleName,
		"table_name":   params.TableName,
		"schema_name":  params.SchemaName,
		"service_name": params.ServiceName,
	}
	// Stamp the mode label only when the caller provides one. Normal production
	// jobs (empty Mode) get NO mode label — their wire format is unchanged and
	// k8s-controller continues to route them through the production lifecycle.
	if params.Mode != "" {
		jobLabels["mode"] = params.Mode
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      params.JobName,
			Namespace: params.Namespace,
			Labels:    jobLabels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: jobLabels,
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
//
// Validation runs the continuo-owned validation-runner image, which carries
// validation_runner.py, the warehouse adapter, and boto3 — never the per-service
// team image. VALIDATION_IMAGE overrides verbatim; else validation-runner:latest,
// DOCKERHUB_USERNAME-prefixed.
//
// build_from_sql nodes receive CANDIDATE_SQL_URI + S3 credentials directly on the
// single main container; the runner fetches the compiled SQL from S3 itself. There
// is no init container and no shared emptyDir for this path. clone_from_prod nodes
// have no candidate SQL and never touch S3, so they remain single-container with no
// emptyDir and no S3 credentials.
func buildValidationPodSpec(p ValidationJobParams) (corev1.PodSpec, error) {
	// VALIDATION_IMAGE pins the validator image. In production the Helm chart sets
	// it to the SHA-tagged continuo-validation-runner:<imageTag>; when unset (local
	// docker-compose, kind e2e) it falls back to the locally-built validation-runner:latest,
	// DOCKERHUB_USERNAME-prefixed when set.
	image := os.Getenv("VALIDATION_IMAGE")
	if image == "" {
		image = "validation-runner:latest"
		if user := os.Getenv("DOCKERHUB_USERNAME"); user != "" {
			image = user + "/" + image
		}
	}

	op := p.ValidationOp
	if op == "" {
		op = "build_from_sql"
	}

	// Env common to both validation ops.
	mainEnv := []corev1.EnvVar{
		{Name: "RELEASE_ID", Value: p.ReleaseID},
		{Name: "NODE_ID", Value: p.NodeID},
		{Name: "SERVICE_NAME", Value: p.ServiceName},
		{Name: "SCHEMA", Value: p.SchemaName},
		{Name: "TABLE_NAME", Value: p.TableName},
		{Name: "JOB_NAME", Value: p.JobName},
		{Name: "DBT_TARGET_SCHEMA", Value: p.CandidateSchema},
		// dbt profile connection — forwarded from executor-controller environment.
		{Name: "DBT_POSTGRES_HOST", Value: os.Getenv("POSTGRES_HOST")},
		{Name: "DBT_POSTGRES_PORT", Value: os.Getenv("POSTGRES_PORT")},
		{Name: "DBT_POSTGRES_DB", Value: os.Getenv("DBT_POSTGRES_DB")},
		{Name: "DBT_POSTGRES_USER", Value: os.Getenv("POSTGRES_USER")},
		{Name: "DBT_POSTGRES_PASSWORD", Value: os.Getenv("POSTGRES_PASSWORD")},
		{Name: "VALIDATION_OP", Value: op},
		{Name: "PROD_SCHEMA", Value: p.ProdSchema},
	}

	mainContainer := corev1.Container{
		Name:            "dbt-job",
		Image:           image,
		ImagePullPolicy: validationImagePullPolicy(),
		Command:         validationmodel.ValidationCommand(p.NodeType, p.TableName),
		Env:             mainEnv,
	}

	switch op {
	case "clone_from_prod":
		// No candidate SQL, no S3, single container.
		return corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    []corev1.Container{mainContainer},
		}, nil

	case "build_from_sql":
		// The validation container fetches its own compiled SQL from S3 (boto3) and
		// builds it WITH NO DATA — no sidecar, no shared emptyDir. CandidateSQLURI must
		// be set: changed models/snapshots always carry one; nodes without candidate SQL
		// (unchanged upstreams, seeds) use clone_from_prod.
		if p.CandidateSQLURI == "" {
			return corev1.PodSpec{}, fmt.Errorf("%w: candidate_sql_uri missing from build_from_sql validation job params for node %s",
				events.ErrPermanent, p.NodeID)
		}
		mainContainer.Env = append(mainContainer.Env, corev1.EnvVar{Name: "CANDIDATE_SQL_URI", Value: p.CandidateSQLURI})
		mainContainer.Env = append(mainContainer.Env, s3CredEnvVars()...)
		return corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    []corev1.Container{mainContainer},
		}, nil

	default:
		return corev1.PodSpec{}, fmt.Errorf("%w: unknown validation_op %q for node %s",
			events.ErrPermanent, op, p.NodeID)
	}
}

// s3SidecarImage resolves the non-dbt S3 I/O sidecar image used by the
// compile leg (manifest upload). The
// S3_SIDECAR_IMAGE env overrides verbatim; otherwise default to s3-sidecar:latest,
// DOCKERHUB_USERNAME-prefixed when set.
func s3SidecarImage() string {
	img := os.Getenv("S3_SIDECAR_IMAGE")
	if img == "" {
		img = "s3-sidecar:latest"
		if user := os.Getenv("DOCKERHUB_USERNAME"); user != "" {
			img = user + "/" + img
		}
	}
	return img
}

// s3CredEnvVars returns the four S3 credential environment variables forwarded
// from the executor-controller environment. S3_BUCKET is intentionally omitted —
// both the compile uploader and the validation runner parse the bucket from their
// respective URI parameters and never read S3_BUCKET.
func s3CredEnvVars() []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "S3_ENDPOINT_URL", Value: os.Getenv("S3_ENDPOINT_URL")},
		{Name: "AWS_ACCESS_KEY_ID", Value: os.Getenv("AWS_ACCESS_KEY_ID")},
		{Name: "AWS_SECRET_ACCESS_KEY", Value: os.Getenv("AWS_SECRET_ACCESS_KEY")},
		{Name: "AWS_DEFAULT_REGION", Value: os.Getenv("AWS_DEFAULT_REGION")},
	}
}

const (
	// sharedDir is where the "shared" emptyDir volume mounts in compile pods:
	// the init container writes the release's artifacts here and the main
	// container uploads them from here.
	sharedDir = "/shared"
	// sharedManifestPath is where the init container leaves manifest.json for
	// the uploader, whatever path the team's own compile wrote it to.
	sharedManifestPath = sharedDir + "/manifest.json"
	// runtimeContextEnvVar carries the controller's canonical parse context
	// into the compile init container.
	runtimeContextEnvVar = "CONTINUO_RUNTIME_CONTEXT_JSON"
)

// sharedEmptyDirVolume returns the "shared" emptyDir volume used as the hand-off
// point between the init container and the main container in compile pods.
func sharedEmptyDirVolume() corev1.Volume {
	return corev1.Volume{
		Name:         "shared",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}
}

// sharedVolumeMount returns the VolumeMount for the "shared" emptyDir volume,
// mounting it at sharedDir in a container.
func sharedVolumeMount() corev1.VolumeMount {
	return corev1.VolumeMount{Name: "shared", MountPath: sharedDir}
}

// validationImagePullPolicy resolves the pull policy applied to both the
// validation main container and the compile leg's s3-sidecar upload container.
//
// The default is PullAlways so the s3-sidecar (:latest, mutable tag) is
// re-pulled when it is re-pushed to the registry. The SHA-pinned validation
// image just receives a cheap digest check under PullAlways rather than a full
// layer download.
//
// e2e and local clusters side-load images directly into the node's image cache
// and have no registry to pull from, so they set VALIDATION_IMAGE_PULL_POLICY
// to IfNotPresent or Never; PullAlways would fail with ErrImagePull there
// because neither image exists in any accessible registry.
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

	podSpec, err := buildSeedBuildPodSpec(params,
		c.commands.SeedBuildCommand(params.ServiceName, params.TableName, params.CandidateSchema))
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
func buildSeedBuildPodSpec(p ValidationJobParams, command []string) (corev1.PodSpec, error) {
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
				Command:         command,
				Env:             envVars,
			},
		},
	}, nil
}

// CreateCompileJob builds and creates a mode=compile K8s Job (idempotent by
// job name). The Job pod has a shared emptyDir volume "shared" mounted at
// /shared in both containers:
//   - initContainer "compile": team image runs the service's resolved compile
//     command and copies the manifest from its declared path into
//     /shared/manifest.json, with the standard DBT_POSTGRES_* warehouse envs.
//   - main container "upload": the shared s3-sidecar image (S3_SIDECAR_IMAGE
//     env, else <DOCKERHUB_USERNAME>/s3-sidecar:latest) runs
//     `python /compile_uploader.py` with COMPILE_MANIFEST_PATH +
//     MANIFEST_S3_URI + the S3 credential envs.
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

	compile := c.commands.CompileCommand(params.ServiceName)
	controllerContext, err := runtimecontext.Build(
		c.commands.RuntimeContext(params.ServiceName), os.Getenv)
	if err != nil {
		return fmt.Errorf("failed to build runtime context for service %s: %w",
			params.ServiceName, err)
	}
	podSpec, err := buildCompilePodSpec(params, compile, controllerContext)
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

// runtimeExporterPath is where the dbt base image installs the runtime manifest
// exporter. Team images built before it existed do not have it, so the compile
// line tests for it rather than assuming it.
const runtimeExporterPath = "/continuo/bin/continuo-export-runtime-manifest"

// buildCompileInitCommand assembles the compile initContainer's `sh -c` line.
// The team's own compile argv runs first and verbatim, then the manifest is
// copied to the shared volume, then the runtime manifest artifacts are exported
// beside it when the image ships the exporter. An image without the exporter
// logs and succeeds, producing a manifest-only release.
//
// The controller context reaches the exporter through the environment rather
// than the command line, so the line stays stable regardless of its contents.
func buildCompileInitCommand(p ValidationJobParams, compile ports.CompileCommand) string {
	exportArgv := []string{
		runtimeExporterPath,
		"--manifest", compile.ManifestPath,
		"--partial-parse", compile.PartialParsePath,
		"--output-dir", sharedDir,
		"--service-name", p.ServiceName,
		"--release-id", p.ReleaseID,
		"--image-tag", p.ImageTag,
		"--artifact-uri", siblingS3URI(p.ManifestS3URI, "partial_parse.msgpack"),
	}
	export := shellJoin(exportArgv) +
		` --controller-context "$` + runtimeContextEnvVar + `"`

	return shellJoin(compile.Argv) +
		" && cp " + shellQuote(compile.ManifestPath) + " " + sharedManifestPath +
		" && if [ -x " + runtimeExporterPath + " ]; then " + export + "; " +
		"else echo " + shellQuote("runtime exporter absent; manifest-only compatibility release") + "; fi"
}

// siblingS3URI replaces the object name in uri with name, keeping the release's
// prefix. The uploader writes the artifact set to these sibling keys.
func siblingS3URI(uri, name string) string {
	if i := strings.LastIndex(uri, "/"); i >= 0 {
		return uri[:i+1] + name
	}
	return name
}

// buildCompilePodSpec constructs the PodSpec for a compile Job. The pod has:
//   - a shared emptyDir volume "shared" mounted at /shared in both containers;
//   - an initContainer "compile" using the team image (ImageTag must be
//     non-empty) that runs the service's resolved compile command, copies the
//     manifest from its declared path into /shared/manifest.json, and exports
//     the runtime manifest artifacts beside it when the image ships the
//     exporter. controllerContext reaches it as CONTINUO_RUNTIME_CONTEXT_JSON.
//   - a main container "upload" using the shared s3-sidecar image (no dbt;
//     S3_SIDECAR_IMAGE env, else <DOCKERHUB_USERNAME>/s3-sidecar:latest)
//     that runs `python /compile_uploader.py` with COMPILE_MANIFEST_PATH,
//     MANIFEST_S3_URI, the runtime artifact paths, and the S3 credential envs
//     forwarded from the executor-controller env.
func buildCompilePodSpec(p ValidationJobParams, compile ports.CompileCommand, controllerContext string) (corev1.PodSpec, error) {
	if p.ImageTag == "" {
		return corev1.PodSpec{}, fmt.Errorf("%w: image_tag missing from compile job params for service %s",
			events.ErrPermanent, p.ServiceName)
	}

	// Team image for the init (compile) container.
	teamImage := p.ServiceName + ":" + p.ImageTag
	if user := os.Getenv("DOCKERHUB_USERNAME"); user != "" {
		teamImage = user + "/" + teamImage
	}

	// Upload container image: the shared minimal python+boto3 sidecar (NO dbt).
	uploadImage := s3SidecarImage()

	mount := sharedVolumeMount()

	initEnvVars := []corev1.EnvVar{
		// dbt profile connection — forwarded from executor-controller environment
		{Name: "DBT_POSTGRES_HOST", Value: os.Getenv("POSTGRES_HOST")},
		{Name: "DBT_POSTGRES_PORT", Value: os.Getenv("POSTGRES_PORT")},
		{Name: "DBT_POSTGRES_DB", Value: os.Getenv("DBT_POSTGRES_DB")},
		{Name: "DBT_POSTGRES_USER", Value: os.Getenv("POSTGRES_USER")},
		{Name: "DBT_POSTGRES_PASSWORD", Value: os.Getenv("POSTGRES_PASSWORD")},
		// The parse context the exporter stamps on the runtime manifest
		// descriptor, so a consumer can tell what the artifact was parsed under.
		{Name: runtimeContextEnvVar, Value: controllerContext},
	}

	// compile_uploader.py parses the bucket from MANIFEST_S3_URI; S3_BUCKET is
	// intentionally omitted (see s3CredEnvVars). The runtime artifact paths are
	// always passed: the uploader keys off whether the exporter actually wrote
	// them, so an image without the exporter uploads the manifest alone.
	uploadEnvVars := append([]corev1.EnvVar{
		{Name: "COMPILE_MANIFEST_PATH", Value: sharedManifestPath},
		{Name: "MANIFEST_S3_URI", Value: p.ManifestS3URI},
		{Name: "COMPILE_PARTIAL_PARSE_PATH", Value: sharedDir + "/partial_parse.msgpack"},
		{Name: "COMPILE_RUNTIME_DESCRIPTOR_PATH", Value: sharedDir + "/runtime-manifest.json"},
	}, s3CredEnvVars()...)

	return corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever,
		Volumes:       []corev1.Volume{sharedEmptyDirVolume()},
		InitContainers: []corev1.Container{
			{
				Name:            "compile",
				Image:           teamImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:      []string{"sh", "-c", buildCompileInitCommand(p, compile)},
				Env:          initEnvVars,
				VolumeMounts: []corev1.VolumeMount{mount},
			},
		},
		Containers: []corev1.Container{
			{
				Name:            "upload",
				Image:           uploadImage,
				ImagePullPolicy: validationImagePullPolicy(),
				Command:         []string{"python", "/compile_uploader.py"},
				Env:             uploadEnvVars,
				VolumeMounts:    []corev1.VolumeMount{mount},
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
func buildPodSpec(params JobParams, command []string) (corev1.PodSpec, error) {
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
				Command:         command,
				Env:             envVars,
			},
		},
	}, nil
}
