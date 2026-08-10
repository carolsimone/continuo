package k8s

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	validationmodel "github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/artifacts"
	"github.com/carolsimone/continuo/executor-controller/service/ports"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/parsecache"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
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

	// Step 2: Build Job spec. Python-model nodes run the domain repository's own
	// image under the runtime harness's environment; every other node type runs
	// the team's dbt image under a resolved dbt command. The Job metadata below
	// is shared, so both kinds route through k8s-controller's production
	// lifecycle identically.
	var podSpec corev1.PodSpec
	if params.NodeType == pkg_model.NodeTypePythonModel {
		podSpec, err = buildPythonPodSpec(params)
	} else {
		podSpec, err = buildPodSpec(params,
			c.commands.NodeCommand(params.ServiceName, params.Operation, params.NodeType, params.TableName),
			c.commands.PartialParsePath(params.ServiceName))
	}
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
	// The runtime label distinguishes python pods for operators without
	// changing the app selector that CountActive uses for the concurrency cap.
	if params.NodeType == pkg_model.NodeTypePythonModel {
		jobLabels["runtime"] = "python"
	}
	// Stamp the mode label only when the caller provides one. Normal production
	// jobs (empty Mode) get NO mode label — their wire format is unchanged and
	// k8s-controller continues to route them through the production lifecycle.
	if params.Mode != "" {
		jobLabels["mode"] = params.Mode
	}
	ttl := jobTTLSecondsAfterFinished
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      params.JobName,
			Namespace: params.Namespace,
			Labels:    jobLabels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
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
// identity, candidate schema, candidate artifact URI).
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

	CandidateSchema      string
	CandidateArtifactURI string

	// ManifestS3URI is the S3 destination where the compile Job uploads the
	// compiled manifest.json. Populated only for mode=compile Jobs.
	ManifestS3URI string

	// ParseProdS3URI / ParseCandidateS3URI are the S3 destinations for the
	// compile Job's exported partial-parse artifacts. Empty (older
	// compile.requested messages without candidate_schema) disables the
	// parse-export leg for this release.
	ParseProdS3URI      string
	ParseCandidateS3URI string

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
		"mode":         events.ModeValidation,
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
	ttl := jobTTLSecondsAfterFinished
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        params.JobName,
			Namespace:   params.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
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
// Validation runs the external continuo-validation-<engine> image (PostgreSQL and
// Trino today) that bakes one engine adapter — never the per-service team image. The
// SRE selects the engine at deploy time (Helm/compose) via VALIDATION_IMAGE; the
// executor runs it verbatim. The warehouse connection is not injected inline: the operator-owned
// Secret named by VALIDATION_WAREHOUSE_SECRET is attached to the container via
// envFrom, so the warehouse credentials are owned by the operator and separate from
// the executor's own DB. The dbt-running team containers attach this same Secret —
// it is the single warehouse connection for validation and dbt jobs alike.
//
// build_from_sql nodes receive CANDIDATE_SQL_URI + S3 credentials directly on the
// single main container; the runner fetches the compiled SQL from S3 itself. There
// is no init container and no shared emptyDir for this path. clone_from_prod nodes
// have no candidate SQL and never touch S3, so they remain single-container with no
// emptyDir and no S3 credentials. build_from_columns (python-model nodes) receive
// CANDIDATE_SPEC_URI + S3 credentials instead: the runner fetches the published
// JSON validation spec (declared reads + output columns), not compiled SQL.
func buildValidationPodSpec(p ValidationJobParams) (corev1.PodSpec, error) {
	// VALIDATION_IMAGE names the engine's continuo-validation-<engine> image; the SRE
	// chooses the engine at deploy time (Helm/compose). The executor bakes in no
	// engine — a hardcoded default would silently force one — so an unset image
	// fails the node permanently with an actionable reason.
	image := os.Getenv("VALIDATION_IMAGE")
	if image == "" {
		return corev1.PodSpec{}, fmt.Errorf("%w: VALIDATION_IMAGE not configured (set it to the matching continuo-validation-<engine> image) for node %s",
			events.ErrPermanent, p.NodeID)
	}

	// The validation container's warehouse credentials come from an operator-owned
	// Secret (envFrom), not inline env. Without it validation cannot connect, so
	// fail the node permanently with an actionable reason rather than launch a pod
	// that can only error.
	whFrom, err := warehouseSecretEnvFrom("node " + p.NodeID)
	if err != nil {
		return corev1.PodSpec{}, err
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
		{Name: "VALIDATION_OP", Value: op},
		{Name: "PROD_SCHEMA", Value: p.ProdSchema},
	}

	mainContainer := corev1.Container{
		Name:            "dbt-job",
		Image:           image,
		ImagePullPolicy: validationImagePullPolicy(),
		Command:         validationmodel.ValidationCommand(p.NodeType, p.TableName),
		Env:             mainEnv,
		// Warehouse connection is operator-owned: the whole Secret lands as env.
		EnvFrom: whFrom,
	}
	mainContainer.SecurityContext = continuoImageSecurityContext()

	switch op {
	case "clone_from_prod":
		// No candidate SQL, no S3, single container.
		return corev1.PodSpec{
			RestartPolicy:   corev1.RestartPolicyNever,
			SecurityContext: jobPodSecurityContext(),
			Containers:      []corev1.Container{mainContainer},
		}, nil

	case "build_from_sql":
		// The validation container fetches its own compiled SQL from S3 (boto3) and
		// builds it WITH NO DATA — no sidecar, no shared emptyDir. CandidateArtifactURI must
		// be set: changed models/snapshots always carry one; nodes without candidate SQL
		// (unchanged upstreams, seeds) use clone_from_prod.
		if p.CandidateArtifactURI == "" {
			return corev1.PodSpec{}, fmt.Errorf("%w: candidate_artifact_uri missing from build_from_sql validation job params for node %s",
				events.ErrPermanent, p.NodeID)
		}
		mainContainer.Env = append(mainContainer.Env, corev1.EnvVar{Name: "CANDIDATE_SQL_URI", Value: p.CandidateArtifactURI})
		mainContainer.Env = append(mainContainer.Env, s3CredEnvVars()...)
		return corev1.PodSpec{
			RestartPolicy:   corev1.RestartPolicyNever,
			SecurityContext: jobPodSecurityContext(),
			Containers:      []corev1.Container{mainContainer},
		}, nil

	case "build_from_columns":
		// Python nodes: the runner fetches the JSON validation spec (declared
		// reads + output columns) from S3, bind-checks each read, and creates
		// the empty typed table. CANDIDATE_SPEC_URI is the published runner's
		// env contract for this op — CANDIDATE_SQL_URI belongs to
		// build_from_sql and is never set here.
		if p.CandidateArtifactURI == "" {
			return corev1.PodSpec{}, fmt.Errorf("%w: candidate_artifact_uri missing from build_from_columns validation job params for node %s",
				events.ErrPermanent, p.NodeID)
		}
		mainContainer.Env = append(mainContainer.Env, corev1.EnvVar{Name: "CANDIDATE_SPEC_URI", Value: p.CandidateArtifactURI})
		mainContainer.Env = append(mainContainer.Env, s3CredEnvVars()...)
		return corev1.PodSpec{
			RestartPolicy:   corev1.RestartPolicyNever,
			SecurityContext: jobPodSecurityContext(),
			Containers:      []corev1.Container{mainContainer},
		}, nil

	default:
		return corev1.PodSpec{}, fmt.Errorf("%w: unknown validation_op %q for node %s",
			events.ErrPermanent, op, p.NodeID)
	}
}

// Candidate-schema lifecycle ops run as one-shot engine-image Jobs: the executor
// schedules them and blocks on the result but never connects to the warehouse itself.
const (
	schemaOpEnsure = "ensure_schema"
	schemaOpDrop   = "drop_schema"

	schemaOpJobPollInterval = 2 * time.Second
	// SchemaOpJobTimeout bounds the wait for a schema-op Job to terminate; the DDL is
	// quick, so a Job still running past this is a failure rather than an infinite
	// block. Exported so main.go can size the handler budget of the stream consumers
	// that block on schema-op Jobs above this wait, not under it.
	SchemaOpJobTimeout = 5 * time.Minute
)

// buildSchemaOpPodSpec constructs the PodSpec for a candidate-schema lifecycle Job. It
// runs the same engine image as validation (VALIDATION_IMAGE) with the operator's
// warehouse Secret attached via envFrom, invoking the harness ensure_schema/drop_schema
// op on DBT_TARGET_SCHEMA — a single short-lived container that runs one DDL statement
// through the engine adapter and exits. No S3, no candidate SQL, no table.
func buildSchemaOpPodSpec(op, candidateSchema string) (corev1.PodSpec, error) {
	image := os.Getenv("VALIDATION_IMAGE")
	if image == "" {
		return corev1.PodSpec{}, fmt.Errorf("%w: VALIDATION_IMAGE not configured (set it to the matching continuo-validation-<engine> image) for schema op %s",
			events.ErrPermanent, op)
	}
	whFrom, err := warehouseSecretEnvFrom("schema op " + op)
	if err != nil {
		return corev1.PodSpec{}, err
	}
	container := corev1.Container{
		Name:            "schema-op",
		Image:           image,
		ImagePullPolicy: validationImagePullPolicy(),
		// Command unset: run the image's default entrypoint (python /validation_runner.py).
		Env: []corev1.EnvVar{
			{Name: "DBT_TARGET_SCHEMA", Value: candidateSchema},
			{Name: "VALIDATION_OP", Value: op},
		},
		// Warehouse connection is operator-owned: the whole Secret lands as env.
		EnvFrom:         whFrom,
		SecurityContext: continuoImageSecurityContext(),
	}
	return corev1.PodSpec{
		RestartPolicy:   corev1.RestartPolicyNever,
		SecurityContext: jobPodSecurityContext(),
		Containers:      []corev1.Container{container},
	}, nil
}

// schemaOpJob wraps buildSchemaOpPodSpec in a one-shot Job. It carries a distinct
// app=continuo-schema-op label (never mode=validation/app=dbt-job) so k8s-controller's
// validation watcher ignores it — its lifecycle is owned here, not surfaced as a
// validation node. TTLSecondsAfterFinished is a cleanup backstop; RunSchemaOpJob also
// deletes the Job once it observes a terminal state. ActiveDeadlineSeconds matches
// SchemaOpJobTimeout so a hung DDL pod (e.g. a stuck warehouse lock) is killed and the
// Job goes Failed — a terminal state submitSchemaOpJob can clear on the next retry —
// instead of staying Active forever and livelocking every retry at the wait timeout.
func schemaOpJob(op, candidateSchema, jobName, namespace string) (*batchv1.Job, error) {
	podSpec, err := buildSchemaOpPodSpec(op, candidateSchema)
	if err != nil {
		return nil, err
	}
	backoffLimit := int32(0)
	ttl := int32(120)
	activeDeadline := int64(SchemaOpJobTimeout / time.Second)
	labels := map[string]string{
		"app":              "continuo-schema-op",
		"schema-op":        op,
		"candidate-schema": sanitizeK8sLabel(candidateSchema),
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: namespace, Labels: labels},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			ActiveDeadlineSeconds:   &activeDeadline,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       podSpec,
			},
		},
	}, nil
}

// schemaOpJobTerminal reports whether the Job has reached a terminal state. Pod
// counters alone are not enough: a Job killed by ActiveDeadlineSeconds surfaces as a
// JobFailed condition (reason DeadlineExceeded) and may leave the Failed counter at
// zero, so conditions are checked too.
func schemaOpJobTerminal(job *batchv1.Job) (succeeded, failed bool) {
	succeeded = job.Status.Succeeded > 0
	failed = job.Status.Failed > 0
	for _, cond := range job.Status.Conditions {
		if cond.Status != corev1.ConditionTrue {
			continue
		}
		switch cond.Type {
		case batchv1.JobComplete:
			succeeded = true
		case batchv1.JobFailed:
			failed = true
		}
	}
	return succeeded, failed
}

// RunSchemaOpJob schedules a candidate-schema lifecycle Job and blocks until it
// terminates: Succeeded -> nil, Failed (or timeout) -> error. It is idempotent by job
// name — a redelivered trigger waits on the in-flight Job instead of duplicating it —
// and a leftover terminal Job from a prior attempt is cleared first so a retry runs
// clean. The engine adapter runs the DDL; the executor holds no warehouse connection.
func (c *K8sClient) RunSchemaOpJob(ctx context.Context, op, candidateSchema, jobName, namespace string) error {
	if err := c.submitSchemaOpJob(ctx, op, candidateSchema, jobName, namespace); err != nil {
		return err
	}
	return c.waitForSchemaOpJob(ctx, namespace, jobName, op)
}

// submitSchemaOpJob ensures exactly one runnable schema-op Job exists for jobName:
// creates it when absent, clears-and-recreates a leftover terminal Job from a prior
// attempt, and leaves an in-flight Job untouched (a redelivered trigger just waits).
func (c *K8sClient) submitSchemaOpJob(ctx context.Context, op, candidateSchema, jobName, namespace string) error {
	job, err := schemaOpJob(op, candidateSchema, jobName, namespace)
	if err != nil {
		return err
	}

	existing, getErr := c.clientset.BatchV1().Jobs(namespace).Get(ctx, jobName, metav1.GetOptions{})
	switch {
	case getErr == nil:
		succeeded, failed := schemaOpJobTerminal(existing)
		if succeeded || failed {
			if err := c.deleteJob(ctx, namespace, jobName); err != nil {
				return fmt.Errorf("clear stale schema-op job %s: %w", jobName, err)
			}
			return c.createSchemaOpJob(ctx, job)
		}
		c.logger.InfoContext(ctx, "schema-op job already in flight; waiting", "job_name", jobName, "op", op)
		return nil
	case errors.IsNotFound(getErr):
		return c.createSchemaOpJob(ctx, job)
	default:
		return fmt.Errorf("get schema-op job %s: %w", jobName, getErr)
	}
}

// createSchemaOpJob creates the Job, treating a lost create race as success: between
// the caller's Get and this Create, a concurrent redelivery (another replica or
// consumer goroutine) may have created the same name first. The Job is deterministic
// by name and its op is idempotent, so the caller just waits on whichever Job now
// holds the name instead of surfacing AlreadyExists and burning a retry cycle.
func (c *K8sClient) createSchemaOpJob(ctx context.Context, job *batchv1.Job) error {
	err := c.CreateJob(ctx, job)
	if err != nil && errors.IsAlreadyExists(err) {
		c.logger.InfoContext(ctx, "schema-op job created concurrently; waiting", "job_name", job.Name)
		return nil
	}
	return err
}

func (c *K8sClient) waitForSchemaOpJob(ctx context.Context, namespace, jobName, op string) error {
	waitCtx, cancel := context.WithTimeout(ctx, SchemaOpJobTimeout)
	defer cancel()
	ticker := time.NewTicker(schemaOpJobPollInterval)
	defer ticker.Stop()
	for {
		job, err := c.clientset.BatchV1().Jobs(namespace).Get(waitCtx, jobName, metav1.GetOptions{})
		switch {
		case err == nil:
			succeeded, failed := schemaOpJobTerminal(job)
			if succeeded {
				c.logger.InfoContext(ctx, "schema-op job succeeded", "job_name", jobName, "op", op)
				_ = c.deleteJob(ctx, namespace, jobName)
				return nil
			}
			if failed {
				return fmt.Errorf("schema-op job %s (%s) failed", jobName, op)
			}
		case errors.IsNotFound(err):
			// Schema-op Jobs are deleted only after a terminal state: delete-on-success
			// (possibly by a concurrent waiter on the same Job) or the 120s
			// TTLSecondsAfterFinished backstop — both far longer than the 2s poll, so a
			// failure would have been observed here first. Treat the disappearance as
			// success instead of polling into the timeout.
			c.logger.InfoContext(ctx, "schema-op job already completed and was cleaned up",
				"job_name", jobName, "op", op)
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("schema-op job %s (%s) did not terminate: %w", jobName, op, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (c *K8sClient) deleteJob(ctx context.Context, namespace, jobName string) error {
	policy := metav1.DeletePropagationBackground
	err := c.clientset.BatchV1().Jobs(namespace).Delete(ctx, jobName, metav1.DeleteOptions{PropagationPolicy: &policy})
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
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

// jobPodSecurityContext returns the pod-level hardening applied to every
// executor-created Job pod: the runtime default seccomp profile.
func jobPodSecurityContext() *corev1.PodSecurityContext {
	return &corev1.PodSecurityContext{
		SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
}

// baseContainerSecurityContext hardens a container regardless of which image
// it runs (team images included): no privilege escalation, no capabilities.
// The container user is intentionally left to the image — team images choose
// their own user (see the dbt image contract).
func baseContainerSecurityContext() *corev1.SecurityContext {
	no := false
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: &no,
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
}

// continuoImageSecurityContext extends the base hardening with a forced
// non-root user for containers running the continuo-validation-<engine> and
// s3-sidecar images. uid 65532 is a cross-repo contract, not a locally
// observed fact: the s3-sidecar image is built in this repository, but
// continuo-validation-<engine> is built and released from
// github.com/carolsimone/continuo-validation, so this uid tracks that
// repository's `useradd --uid` and changes there require a coordinated
// change here.
func continuoImageSecurityContext() *corev1.SecurityContext {
	sc := baseContainerSecurityContext()
	yes := true
	uid := int64(65532)
	sc.RunAsNonRoot = &yes
	sc.RunAsUser = &uid
	return sc
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

// warehouseSecretEnvFrom returns the envFrom source that attaches the
// operator-owned warehouse Secret (named by VALIDATION_WAREHOUSE_SECRET) to a
// dbt-running container. dbt team profiles read the Secret's engine-native
// keys (POSTGRES_*, TRINO_*, ...) directly; the executor forwards no warehouse
// connection env of its own. The compile parse rehearsal containers and every
// dbt run/seed pod MUST attach this same source — connection drift between
// them silently invalidates every hydrated partial-parse cache.
func warehouseSecretEnvFrom(subject string) ([]corev1.EnvFromSource, error) {
	name := os.Getenv("VALIDATION_WAREHOUSE_SECRET")
	if name == "" {
		return nil, fmt.Errorf("%w: validation warehouse secret not configured (set VALIDATION_WAREHOUSE_SECRET) for %s",
			events.ErrPermanent, subject)
	}
	return []corev1.EnvFromSource{{
		SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: name},
		},
	}}, nil
}

// parseCacheInitContainer returns the s3-sidecar initContainer + emptyDir
// volume + team-container mount that hydrate a dbt Job with the release-proven
// partial-parse artifact. The fetcher NEVER fails the Job: a missing or
// unfetchable artifact degrades to a full parse (logged + termination message
// "degraded:<reason>", exit 0). targetDir is where the team's dbt looks for
// partial_parse.msgpack (dirname of the resolved partial-parse path).
func parseCacheInitContainer(cacheURI, targetDir string) (corev1.Container, corev1.Volume, corev1.VolumeMount) {
	vol := corev1.Volume{
		Name:         "parse-cache",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}
	teamMount := corev1.VolumeMount{Name: "parse-cache", MountPath: targetDir}
	init := corev1.Container{
		Name:            parsecache.ContainerName,
		Image:           s3SidecarImage(),
		ImagePullPolicy: validationImagePullPolicy(),
		Command:         []string{"python", "/parse_cache_fetcher.py"},
		Env: append([]corev1.EnvVar{
			{Name: "PARSE_CACHE_S3_URI", Value: cacheURI},
			{Name: "PARSE_CACHE_DEST", Value: "/parse-cache/partial_parse.msgpack"},
		}, s3CredEnvVars()...),
		VolumeMounts:    []corev1.VolumeMount{{Name: "parse-cache", MountPath: "/parse-cache"}},
		SecurityContext: continuoImageSecurityContext(),
	}
	return init, vol, teamMount
}

// buildParseExportCommand returns the sh script for one parse-export/rehearsal
// initContainer. It runs the team's parse argv cold (export), then re-runs the
// same argv at DBT_LOG_LEVEL=debug (rehearsal) and greps the debug log for one
// of three mutually exclusive dbt markers, empirically pinned against real dbt
// (dbt-core==1.12.0b1) in dbt/tests/test_parse_rehearsal.py:
//
//   - "Partial parsing not enabled": partial parsing is DISABLED for this
//     project (flags.partial_parse: false or --no-partial-parse) — the run
//     pods can never use the exported cache. exit 43.
//   - "Unable to do partial parsing": the project re-parses under run-pod
//     conditions, typically an env_var() read at parse time (e.g.
//     DBT_TARGET_SCHEMA) whose value differs between compile and run pods.
//     exit 42.
//   - neither marker present and "skipping partial parsing" is ALSO absent:
//     the rehearsal did not report a clean partial-parse hit for a reason
//     this gate does not recognize. exit 45.
//
// dbt writes partial_parse.msgpack unconditionally on every successful parse
// regardless of the partial_parse flag — the flag only suppresses *reading*
// an existing cache, never *writing* one — so a missing msgpack after run 1
// is never a disabled-project signal; it means the parse command did not
// write to the configured target-path at all, and the script fails loudly
// (exit 46) before ever reaching the rehearsal. Only a run 2 that hits none
// of the three markers' failure conditions hands the exported artifact to
// the upload container via /shared.
func buildParseExportCommand(parseArgv []string, partialParsePath, ctx string) string {
	dir := "/shared/parse/" + ctx
	parse := shellJoin(parseArgv)
	pp := shellQuote(partialParsePath)
	return "set -e\n" +
		"mkdir -p " + dir + "\n" +
		"chmod 755 " + dir + "\n" +
		parse + "\n" +
		"[ -f " + pp + " ] || { echo 'continuo parse-export: the parse command completed without writing partial_parse.msgpack at " + pp + " — check compile.partial_parse_path in dbt-commands.yaml.' >&2; exit 46; }\n" +
		"DBT_LOG_LEVEL=debug " + parse + " > " + dir + "/rehearse.log 2>&1 || { cat " + dir + "/rehearse.log >&2; exit 44; }\n" +
		"if grep -q 'Partial parsing not enabled' " + dir + "/rehearse.log; then\n" +
		"  cat " + dir + "/rehearse.log >&2\n" +
		"  echo 'continuo parse-rehearsal FAILED (" + ctx + "): partial parsing is DISABLED in this project (flags: partial_parse: false or --no-partial-parse in the parse command) — the run pods can never use the exported cache. This is not a SQL error.' >&2\n" +
		"  exit 43\n" +
		"elif grep -q 'Unable to do partial parsing' " + dir + "/rehearse.log; then\n" +
		"  cat " + dir + "/rehearse.log >&2\n" +
		"  echo 'continuo parse-rehearsal FAILED (" + ctx + "): the project re-parses under run-pod conditions — typically an env_var() read at parse time whose value differs between compile and run pods. This is not a SQL error.' >&2\n" +
		"  exit 42\n" +
		"elif ! grep -q 'skipping partial parsing' " + dir + "/rehearse.log; then\n" +
		"  cat " + dir + "/rehearse.log >&2\n" +
		"  echo 'continuo parse-rehearsal FAILED (" + ctx + "): the second parse did not report a clean partial-parse hit. This is not a SQL error.' >&2\n" +
		"  exit 45\n" +
		"fi\n" +
		"cp " + pp + " " + dir + "/partial_parse.msgpack\n" +
		"chmod 644 " + dir + "/partial_parse.msgpack\n"
}

// sharedEmptyDirVolume returns the "shared" emptyDir volume used as the hand-off
// point between the init container and the main container in compile pods.
func sharedEmptyDirVolume() corev1.Volume {
	return corev1.Volume{
		Name:         "shared",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}
}

// sharedVolumeMount returns the VolumeMount for the "shared" emptyDir volume,
// mounting it at /shared in a container.
func sharedVolumeMount() corev1.VolumeMount {
	return corev1.VolumeMount{Name: "shared", MountPath: "/shared"}
}

// validationImagePullPolicy resolves the pull policy applied to both the
// validation main container and the compile leg's s3-sidecar upload container.
//
// The default is PullAlways so that when an image reference is a mutable tag a
// re-push is picked up on the next Job. When both images are pinned to a fixed
// tag, PullAlways costs only a cheap digest check.
//
// e2e and local clusters side-load images directly into the node's image cache
// and set VALIDATION_IMAGE_PULL_POLICY to IfNotPresent or Never so the cached
// image is used instead of failing with ErrImagePull.
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
		c.commands.SeedBuildCommand(params.ServiceName, params.TableName, params.CandidateSchema),
		c.commands.PartialParsePath(params.ServiceName))
	if err != nil {
		return fmt.Errorf("failed to build seed-build pod spec: %w", err)
	}

	backoffLimit := int32(0)
	labels := map[string]string{
		"app":          "dbt-job",
		"mode":         events.ModeSeedBuild,
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
	ttl := jobTTLSecondsAfterFinished
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        params.JobName,
			Namespace:   params.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations},
				Spec:       podSpec,
			},
		},
	}

	return c.CreateJob(ctx, job)
}

// buildSeedBuildPodSpec constructs the PodSpec for a seed-build Job.
// It mirrors buildPodSpec (production team image + warehouse-Secret envFrom +
// SCHEMA/TABLE_NAME env) and adds DBT_TARGET_SCHEMA=<CandidateSchema> so the
// generate_schema_name macro materializes the seed into the candidate schema.
// ImageTag must be non-empty — the team image must be explicitly versioned.
// When S3_BUCKET is set, the pod also gets a hydrate-parse-cache initContainer
// that pre-seeds the candidate-context partial-parse artifact (see
// parseCacheInitContainer); partialParsePath is the service's resolved
// partial_parse.msgpack path, used only to derive the team container's mount
// directory.
func buildSeedBuildPodSpec(p ValidationJobParams, command []string, partialParsePath string) (corev1.PodSpec, error) {
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
	}
	whFrom, err := warehouseSecretEnvFrom("seed-build job " + p.JobName)
	if err != nil {
		return corev1.PodSpec{}, err
	}

	spec := corev1.PodSpec{
		RestartPolicy:   corev1.RestartPolicyNever,
		SecurityContext: jobPodSecurityContext(),
		Containers: []corev1.Container{
			{
				Name:            "dbt-job",
				Image:           image,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         command,
				Env:             envVars,
				EnvFrom:         whFrom,
				SecurityContext: baseContainerSecurityContext(),
			},
		},
	}

	if bucket := os.Getenv("S3_BUCKET"); bucket != "" {
		uri := artifacts.ParseCacheCandidateURI(bucket, p.ServiceName, p.ReleaseID)
		init, vol, teamMount := parseCacheInitContainer(uri, path.Dir(partialParsePath))
		spec.InitContainers = []corev1.Container{init}
		spec.Volumes = []corev1.Volume{vol}
		spec.Containers[0].VolumeMounts = []corev1.VolumeMount{teamMount}
	}

	return spec, nil
}

// CreateCompileJob builds and creates a mode=compile K8s Job (idempotent by
// job name). The Job pod has a shared emptyDir volume "shared" mounted at
// /shared in every container:
//   - initContainer "compile": team image runs the service's resolved compile
//     command and copies the manifest from its declared path into
//     /shared/manifest.json, with the warehouse-Secret envFrom attached.
//   - when params.CandidateSchema is set, two more team-image initContainers,
//     "parse-prod" and "parse-candidate", export and rehearse the service's
//     partial-parse cache (see buildParseExportCommand) into
//     /shared/parse/<prod|candidate>/partial_parse.msgpack.
//   - main container "upload": the shared s3-sidecar image (S3_SIDECAR_IMAGE
//     env, else <DOCKERHUB_USERNAME>/s3-sidecar:latest) runs
//     `python /compile_uploader.py` with COMPILE_MANIFEST_PATH +
//     MANIFEST_S3_URI + the S3 credential envs, plus the four PARSE_* envs
//     when the parse-export leg ran.
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

	compileArgv, manifestPath := c.commands.CompileCommand(params.ServiceName)
	parseArgv := c.commands.ParseCommand(params.ServiceName)
	partialParsePath := c.commands.PartialParsePath(params.ServiceName)
	podSpec, err := buildCompilePodSpec(params, compileArgv, manifestPath, parseArgv, partialParsePath)
	if err != nil {
		return fmt.Errorf("failed to build compile pod spec: %w", err)
	}

	backoffLimit := int32(0)
	labels := map[string]string{
		"app":          "dbt-job",
		"mode":         events.ModeCompile,
		"release-id":   sanitizeK8sLabel(params.ReleaseID),
		"node-id":      sanitizeK8sLabel(params.NodeID),
		"service_name": params.ServiceName,
	}
	annotations := map[string]string{
		pkg_model.AnnotationReleaseID: params.ReleaseID,
		pkg_model.AnnotationNodeID:    params.NodeID,
	}
	ttl := jobTTLSecondsAfterFinished
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        params.JobName,
			Namespace:   params.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations},
				Spec:       podSpec,
			},
		},
	}

	return c.CreateJob(ctx, job)
}

// buildCompilePodSpec constructs the PodSpec for a compile Job. The pod has:
//   - a shared emptyDir volume "shared" mounted at /shared in every container;
//   - an initContainer "compile" using the team image (ImageTag must be
//     non-empty) that runs the service's resolved compile command, copies
//     the manifest from its declared path into /shared/manifest.json, and
//     chmods it 644 so it is world-readable regardless of the team image's
//     uid/umask (the upload container below runs as a fixed, different uid);
//   - when p.CandidateSchema is non-empty, two more team-image initContainers,
//     "parse-prod" and "parse-candidate", that export and rehearse the
//     service's partial-parse cache under prod and candidate connection
//     contexts respectively (see buildParseExportCommand) into
//     /shared/parse/<ctx>/partial_parse.msgpack; when CandidateSchema is
//     empty the parse-export leg is disabled and the pod keeps the
//     two-container layout (compile initContainer + upload main container);
//   - a main container "upload" using the shared s3-sidecar image (no dbt;
//     S3_SIDECAR_IMAGE env, else <DOCKERHUB_USERNAME>/s3-sidecar:latest)
//     that runs `python /compile_uploader.py` with COMPILE_MANIFEST_PATH,
//     MANIFEST_S3_URI, and the S3 credential envs forwarded from the
//     executor-controller env, plus the four PARSE_* envs (local paths +
//     S3 destinations) when the parse-export leg ran.
func buildCompilePodSpec(p ValidationJobParams, compileArgv []string, manifestPath string, parseArgv []string, partialParsePath string) (corev1.PodSpec, error) {
	if p.ImageTag == "" {
		return corev1.PodSpec{}, fmt.Errorf("%w: image_tag missing from compile job params for service %s",
			events.ErrPermanent, p.ServiceName)
	}

	// Team image for the init (compile, parse-prod, parse-candidate) containers.
	teamImage := p.ServiceName + ":" + p.ImageTag
	if user := os.Getenv("DOCKERHUB_USERNAME"); user != "" {
		teamImage = user + "/" + teamImage
	}

	// Upload container image: the shared minimal python+boto3 sidecar (NO dbt).
	uploadImage := s3SidecarImage()

	// Warehouse connection for every dbt-running init container (compile and the
	// parse legs): the operator-owned Secret, attached via envFrom.
	whFrom, err := warehouseSecretEnvFrom("compile job for service " + p.ServiceName)
	if err != nil {
		return corev1.PodSpec{}, err
	}

	mount := sharedVolumeMount()

	// compile_uploader.py parses the bucket from MANIFEST_S3_URI; S3_BUCKET is
	// intentionally omitted (see s3CredEnvVars).
	uploadEnvVars := append([]corev1.EnvVar{
		{Name: "COMPILE_MANIFEST_PATH", Value: "/shared/manifest.json"},
		{Name: "MANIFEST_S3_URI", Value: p.ManifestS3URI},
	}, s3CredEnvVars()...)

	spec := corev1.PodSpec{
		RestartPolicy:   corev1.RestartPolicyNever,
		SecurityContext: jobPodSecurityContext(),
		Volumes:         []corev1.Volume{sharedEmptyDirVolume()},
		InitContainers: []corev1.Container{
			{
				Name:            "compile",
				Image:           teamImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command: []string{
					"sh", "-c",
					shellJoin(compileArgv) + " && cp " + shellQuote(manifestPath) + " /shared/manifest.json" +
						" && chmod 644 /shared/manifest.json",
				},
				EnvFrom:         whFrom,
				VolumeMounts:    []corev1.VolumeMount{mount},
				SecurityContext: baseContainerSecurityContext(),
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
				SecurityContext: continuoImageSecurityContext(),
			},
		},
	}

	if p.CandidateSchema != "" {
		candEnv := []corev1.EnvVar{{Name: "DBT_TARGET_SCHEMA", Value: p.CandidateSchema}}
		spec.InitContainers = append(spec.InitContainers,
			corev1.Container{
				Name:            "parse-prod",
				Image:           teamImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         []string{"sh", "-c", buildParseExportCommand(parseArgv, partialParsePath, "prod")},
				EnvFrom:         whFrom,
				VolumeMounts:    []corev1.VolumeMount{mount},
				SecurityContext: baseContainerSecurityContext(),
			},
			corev1.Container{
				Name:            "parse-candidate",
				Image:           teamImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         []string{"sh", "-c", buildParseExportCommand(parseArgv, partialParsePath, "candidate")},
				Env:             candEnv,
				EnvFrom:         whFrom,
				VolumeMounts:    []corev1.VolumeMount{mount},
				SecurityContext: baseContainerSecurityContext(),
			})
		spec.Containers[0].Env = append(spec.Containers[0].Env,
			corev1.EnvVar{Name: "PARSE_PROD_LOCAL_PATH", Value: "/shared/parse/prod/partial_parse.msgpack"},
			corev1.EnvVar{Name: "PARSE_PROD_S3_URI", Value: p.ParseProdS3URI},
			corev1.EnvVar{Name: "PARSE_CANDIDATE_LOCAL_PATH", Value: "/shared/parse/candidate/partial_parse.msgpack"},
			corev1.EnvVar{Name: "PARSE_CANDIDATE_S3_URI", Value: p.ParseCandidateS3URI},
		)
	}

	return spec, nil
}

// setClientsetForTest swaps the clientset for a fake in unit tests.
func (c *K8sClient) setClientsetForTest(cs kubernetes.Interface) { c.clientset = cs }

// buildPodSpec constructs the PodSpec for a query executor job.
// The warehouse connection arrives via envFrom of the operator-owned Secret named
// by VALIDATION_WAREHOUSE_SECRET; the team image's dbt profile reads the Secret's
// engine-native keys (POSTGRES_*, TRINO_*, ...).
// Returns an error if ImageTag is empty — content-addressed tags must be explicit;
// falling back to "latest" is intentionally refused.
// When S3_BUCKET is set, the pod also gets a hydrate-parse-cache initContainer
// that pre-seeds the prod-context partial-parse artifact (see
// parseCacheInitContainer); partialParsePath is the service's resolved
// partial_parse.msgpack path, used only to derive the team container's mount
// directory.
func buildPodSpec(params JobParams, command []string, partialParsePath string) (corev1.PodSpec, error) {
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
	}
	whFrom, err := warehouseSecretEnvFrom("dbt job " + params.JobName)
	if err != nil {
		return corev1.PodSpec{}, err
	}

	spec := corev1.PodSpec{
		RestartPolicy:   corev1.RestartPolicyNever,
		SecurityContext: jobPodSecurityContext(),
		Containers: []corev1.Container{
			{
				Name:            "dbt-job",
				Image:           image,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         command,
				Env:             envVars,
				EnvFrom:         whFrom,
				SecurityContext: baseContainerSecurityContext(),
			},
		},
	}

	if bucket := os.Getenv("S3_BUCKET"); bucket != "" {
		uri := artifacts.ParseCacheProdURI(bucket, params.ServiceName, params.ImageTag)
		init, vol, teamMount := parseCacheInitContainer(uri, path.Dir(partialParsePath))
		spec.InitContainers = []corev1.Container{init}
		spec.Volumes = []corev1.Volume{vol}
		spec.Containers[0].VolumeMounts = []corev1.VolumeMount{teamMount}
	}

	return spec, nil
}

// pythonImageHasExplicitTag reports whether ref names an explicit tag or
// digest. A reference without one resolves to :latest implicitly, which makes
// the code that actually ran unidentifiable from the release that promoted it.
// The tag separator is searched only in the final path segment, so the port in
// a registry host ("registry.local:5000/name") is not mistaken for one.
func pythonImageHasExplicitTag(ref string) bool {
	if strings.Contains(ref, "@") {
		return true
	}
	lastSegment := ref
	if i := strings.LastIndex(ref, "/"); i >= 0 {
		lastSegment = ref[i+1:]
	}
	return strings.Contains(lastSegment, ":")
}

// buildPythonPodSpec constructs the PodSpec for a python-model run Job.
//
// The pod is a single container running the node's own image verbatim: the
// release's image_tag is a complete registry reference, built by the domain
// repository FROM the engine-matched continuo-python-runtime base, and that
// image's entrypoint is the harness that selects the node from the contract
// files baked inside it. The executor therefore sets no command and resolves no
// dbt command dialect.
//
// The environment is exactly the three variables the harness requires: NODE_ID
// (whose trailing schema.table segments select the node), TABLE_NAME, and
// TARGET_SCHEMA. These deliberately differ from the dbt Job env — the harness
// recognizes no SCHEMA/DBT_TARGET_SCHEMA fallback — and CONTRACT_DIR and
// APP_ROOT are left to the image, which declares its own layout. The warehouse
// connection arrives via envFrom of the same operator-owned Secret dbt Jobs
// attach; its engine-native keys are what the image's baked runtime adapter
// reads.
//
// None of the dbt Job's pod plumbing applies: no parse-cache hydrate
// initContainer, no shared volume, and no S3 credentials, which a domain image
// never receives — its contract files travel inside the image itself.
func buildPythonPodSpec(p JobParams) (corev1.PodSpec, error) {
	switch p.Operation {
	case pkg_model.OperationRun, pkg_model.OperationBuild:
		// build materializes and tests in one step; a python node declares no
		// tests, so it reduces to the same dispatch as run.
	default:
		return corev1.PodSpec{}, fmt.Errorf("%w: operation %q is not supported for python-model node %s.%s",
			events.ErrPermanent, p.Operation, p.SchemaName, p.TableName)
	}

	if p.ImageTag == "" {
		return corev1.PodSpec{}, fmt.Errorf("%w: image_tag missing from job params for python-model node %s.%s",
			events.ErrPermanent, p.SchemaName, p.TableName)
	}
	if !pythonImageHasExplicitTag(p.ImageTag) {
		return corev1.PodSpec{}, fmt.Errorf("%w: image_tag %q for python-model node %s.%s carries no explicit tag or digest",
			events.ErrPermanent, p.ImageTag, p.SchemaName, p.TableName)
	}

	whFrom, err := warehouseSecretEnvFrom("python job " + p.JobName)
	if err != nil {
		return corev1.PodSpec{}, err
	}

	return corev1.PodSpec{
		RestartPolicy:   corev1.RestartPolicyNever,
		SecurityContext: jobPodSecurityContext(),
		Containers: []corev1.Container{
			{
				Name:            "python-job",
				Image:           p.ImageTag,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Env: []corev1.EnvVar{
					{Name: "NODE_ID", Value: p.SchemaName + "." + p.TableName},
					{Name: "TABLE_NAME", Value: p.TableName},
					{Name: "TARGET_SCHEMA", Value: p.SchemaName},
				},
				EnvFrom:         whFrom,
				SecurityContext: baseContainerSecurityContext(),
			},
		},
	}, nil
}
