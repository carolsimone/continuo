package k8s

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path"
	"regexp"

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
		c.commands.NodeCommand(params.ServiceName, params.Operation, params.NodeType, params.TableName),
		c.commands.PartialParsePath(params.ServiceName))
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
// Validation runs a slim continuo-owned validation-runner image (PostgreSQL today)
// that bakes one engine adapter library — never the per-service team image. The SRE
// selects the engine at deploy time (Helm/compose), which resolves VALIDATION_IMAGE
// to the matching continuo-validation-runner-<engine> image; the executor runs it
// verbatim. The warehouse connection is not injected inline: the operator-owned
// Secret named by VALIDATION_WAREHOUSE_SECRET is attached to the container via
// envFrom, so the validation credentials are owned by the operator and separate from
// both the executor's own DB and the dbt team images.
//
// build_from_sql nodes receive CANDIDATE_SQL_URI + S3 credentials directly on the
// single main container; the runner fetches the compiled SQL from S3 itself. There
// is no init container and no shared emptyDir for this path. clone_from_prod nodes
// have no candidate SQL and never touch S3, so they remain single-container with no
// emptyDir and no S3 credentials.
func buildValidationPodSpec(p ValidationJobParams) (corev1.PodSpec, error) {
	// VALIDATION_IMAGE pins the validator image (Helm/compose set it per the chosen
	// engine). When unset it defaults to the postgres runner.
	image := os.Getenv("VALIDATION_IMAGE")
	if image == "" {
		image = "ghcr.io/carolsimone/continuo-validation-runner-postgres:latest"
	}

	// The validation container's warehouse credentials come from an operator-owned
	// Secret (envFrom), not inline env. Without it validation cannot connect, so
	// fail the node permanently with an actionable reason rather than launch a pod
	// that can only error.
	warehouseSecret := os.Getenv("VALIDATION_WAREHOUSE_SECRET")
	if warehouseSecret == "" {
		return corev1.PodSpec{}, fmt.Errorf("%w: validation warehouse secret not configured (set VALIDATION_WAREHOUSE_SECRET) for node %s",
			events.ErrPermanent, p.NodeID)
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
		EnvFrom: []corev1.EnvFromSource{{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: warehouseSecret},
			},
		}},
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
			RestartPolicy:   corev1.RestartPolicyNever,
			SecurityContext: jobPodSecurityContext(),
			Containers:      []corev1.Container{mainContainer},
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
// non-root user for containers running the validation-runner and s3-sidecar
// images, which are built with uid 65532.
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

// dbtConnectionEnvVars is the release-independent dbt profile env every
// dbt-running container receives. It is the ONLY env the parse rehearsal can
// legitimately depend on: per-node vars (TASK_ID, TABLE_NAME, SCHEMA, ...) are
// not rehearsable and DBT_TARGET_SCHEMA is added per candidate context. The
// parse containers and the run pods MUST build from this same function — env
// drift between them silently invalidates every hydrated cache.
func dbtConnectionEnvVars() []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "DBT_POSTGRES_HOST", Value: os.Getenv("POSTGRES_HOST")},
		{Name: "DBT_POSTGRES_PORT", Value: os.Getenv("POSTGRES_PORT")},
		{Name: "DBT_POSTGRES_DB", Value: os.Getenv("DBT_POSTGRES_DB")},
		{Name: "DBT_POSTGRES_USER", Value: os.Getenv("POSTGRES_USER")},
		{Name: "DBT_POSTGRES_PASSWORD", Value: os.Getenv("POSTGRES_PASSWORD")},
	}
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
	envVars = append(envVars, dbtConnectionEnvVars()...)

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
//     /shared/manifest.json, with the standard DBT_POSTGRES_* warehouse envs.
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
				Env:             dbtConnectionEnvVars(),
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
		prodEnv := dbtConnectionEnvVars()
		candEnv := append(dbtConnectionEnvVars(),
			corev1.EnvVar{Name: "DBT_TARGET_SCHEMA", Value: p.CandidateSchema})
		spec.InitContainers = append(spec.InitContainers,
			corev1.Container{
				Name:            "parse-prod",
				Image:           teamImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         []string{"sh", "-c", buildParseExportCommand(parseArgv, partialParsePath, "prod")},
				Env:             prodEnv,
				VolumeMounts:    []corev1.VolumeMount{mount},
				SecurityContext: baseContainerSecurityContext(),
			},
			corev1.Container{
				Name:            "parse-candidate",
				Image:           teamImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         []string{"sh", "-c", buildParseExportCommand(parseArgv, partialParsePath, "candidate")},
				Env:             candEnv,
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
// PostgreSQL connection env vars are forwarded from the executor-controller's
// own environment so dbt pods can reach the same database.
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
	envVars = append(envVars, dbtConnectionEnvVars()...)

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
