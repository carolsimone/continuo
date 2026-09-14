package k8s

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"

	validationmodel "github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/service/artifacts"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/parsecache"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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

	// Step 2: Build Job spec. Python-family nodes (python-model, python-csv) run
	// the domain repository's own image under the runtime harness's
	// environment; every other node type runs the team's dbt image under a
	// resolved dbt command. The Job metadata below is shared, so both kinds
	// route through the production lifecycle identically.
	var podSpec corev1.PodSpec
	if params.NodeType.IsPython() {
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
		"table_name":   sanitizeK8sLabel(params.TableName),
		"schema_name":  params.SchemaName,
		"service_name": params.ServiceName,
	}
	// The runtime label distinguishes python pods for operators without
	// changing the app selector that CountActive uses for the concurrency cap.
	if params.NodeType.IsPython() {
		jobLabels["runtime"] = "python"
	}
	// Only legacy promote-seed work still supplies a mode here; current jobs get
	// no mode label and route through the production lifecycle.
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

	// SourceOverlayURI locates the source-overlay tarball a verification run
	// lays over a staged copy of the team project before dbt runs. Populated
	// only for mode=compile and mode=seed_build dispatches of a verification
	// run.
	SourceOverlayURI string

	Namespace string
}

// CreateValidationJob builds and creates a mode=validation K8s Job
// (idempotent by job name). The Job carries app=dbt-job so existing watchers
// stay correct, plus the mode=validation label so the job-status handler
// routes its terminal outcome to outcomes.Recorder, which settles it
// in-process. release-id/node-id are stored twice: as sanitized labels (for
// selection/observability) and as raw annotations (the authoritative identity
// echoed into the recorded outcome).
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
		"table_name":   sanitizeK8sLabel(params.TableName),
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
// Validation runs the external continuo-python-runtime-<engine> image (PostgreSQL and
// Trino today) that bakes one engine adapter — never the per-service team image. The
// SRE selects the engine at deploy time (Helm/compose) via VALIDATION_IMAGE; the
// executor runs it with an explicit command (ValidationCommand), because the image's
// default command runs the python-node harness rather than the validation runner.
// The warehouse connection is not injected inline: the operator-owned
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
	// VALIDATION_IMAGE names the engine's continuo-python-runtime-<engine> image; the SRE
	// chooses the engine at deploy time (Helm/compose). The executor bakes in no
	// engine — a hardcoded default would silently force one — so an unset image
	// fails the node permanently with an actionable reason.
	image := os.Getenv("VALIDATION_IMAGE")
	if image == "" {
		return corev1.PodSpec{}, fmt.Errorf("%w: VALIDATION_IMAGE not configured (set it to the matching continuo-python-runtime-<engine> image) for node %s",
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

	case "build_from_sql", "check_binds":
		// The validation container fetches its own compiled SQL from S3 (boto3).
		// build_from_sql builds it WITH NO DATA; check_binds — a dbt test's bind
		// check — only EXPLAINs the SQL and creates nothing. Either way there is no
		// sidecar and no shared emptyDir, and CandidateArtifactURI must be set:
		// changed models, snapshots, and tests always carry one; nodes without
		// candidate SQL (unchanged upstreams, seeds) use clone_from_prod.
		if p.CandidateArtifactURI == "" {
			return corev1.PodSpec{}, fmt.Errorf("%w: candidate_artifact_uri missing from %s validation job params for node %s",
				events.ErrPermanent, op, p.NodeID)
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

// CreateSeedBuildJob builds and creates a mode=seed_build K8s Job (idempotent
// by job name). The Job uses the team image (same as production) and runs
// `dbt seed --select <TableName>`, materializing into the candidate schema via
// DBT_TARGET_SCHEMA so the generate_schema_name macro routes the output there.
// The mode=seed_build label lets the job-status handler route its terminal
// outcome to outcomes.Recorder for in-process settlement.
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
		"table_name":   sanitizeK8sLabel(params.TableName),
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
// When p.SourceOverlayURI is non-empty (a verification run verifying a
// proposed fix), an "overlay" initContainer using the s3-sidecar image fetches
// the proposed source files into /shared/overlay, the pod gets a writable
// workdir emptyDir, and the team container's command is wrapped in `sh -c` and
// prefixed with overlayStagePrefix, so `dbt seed` loads the proposed CSV from
// a staged copy of the project rather than the checked-in one — the same
// treatment buildCompilePodSpec gives its team-image containers.
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

	if p.SourceOverlayURI != "" {
		// A verification run verifies a proposed fix: the overlay fetcher lays
		// the proposed files into /shared/overlay and the team container
		// stages the project into its workdir, lays them over the copy, and
		// seeds from there, so a proposed seed CSV is what gets loaded.
		mount := sharedVolumeMount()
		teamContainer := spec.Containers[0].Name
		spec.InitContainers = append(spec.InitContainers, corev1.Container{
			Name:            "overlay",
			Image:           s3SidecarImage(),
			ImagePullPolicy: validationImagePullPolicy(),
			Command:         []string{"python", "/overlay_fetcher.py"},
			Env: append([]corev1.EnvVar{
				{Name: "SOURCE_OVERLAY_URI", Value: p.SourceOverlayURI},
				{Name: "OVERLAY_DEST", Value: "/shared/overlay"},
			}, s3CredEnvVars()...),
			VolumeMounts:    []corev1.VolumeMount{mount},
			SecurityContext: continuoImageSecurityContext(),
		})
		spec.Volumes = append(spec.Volumes, sharedEmptyDirVolume(), workdirVolume(teamContainer))
		spec.Containers[0].VolumeMounts = append(spec.Containers[0].VolumeMounts,
			mount, workdirVolumeMount(teamContainer))
		spec.Containers[0].Command = []string{"sh", "-c", overlayStagePrefix() + shellJoin(command)}
	}

	if bucket := os.Getenv("S3_BUCKET"); bucket != "" {
		// The cache is hydrated into the directory the team's dbt reads it from
		// inside the image. Under a source overlay the staging copy runs after
		// this initContainer and copies that directory along with the rest of
		// the project, so the hydrated artifact reaches the workdir too.
		uri := artifacts.ParseCacheCandidateURI(bucket, p.ServiceName, p.ReleaseID)
		init, vol, teamMount := parseCacheInitContainer(uri, path.Dir(partialParsePath))
		spec.InitContainers = append(spec.InitContainers, init)
		spec.Volumes = append(spec.Volumes, vol)
		spec.Containers[0].VolumeMounts = append(spec.Containers[0].VolumeMounts, teamMount)
	}

	return spec, nil
}

// CreateCompileJob builds and creates a mode=compile K8s Job (idempotent by
// job name). The Job pod has a shared emptyDir volume "shared" mounted at
// /shared in every container:
//   - when params.SourceOverlayURI is set, initContainer "overlay": the
//     s3-sidecar image fetches a verification run's proposed source files
//     into /shared/overlay before any other team-image init container runs.
//   - initContainer "compile": team image runs the service's resolved compile
//     command and copies the manifest from its declared path into
//     /shared/manifest.json, with the warehouse-Secret envFrom attached; when
//     the overlay ran, the command first stages the project into its own
//     workdir emptyDir with /shared/overlay laid over it and compiles there,
//     reading the manifest back from that copy.
//   - when params.CandidateSchema is set, two more team-image initContainers,
//     "parse-prod" and "parse-candidate", export and rehearse the service's
//     partial-parse cache (see buildParseExportCommand) into
//     /shared/parse/<prod|candidate>/partial_parse.msgpack; when the overlay
//     ran, both commands stage the project the same way into workdirs of their
//     own, so the parse rehearsal exercises the same proposed source the
//     compile container compiled, not the pristine checked-in one.
//   - main container "upload": the shared s3-sidecar image (S3_SIDECAR_IMAGE
//     env, else <DOCKERHUB_USERNAME>/s3-sidecar:latest) runs
//     `python /compile_uploader.py` with COMPILE_MANIFEST_PATH +
//     MANIFEST_S3_URI + the S3 credential envs, plus the four PARSE_* envs
//     when the parse-export leg ran.
//
// The mode=compile label lets the job-status handler route its terminal
// outcome to outcomes.Recorder for in-process settlement. release-id/node-id
// annotations carry the authoritative identity. ImageTag must be non-empty —
// the team image must be explicitly versioned.
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
//   - when p.SourceOverlayURI is non-empty, an initContainer "overlay" using
//     the shared s3-sidecar image that runs `python /overlay_fetcher.py` with
//     SOURCE_OVERLAY_URI, OVERLAY_DEST=/shared/overlay, and the S3 credential
//     envs, laying a verification run's proposed source files into
//     /shared/overlay before any other team-image init container runs. Every
//     team-image init container's shell command (compile and both parse legs
//     below) is then prefixed with overlayStagePrefix and gets a workdir
//     emptyDir of its own at /work, so the overlay is the project under test
//     throughout the pod, not just in compile, and no container ever writes
//     into the team image's own project directory;
//   - an initContainer "compile" using the team image (ImageTag must be
//     non-empty) that, when the overlay ran, first stages the project into its
//     workdir with /shared/overlay laid over it, then runs the service's
//     resolved compile command there, copies the manifest dbt wrote in that
//     copy into /shared/manifest.json, and chmods it 644 so it is
//     world-readable regardless of the team image's uid/umask (the upload
//     container below runs as a fixed, different uid);
//   - when p.CandidateSchema is non-empty, two more team-image initContainers,
//     "parse-prod" and "parse-candidate", that (each staging the project into
//     its own workdir first, when the overlay is set) export and rehearse the
//     service's partial-parse cache under prod and candidate connection
//     contexts respectively (see buildParseExportCommand) into
//     /shared/parse/<ctx>/partial_parse.msgpack;
//     when CandidateSchema is empty the parse-export leg is disabled and the
//     pod keeps the two-container layout (compile initContainer + upload main
//     container);
//   - a main container "upload" using the shared s3-sidecar image (no dbt;
//     S3_SIDECAR_IMAGE env, else <DOCKERHUB_USERNAME>/s3-sidecar:latest)
//     that runs `python /compile_uploader.py` with COMPILE_MANIFEST_PATH,
//     MANIFEST_S3_URI, and the S3 credential envs forwarded from the
//     execution-controller env, plus the four PARSE_* envs (local paths +
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

	// staged is true for a verification run verifying a proposed fix. Every
	// team-image init container then stages the project into its own workdir,
	// lays the proposed files over the copy and runs dbt there, and reads the
	// artifacts dbt wrote back from that copy rather than from the configured
	// path inside the image. False (the common case) leaves every command
	// exactly as it was before the overlay feature existed.
	staged := p.SourceOverlayURI != ""
	var overlayPrefix string
	compiledManifestPath := manifestPath
	if staged {
		overlayPrefix = overlayStagePrefix()
		compiledManifestPath = overlayWorkdirPath(manifestPath)
	}
	compileCmd := overlayPrefix + shellJoin(compileArgv) +
		" && cp " + shellQuote(compiledManifestPath) + " /shared/manifest.json" +
		" && chmod 644 /shared/manifest.json"
	var initContainers []corev1.Container
	if staged {
		// The overlay fetcher lays the proposed files into /shared/overlay,
		// where every team-image init container reads them from.
		initContainers = append(initContainers, corev1.Container{
			Name:            "overlay",
			Image:           uploadImage,
			ImagePullPolicy: validationImagePullPolicy(),
			Command:         []string{"python", "/overlay_fetcher.py"},
			Env: append([]corev1.EnvVar{
				{Name: "SOURCE_OVERLAY_URI", Value: p.SourceOverlayURI},
				{Name: "OVERLAY_DEST", Value: "/shared/overlay"},
			}, s3CredEnvVars()...),
			VolumeMounts:    []corev1.VolumeMount{mount},
			SecurityContext: continuoImageSecurityContext(),
		})
	}
	initContainers = append(initContainers, corev1.Container{
		Name:            "compile",
		Image:           teamImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"sh", "-c", compileCmd},
		EnvFrom:         whFrom,
		VolumeMounts:    stagingVolumeMounts(mount, "compile", staged),
		SecurityContext: baseContainerSecurityContext(),
	})

	volumes := []corev1.Volume{sharedEmptyDirVolume()}
	if staged {
		volumes = append(volumes, workdirVolume("compile"))
	}
	spec := corev1.PodSpec{
		RestartPolicy:   corev1.RestartPolicyNever,
		SecurityContext: jobPodSecurityContext(),
		Volumes:         volumes,
		InitContainers:  initContainers,
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
				Command:         []string{"sh", "-c", overlayPrefix + buildParseExportCommand(parseArgv, partialParsePath, "prod", staged)},
				EnvFrom:         whFrom,
				VolumeMounts:    stagingVolumeMounts(mount, "parse-prod", staged),
				SecurityContext: baseContainerSecurityContext(),
			},
			corev1.Container{
				Name:            "parse-candidate",
				Image:           teamImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         []string{"sh", "-c", overlayPrefix + buildParseExportCommand(parseArgv, partialParsePath, "candidate", staged)},
				Env:             candEnv,
				EnvFrom:         whFrom,
				VolumeMounts:    stagingVolumeMounts(mount, "parse-candidate", staged),
				SecurityContext: baseContainerSecurityContext(),
			})
		if staged {
			spec.Volumes = append(spec.Volumes, workdirVolume("parse-prod"), workdirVolume("parse-candidate"))
		}
		spec.Containers[0].Env = append(spec.Containers[0].Env,
			corev1.EnvVar{Name: "PARSE_PROD_LOCAL_PATH", Value: "/shared/parse/prod/partial_parse.msgpack"},
			corev1.EnvVar{Name: "PARSE_PROD_S3_URI", Value: p.ParseProdS3URI},
			corev1.EnvVar{Name: "PARSE_CANDIDATE_LOCAL_PATH", Value: "/shared/parse/candidate/partial_parse.msgpack"},
			corev1.EnvVar{Name: "PARSE_CANDIDATE_S3_URI", Value: p.ParseCandidateS3URI},
		)
	}

	return spec, nil
}

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
// initContainer and no shared volume. A domain image never receives S3
// credentials — its contract files travel inside the image itself — except a
// python-csv node, whose harness fetches its own source and so gets the same
// four S3 credential env vars the validation pods use.
func buildPythonPodSpec(p JobParams) (corev1.PodSpec, error) {
	switch p.Operation {
	case pkg_model.OperationRun, pkg_model.OperationBuild:
		// build materializes and tests in one step; a python node declares no
		// tests, so it reduces to the same dispatch as run.
	default:
		return corev1.PodSpec{}, fmt.Errorf("%w: operation %q is not supported for python node %s.%s",
			events.ErrPermanent, p.Operation, p.SchemaName, p.TableName)
	}

	if p.ImageTag == "" {
		return corev1.PodSpec{}, fmt.Errorf("%w: image_tag missing from job params for python node %s.%s",
			events.ErrPermanent, p.SchemaName, p.TableName)
	}
	if !pythonImageHasExplicitTag(p.ImageTag) {
		return corev1.PodSpec{}, fmt.Errorf("%w: image_tag %q for python node %s.%s carries no explicit tag or digest",
			events.ErrPermanent, p.ImageTag, p.SchemaName, p.TableName)
	}

	whFrom, err := warehouseSecretEnvFrom("python job " + p.JobName)
	if err != nil {
		return corev1.PodSpec{}, err
	}

	env := []corev1.EnvVar{
		// NODE_ID is built from SchemaName/TableName, not p.NodeID (the
		// lowercase-normalized unique_id used elsewhere in this file), and
		// must stay that way: continuo-python-runtime's select_node matches
		// these trailing segments case-sensitively against the schema/table
		// declared in the image's baked contract.yaml. Switching this to
		// p.NodeID would silently fail to select the node for any contract
		// that declares mixed- or upper-case names, breaking every
		// production python Job for that node.
		{Name: "NODE_ID", Value: p.SchemaName + "." + p.TableName},
		{Name: "TABLE_NAME", Value: p.TableName},
		{Name: "TARGET_SCHEMA", Value: p.SchemaName},
	}
	// A csv node's harness fetches its source itself, so — a narrow,
	// deliberate exception to "a domain image never receives S3
	// credentials" — the S3 Secret env is attached for every python-csv
	// node regardless of the uri scheme: the executor knows only the node
	// type, and parsing the contract's uri here would leak contract
	// knowledge across the boundary.
	if p.NodeType == pkg_model.NodeTypePythonCsv {
		env = append(env, s3CredEnvVars()...)
	}

	return corev1.PodSpec{
		RestartPolicy:   corev1.RestartPolicyNever,
		SecurityContext: jobPodSecurityContext(),
		Containers: []corev1.Container{
			{
				Name:            "python-job",
				Image:           p.ImageTag,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Env:             env,
				EnvFrom:         whFrom,
				SecurityContext: baseContainerSecurityContext(),
			},
		},
	}, nil
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
// non-root user for containers running the continuo-python-runtime-<engine> and
// s3-sidecar images. uid 65532 is a cross-repo contract, not a locally
// observed fact: the s3-sidecar image is built in this repository, but
// continuo-python-runtime-<engine> is built and released from
// github.com/carolsimone/continuo-python-runtime, so this uid tracks that
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
// from the execution-controller environment. S3_BUCKET is intentionally omitted —
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
// When staged is true the container runs under a source overlay: dbt runs from
// the workdir copy of the project, so the artifact is read back from there
// rather than from the path configured inside the image. The diagnostic still
// names the configured path, which is the one an operator would edit.
func buildParseExportCommand(parseArgv []string, partialParsePath, ctx string, staged bool) string {
	dir := "/shared/parse/" + ctx
	parse := shellJoin(parseArgv)
	configured := partialParsePath
	if staged {
		partialParsePath = overlayWorkdirPath(partialParsePath)
	}
	pp := shellQuote(partialParsePath)
	return "set -e\n" +
		"mkdir -p " + dir + "\n" +
		"chmod 755 " + dir + "\n" +
		parse + "\n" +
		"[ -f " + pp + " ] || { echo 'continuo parse-export: the parse command completed without writing partial_parse.msgpack at " + configured + " — check compile.partial_parse_path in dbt-commands.yaml.' >&2; exit 46; }\n" +
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

// overlayWorkDir is where a team-image container running under a source overlay
// stages a writable copy of the team project and runs dbt from.
const overlayWorkDir = "/work"

// overlayStagePrefix is prepended to the shell command of every team-image
// container of a Job that carries a source overlay. It copies the image's
// project directory — the directory the container's shell starts in — into the
// workdir emptyDir at the same absolute path underneath it, lays the proposed
// files over that copy, and moves the shell there, so everything after it runs
// against the copy.
//
// The overlay is never applied in place. A team image's project files may be
// owned by a user the container does not run as (a plain Dockerfile `COPY`
// writes root-owned files whatever the image's USER is), which makes an
// in-place copy fail with EACCES; copying first makes the running user the
// owner of every staged file, so no team image needs a Dockerfile change to be
// verifiable. Mirroring the project's absolute path under the workdir is what
// lets overlayWorkdirPath re-point a configured artifact path at the copy
// without the executor having to know where the team's project lives.
func overlayStagePrefix() string {
	work := overlayWorkDir
	return "mkdir -p \"" + work + "${PWD%/*}\"" +
		" && cp -R \"$PWD\" \"" + work + "${PWD%/*}/\"" +
		" && cp -R /shared/overlay/. \"" + work + "$PWD/\"" +
		" && cd \"" + work + "$PWD\" && "
}

// overlayWorkdirPath maps an absolute path configured under the team image's
// project directory (the compile manifest, the partial-parse artifact) to where
// dbt writes it when the container runs staged: overlayStagePrefix mirrors the
// project's absolute path under the workdir, so the counterpart is that path
// with the workdir prefixed.
func overlayWorkdirPath(configured string) string {
	return overlayWorkDir + configured
}

// workdirVolumeName is the name of the workdir emptyDir serving one container.
// Each staging container gets its own, because they run sequentially against
// the same project and a shared scratch dir would hand one container's dbt
// output (a written target/) to the next one as if the image had shipped it.
func workdirVolumeName(container string) string { return "workdir-" + container }

// workdirVolume returns the scratch emptyDir a staging container runs dbt in.
func workdirVolume(container string) corev1.Volume {
	return corev1.Volume{
		Name:         workdirVolumeName(container),
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}
}

// workdirVolumeMount mounts a container's workdir emptyDir at /work.
func workdirVolumeMount(container string) corev1.VolumeMount {
	return corev1.VolumeMount{Name: workdirVolumeName(container), MountPath: overlayWorkDir}
}

// stagingVolumeMounts returns the mounts of a team-image container: the shared
// hand-off volume, plus its own workdir when the container stages the project
// to run a source overlay. Without an overlay the mount list is exactly what it
// was before staging existed.
func stagingVolumeMounts(shared corev1.VolumeMount, container string, staged bool) []corev1.VolumeMount {
	if !staged {
		return []corev1.VolumeMount{shared}
	}
	return []corev1.VolumeMount{shared, workdirVolumeMount(container)}
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
