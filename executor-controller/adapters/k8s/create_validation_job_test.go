package k8s

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/adapters/commandcfg"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

// TestMain defaults VALIDATION_IMAGE and VALIDATION_WAREHOUSE_SECRET for the whole
// package: validation pods now require both (the engine image is the SRE's choice, the
// warehouse Secret is operator-owned), so success-path tests need not set them
// individually. Tests exercising the missing-image / missing-secret paths override
// them with t.Setenv.
func TestMain(m *testing.M) {
	os.Setenv("VALIDATION_IMAGE", "ghcr.io/carolsimone/continuo-validation-runner-postgres:latest")
	os.Setenv("VALIDATION_WAREHOUSE_SECRET", "continuo-warehouse-validation")
	os.Exit(m.Run())
}

func newValidationTestClient(objects ...*batchv1.Job) *K8sClient {
	runtimeObjs := make([]runtime.Object, 0, len(objects))
	for _, o := range objects {
		runtimeObjs = append(runtimeObjs, o)
	}
	cs := fake.NewSimpleClientset(runtimeObjs...)
	c := &K8sClient{logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), commands: commandcfg.Defaults()}
	c.setClientsetForTest(cs)
	return c
}

func fetchJob(t *testing.T, c *K8sClient, namespace, name string) *batchv1.Job {
	t.Helper()
	job, err := c.clientset.BatchV1().Jobs(namespace).Get(context.Background(), name, metav1.GetOptions{})
	require.NoError(t, err)
	return job
}

func validationParams() ValidationJobParams {
	return ValidationJobParams{
		JobName:         "validate-orders-rel123",
		ReleaseID:       "rel123",
		NodeID:          "svc.orders",
		ServiceName:     "service-1",
		SchemaName:      "analytics",
		TableName:       "orders",
		NodeType:        pkg_model.NodeTypeDbtModel,
		ImageTag:        "abc-1714300000",
		CandidateSchema: "_candidate_rel_123",
		CandidateSQLURI: "s3://continuo-artifacts/candidate-sql/rel123/svc.orders.sql",
		Namespace:       "default",
		// ValidationOp/ProdSchema intentionally empty here -> defaults exercised.
	}
}

func TestCreateValidationJob_BuildFromSql_SingleContainerFetchesOwnSQL(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	t.Setenv("S3_ENDPOINT_URL", "http://minio:9000")
	t.Setenv("AWS_ACCESS_KEY_ID", "test-key-id")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	c := newValidationTestClient()
	p := validationParams() // op defaults to build_from_sql, CandidateSQLURI set

	require.NoError(t, c.CreateValidationJob(context.Background(), p))
	spec := fetchJob(t, c, p.Namespace, p.JobName).Spec.Template.Spec

	// No sidecar: one container, no init container, no shared emptyDir.
	assert.Empty(t, spec.InitContainers, "validation must carry no fetch sidecar")
	assert.Empty(t, spec.Volumes, "validation needs no shared emptyDir")
	require.Len(t, spec.Containers, 1)

	main := spec.Containers[0]
	assert.Equal(t, "dbt-job", main.Name)
	assert.Equal(t, "ghcr.io/carolsimone/continuo-validation-runner-postgres:latest", main.Image)
	assert.Equal(t, []string{"python", "/validation_runner.py"}, main.Command)
	// The main container fetches its own SQL: it carries the URI + S3 creds.
	assert.Equal(t, p.CandidateSQLURI, envByName(spec, "CANDIDATE_SQL_URI"))
	assert.Equal(t, "http://minio:9000", envByName(spec, "S3_ENDPOINT_URL"))
	assert.Equal(t, "test-secret", envByName(spec, "AWS_SECRET_ACCESS_KEY"))
	// CANDIDATE_SQL_PATH / the shared-file indirection is gone.
	assert.Empty(t, envByName(spec, "CANDIDATE_SQL_PATH"), "no shared file: path env must be unset")
}

func TestCreateValidationJob_BuildsExpectedCommand_DbtSeed(t *testing.T) {
	c := newValidationTestClient()
	p := validationParams()
	p.NodeType = pkg_model.NodeTypeDbtSeed
	p.TableName = "country_codes"

	require.NoError(t, c.CreateValidationJob(context.Background(), p))

	job := fetchJob(t, c, p.Namespace, p.JobName)
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	assert.Equal(t,
		[]string{"python", "/validation_runner.py"},
		job.Spec.Template.Spec.Containers[0].Command)
}

func TestCreateValidationJob_ForwardsS3CredsToMainContainer(t *testing.T) {
	t.Setenv("S3_ENDPOINT_URL", "http://minio:9000")
	t.Setenv("AWS_ACCESS_KEY_ID", "test-key-id")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	t.Setenv("AWS_DEFAULT_REGION", "us-east-1")

	c := newValidationTestClient()
	p := validationParams()
	require.NoError(t, c.CreateValidationJob(context.Background(), p))
	spec := fetchJob(t, c, p.Namespace, p.JobName).Spec.Template.Spec

	assert.Equal(t, p.CandidateSchema, envByName(spec, "DBT_TARGET_SCHEMA"))
	assert.Equal(t, "http://minio:9000", envByName(spec, "S3_ENDPOINT_URL"))
	assert.Equal(t, "test-key-id", envByName(spec, "AWS_ACCESS_KEY_ID"))
	assert.Equal(t, "test-secret", envByName(spec, "AWS_SECRET_ACCESS_KEY"))
	assert.Equal(t, "us-east-1", envByName(spec, "AWS_DEFAULT_REGION"))
}

func TestCreateValidationJob_LabelsCarryModeReleaseNodeIDs(t *testing.T) {
	c := newValidationTestClient()
	p := validationParams()

	require.NoError(t, c.CreateValidationJob(context.Background(), p))

	job := fetchJob(t, c, p.Namespace, p.JobName)
	want := map[string]string{
		"app":          "dbt-job",
		"mode":         "validation",
		"release-id":   "rel123",
		"node-id":      "svc.orders",
		"service_name": "service-1",
		"schema_name":  "analytics",
		"table_name":   "orders",
	}
	assert.Equal(t, want, job.Labels)
	assert.Equal(t, want, job.Spec.Template.Labels)

	// Raw identity is also stamped as annotations (authoritative for the
	// validation.node.completed payload). Here the values are charset-clean and
	// short, so labels and annotations agree.
	wantAnnotations := map[string]string{
		pkg_model.AnnotationReleaseID: "rel123",
		pkg_model.AnnotationNodeID:    "svc.orders",
	}
	assert.Equal(t, wantAnnotations, job.Annotations)
	assert.Equal(t, wantAnnotations, job.Spec.Template.Annotations)

	require.NotNil(t, job.Spec.BackoffLimit)
	assert.Equal(t, int32(0), *job.Spec.BackoffLimit)
	assert.Equal(t, corev1.RestartPolicyNever, job.Spec.Template.Spec.RestartPolicy)
}

// TestCreateValidationJob_AnnotationsCarryRawIDsWhenLabelWouldSanitize verifies
// the I2 fix: a release/node id that sanitizeK8sLabel would alter (out-of-charset
// chars and >63 chars) is preserved verbatim in the Job annotations even though
// the label is sanitized — so the round-trip into the outcome lookup is lossless.
func TestCreateValidationJob_AnnotationsCarryRawIDsWhenLabelWouldSanitize(t *testing.T) {
	c := newValidationTestClient()
	p := validationParams()
	rawNodeID := "service-1.analytics.my_model:with/colon+" + repeatStr("x", 60) // out-of-charset AND >63 chars
	rawReleaseID := "release/2026-05-29T12:00:00+00:00"
	p.NodeID = rawNodeID
	p.ReleaseID = rawReleaseID

	require.NoError(t, c.CreateValidationJob(context.Background(), p))

	job := fetchJob(t, c, p.Namespace, p.JobName)

	// Annotations carry the raw values verbatim.
	assert.Equal(t, rawNodeID, job.Annotations[pkg_model.AnnotationNodeID])
	assert.Equal(t, rawReleaseID, job.Annotations[pkg_model.AnnotationReleaseID])

	// Labels are sanitized (differ from raw) — proving annotations are the
	// authoritative carrier, not the labels.
	assert.NotEqual(t, rawNodeID, job.Labels["node-id"])
	assert.Equal(t, sanitizeK8sLabel(rawNodeID), job.Labels["node-id"])
	assert.Equal(t, sanitizeK8sLabel(rawReleaseID), job.Labels["release-id"])
}

func envByName(spec corev1.PodSpec, name string) string {
	for _, e := range spec.Containers[0].Env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

func TestCreateValidationJob_CloneFromProd_SingleContainerNoS3(t *testing.T) {
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	c := newValidationTestClient()
	p := validationParams()
	p.JobName = "validate-orders-rel123-clone"
	p.ValidationOp = "clone_from_prod"
	p.ProdSchema = "analytics"
	p.CandidateSQLURI = "" // clone nodes have no candidate SQL

	require.NoError(t, c.CreateValidationJob(context.Background(), p))
	spec := fetchJob(t, c, p.Namespace, p.JobName).Spec.Template.Spec

	assert.Empty(t, spec.InitContainers, "clone_from_prod has no fetch init container")
	assert.Empty(t, spec.Volumes, "clone_from_prod needs no shared emptyDir")
	require.Len(t, spec.Containers, 1)
	assert.Equal(t, "clone_from_prod", envByName(spec, "VALIDATION_OP"))
	assert.Equal(t, "analytics", envByName(spec, "PROD_SCHEMA"))
	assert.Empty(t, envByName(spec, "AWS_SECRET_ACCESS_KEY"), "clone_from_prod must not carry S3 creds")
	assert.Empty(t, envByName(spec, "CANDIDATE_SQL_PATH"),
		"clone_from_prod has no fetch sidecar/emptyDir, so it must not carry CANDIDATE_SQL_PATH")
}

func repeatStr(s string, n int) string {
	out := ""
	for i := 0; i < n; i++ {
		out += s
	}
	return out
}

func TestCreateValidationJob_IsIdempotent_AlreadyExists(t *testing.T) {
	p := validationParams()
	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      p.JobName,
			Namespace: p.Namespace,
			Labels:    map[string]string{"app": "dbt-job", "preexisting": "true"},
		},
	}
	c := newValidationTestClient(existing)

	require.NoError(t, c.CreateValidationJob(context.Background(), p))

	// The pre-existing Job is untouched: our create was a no-op, not an overwrite.
	job := fetchJob(t, c, p.Namespace, p.JobName)
	assert.Equal(t, "true", job.Labels["preexisting"])
}

// TestCreateValidationJob_DefaultsToPullAlways verifies that, with no override,
// validation Jobs are built with imagePullPolicy=Always so that a re-push to a
// mutable tag is picked up on the next run. Side-loaded clusters override this
// via VALIDATION_IMAGE_PULL_POLICY (see the next test).
func TestCreateValidationJob_DefaultsToPullAlways(t *testing.T) {
	t.Setenv("VALIDATION_IMAGE_PULL_POLICY", "")
	c := newValidationTestClient()
	p := validationParams()

	require.NoError(t, c.CreateValidationJob(context.Background(), p))

	job := fetchJob(t, c, p.Namespace, p.JobName)
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, corev1.PullAlways, job.Spec.Template.Spec.Containers[0].ImagePullPolicy,
		"validation job must default to PullAlways so a re-pushed mutable service tag is actually re-pulled")
}

// TestCreateValidationJob_PullPolicyOverride verifies that environments which
// side-load images into the node cache and have no registry to pull from (the
// kind-based e2e suite, local clusters) can set VALIDATION_IMAGE_PULL_POLICY to
// IfNotPresent (or Never) so the locally-loaded image is used instead of an
// ErrImagePull. Any unrecognized value falls back to the prod-safe PullAlways.
func TestCreateValidationJob_PullPolicyOverride(t *testing.T) {
	cases := []struct {
		env  string
		want corev1.PullPolicy
	}{
		{"IfNotPresent", corev1.PullIfNotPresent},
		{"Never", corev1.PullNever},
		{"Always", corev1.PullAlways},
		{"bogus", corev1.PullAlways},
	}
	for _, tc := range cases {
		t.Run(tc.env, func(t *testing.T) {
			t.Setenv("VALIDATION_IMAGE_PULL_POLICY", tc.env)
			c := newValidationTestClient()
			p := validationParams()

			require.NoError(t, c.CreateValidationJob(context.Background(), p))

			job := fetchJob(t, c, p.Namespace, p.JobName)
			require.Len(t, job.Spec.Template.Spec.Containers, 1)
			assert.Equal(t, tc.want, job.Spec.Template.Spec.Containers[0].ImagePullPolicy)
		})
	}
}

// The executor bakes in no engine: with VALIDATION_IMAGE unset the node fails
// permanently rather than silently defaulting to one engine. The SRE sets
// VALIDATION_IMAGE (Helm/compose) to the chosen engine's runner image.
func TestCreateValidationJob_MissingImageErrors(t *testing.T) {
	t.Setenv("VALIDATION_IMAGE", "")
	c := newValidationTestClient()
	p := validationParams()

	err := c.CreateValidationJob(context.Background(), p)
	require.ErrorIs(t, err, events.ErrPermanent)
	assert.Contains(t, err.Error(), "VALIDATION_IMAGE not configured")
}

// The validation container gets its warehouse credentials from the operator-owned
// Secret named by VALIDATION_WAREHOUSE_SECRET (envFrom), never inline DBT_POSTGRES_*.
func TestCreateValidationJob_AttachesWarehouseSecretEnvFrom(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "continuo-warehouse-validation")
	c := newValidationTestClient()
	p := validationParams()
	p.ValidationOp = "clone_from_prod"
	p.ProdSchema = "analytics"
	p.CandidateSQLURI = ""

	require.NoError(t, c.CreateValidationJob(context.Background(), p))
	main := fetchJob(t, c, p.Namespace, p.JobName).Spec.Template.Spec.Containers[0]

	require.Len(t, main.EnvFrom, 1)
	require.NotNil(t, main.EnvFrom[0].SecretRef)
	assert.Equal(t, "continuo-warehouse-validation", main.EnvFrom[0].SecretRef.Name)
	for _, e := range main.Env {
		assert.NotContains(t, e.Name, "DBT_POSTGRES_",
			"warehouse creds must come from the Secret, not inline env")
	}
}

// Without VALIDATION_WAREHOUSE_SECRET configured, validation cannot connect to the
// warehouse, so the node fails permanently with an actionable reason.
func TestCreateValidationJob_MissingWarehouseSecretErrors(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "")
	c := newValidationTestClient()
	p := validationParams()

	err := c.CreateValidationJob(context.Background(), p)
	require.ErrorIs(t, err, events.ErrPermanent)
	assert.Contains(t, err.Error(), "warehouse secret")
}

// An explicit VALIDATION_IMAGE override is used verbatim (no prefixing).
func TestCreateValidationJob_VALIDATION_IMAGE_OverrideUsedVerbatim(t *testing.T) {
	t.Setenv("VALIDATION_IMAGE", "ghcr.io/acme/continuo-validator:v3")
	t.Setenv("DOCKERHUB_USERNAME", "carolsimone")
	c := newValidationTestClient()
	p := validationParams()

	require.NoError(t, c.CreateValidationJob(context.Background(), p))

	job := fetchJob(t, c, p.Namespace, p.JobName)
	assert.Equal(t, "ghcr.io/acme/continuo-validator:v3", job.Spec.Template.Spec.Containers[0].Image)
}

// VALIDATION_OP defaults to build_from_sql when unset; PROD_SCHEMA is forwarded
// from params. Plan 3 will set these per node.
func TestCreateValidationJob_SetsValidationOpAndProdSchema(t *testing.T) {
	c := newValidationTestClient()

	// default op when params leave it empty
	p := validationParams()
	require.NoError(t, c.CreateValidationJob(context.Background(), p))
	job := fetchJob(t, c, p.Namespace, p.JobName)
	assert.Equal(t, "build_from_sql", envByName(job.Spec.Template.Spec, "VALIDATION_OP"))

	// explicit clone op + prod schema forwarded; clone nodes have no candidate SQL.
	p2 := validationParams()
	p2.JobName = "validate-orders-rel123-clone"
	p2.ValidationOp = "clone_from_prod"
	p2.ProdSchema = "analytics"
	p2.CandidateSQLURI = ""
	require.NoError(t, c.CreateValidationJob(context.Background(), p2))
	job2 := fetchJob(t, c, p2.Namespace, p2.JobName)
	assert.Equal(t, "clone_from_prod", envByName(job2.Spec.Template.Spec, "VALIDATION_OP"))
	assert.Equal(t, "analytics", envByName(job2.Spec.Template.Spec, "PROD_SCHEMA"))
}

// TestCreateValidationJob_BuildFromSql_EmptyCandidateURIErrors verifies that a
// build_from_sql validation job with no CandidateSQLURI fails with a permanent
// error at job-build time rather than producing a pod that fails at runtime.
func TestCreateValidationJob_BuildFromSql_EmptyCandidateURIErrors(t *testing.T) {
	c := newValidationTestClient()
	p := validationParams()
	p.CandidateSQLURI = "" // omit the required URI

	err := c.CreateValidationJob(context.Background(), p)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

// TestCreateValidationJob_UnknownOpErrors verifies that an unrecognised
// ValidationOp returns a permanent error rather than silently falling through
// to a default pod shape. A future op must be explicitly named; unknown ops are
// treated as permanent failures so the release is not silently misvalidated.
func TestCreateValidationJob_UnknownOpErrors(t *testing.T) {
	c := newValidationTestClient()
	p := validationParams()
	p.ValidationOp = "bogus_op"

	err := c.CreateValidationJob(context.Background(), p)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}
