package k8s

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

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

func newValidationTestClient(objects ...*batchv1.Job) *K8sClient {
	runtimeObjs := make([]runtime.Object, 0, len(objects))
	for _, o := range objects {
		runtimeObjs = append(runtimeObjs, o)
	}
	cs := fake.NewSimpleClientset(runtimeObjs...)
	c := &K8sClient{logger: slog.New(slog.NewTextHandler(os.Stderr, nil))}
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
	}
}

// TestCreateValidationJob_BuildsExpectedCommand_DbtModel verifies that model
// nodes run validation_runner.py (CTAS path) rather than a dbt command.
// CANDIDATE_SQL_URI env must be populated from the params so the runner can
// fetch the SQL from S3 and build the empty table without a dbt recompile.
func TestCreateValidationJob_BuildsExpectedCommand_DbtModel(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newValidationTestClient()
	p := validationParams()

	require.NoError(t, c.CreateValidationJob(context.Background(), p))

	job := fetchJob(t, c, p.Namespace, p.JobName)
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	assert.Equal(t,
		[]string{"python", "/validation_runner.py"},
		job.Spec.Template.Spec.Containers[0].Command)
	assert.Equal(t, "service-1:abc-1714300000", job.Spec.Template.Spec.Containers[0].Image)

	// CANDIDATE_SQL_URI must be wired through so validation_runner.py can fetch
	// the SQL from S3 and build the empty CTAS table.
	assert.Equal(t, p.CandidateSQLURI, envByName(job.Spec.Template.Spec, "CANDIDATE_SQL_URI"),
		"model validation job must set CANDIDATE_SQL_URI env")
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
		[]string{"dbt", "seed", "--select", "country_codes", "--empty"},
		job.Spec.Template.Spec.Containers[0].Command)
}

// TestCreateValidationJob_PassesCandidateEnvsViaEnv verifies that
// DBT_TARGET_SCHEMA, CANDIDATE_SQL_URI, and the five S3 credential vars are
// all set on the validation pod.
// DBT_TARGET_SCHEMA routes the generate_schema_name macro to the candidate
// schema; CANDIDATE_SQL_URI is the S3 address the runner fetches to build the
// empty CTAS table; the S3 vars give the runner credentials to perform that
// fetch. Dropping any of these would silently break the validation run.
func TestCreateValidationJob_PassesCandidateEnvsViaEnv(t *testing.T) {
	t.Setenv("S3_ENDPOINT_URL", "http://minio:9000")
	t.Setenv("S3_BUCKET", "continuo-artifacts")
	t.Setenv("AWS_ACCESS_KEY_ID", "test-key-id")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	t.Setenv("AWS_DEFAULT_REGION", "us-east-1")

	c := newValidationTestClient()
	p := validationParams()

	require.NoError(t, c.CreateValidationJob(context.Background(), p))

	job := fetchJob(t, c, p.Namespace, p.JobName)
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	spec := job.Spec.Template.Spec

	require.NotEmpty(t, envByName(spec, "DBT_TARGET_SCHEMA"), "validation job must set DBT_TARGET_SCHEMA")
	assert.Equal(t, p.CandidateSchema, envByName(spec, "DBT_TARGET_SCHEMA"))

	require.NotEmpty(t, envByName(spec, "CANDIDATE_SQL_URI"), "validation job must set CANDIDATE_SQL_URI")
	assert.Equal(t, p.CandidateSQLURI, envByName(spec, "CANDIDATE_SQL_URI"))

	// S3 credentials — the runner needs these to boto3-GET the candidate SQL.
	assert.Equal(t, "http://minio:9000", envByName(spec, "S3_ENDPOINT_URL"), "validation job must forward S3_ENDPOINT_URL")
	assert.Equal(t, "continuo-artifacts", envByName(spec, "S3_BUCKET"), "validation job must forward S3_BUCKET")
	assert.Equal(t, "test-key-id", envByName(spec, "AWS_ACCESS_KEY_ID"), "validation job must forward AWS_ACCESS_KEY_ID")
	assert.Equal(t, "test-secret", envByName(spec, "AWS_SECRET_ACCESS_KEY"), "validation job must forward AWS_SECRET_ACCESS_KEY")
	assert.Equal(t, "us-east-1", envByName(spec, "AWS_DEFAULT_REGION"), "validation job must forward AWS_DEFAULT_REGION")
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
// validation Jobs are built with imagePullPolicy=Always. A service image is
// re-baked FROM dbt-base and pushed under the same mutable service tag whenever
// the validation runner changes; PullIfNotPresent would keep a stale cached
// image on the node and validate the candidate with an out-of-date runner.
// PullAlways forces the node to fetch the freshly pushed image for every run.
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

func TestCreateValidationJob_FailsPermanentOnEmptyImageTag(t *testing.T) {
	c := newValidationTestClient()
	p := validationParams()
	p.ImageTag = ""

	err := c.CreateValidationJob(context.Background(), p)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "image_tag missing")
	assert.True(t, errors.Is(err, events.ErrPermanent),
		"empty image_tag must wrap events.ErrPermanent so outbox processor classifies non-retryable")
}
