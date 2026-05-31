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
		DeferStateURI:   "s3://continuo/releases/prev/manifests/",
		Namespace:       "default",
	}
}

func TestCreateValidationJob_BuildsExpectedCommand_DbtModel_WithDefer(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newValidationTestClient()
	p := validationParams()

	require.NoError(t, c.CreateValidationJob(context.Background(), p))

	job := fetchJob(t, c, p.Namespace, p.JobName)
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	assert.Equal(t,
		[]string{"dbt", "run", "--select", "orders",
			"--empty",
			"--defer", "--state", "s3://continuo/releases/prev/manifests/"},
		job.Spec.Template.Spec.Containers[0].Command)
	assert.Equal(t, "service-1:abc-1714300000", job.Spec.Template.Spec.Containers[0].Image)
}

func TestCreateValidationJob_BuildsExpectedCommand_DbtModel_BootstrapNoDefer(t *testing.T) {
	c := newValidationTestClient()
	p := validationParams()
	p.DeferStateURI = ""

	require.NoError(t, c.CreateValidationJob(context.Background(), p))

	job := fetchJob(t, c, p.Namespace, p.JobName)
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	assert.Equal(t,
		[]string{"dbt", "run", "--select", "orders",
			"--empty"},
		job.Spec.Template.Spec.Containers[0].Command)
}

func TestCreateValidationJob_BuildsExpectedCommand_DbtSeed_NoDefer(t *testing.T) {
	c := newValidationTestClient()
	p := validationParams()
	p.NodeType = pkg_model.NodeTypeDbtSeed
	p.TableName = "country_codes"
	p.DeferStateURI = ""

	require.NoError(t, c.CreateValidationJob(context.Background(), p))

	job := fetchJob(t, c, p.Namespace, p.JobName)
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	assert.Equal(t,
		[]string{"dbt", "seed", "--select", "country_codes",
			"--empty"},
		job.Spec.Template.Spec.Containers[0].Command)
}

// The candidate schema is delivered to dbt through the DBT_TARGET_SCHEMA env
// var (read by each service's generate_schema_name macro), not a CLI flag.
// Dropping this env would silently route validation runs into the production
// schema, so pin it here.
func TestCreateValidationJob_PassesCandidateSchemaViaEnv(t *testing.T) {
	c := newValidationTestClient()
	p := validationParams()

	require.NoError(t, c.CreateValidationJob(context.Background(), p))

	job := fetchJob(t, c, p.Namespace, p.JobName)
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	var got string
	found := false
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		if e.Name == "DBT_TARGET_SCHEMA" {
			got, found = e.Value, true
		}
	}
	require.True(t, found, "validation job must set DBT_TARGET_SCHEMA")
	assert.Equal(t, p.CandidateSchema, got)
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
