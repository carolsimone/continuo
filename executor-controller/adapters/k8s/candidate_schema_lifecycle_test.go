package k8s

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(os.Stderr, nil)) }

// The schema-op Job runs the engine image with only the schema + op env and the
// operator warehouse Secret via envFrom — no S3, no candidate SQL, no table — so the
// executor never connects to the warehouse. VALIDATION_IMAGE/VALIDATION_WAREHOUSE_SECRET
// come from TestMain.
func TestSchemaOpJob_RunsEngineImageWithSecretAndOpEnv(t *testing.T) {
	job, err := schemaOpJob(schemaOpEnsure, "_candidate_rel_123", "ensure-schema-candidate-rel-123", "default")
	require.NoError(t, err)

	spec := job.Spec.Template.Spec
	require.Len(t, spec.Containers, 1)
	c := spec.Containers[0]
	assert.Equal(t, "ghcr.io/carolsimone/continuo-validation-runner-postgres:latest", c.Image)
	assert.Equal(t, corev1.RestartPolicyNever, spec.RestartPolicy)
	assert.Nil(t, c.Command, "schema ops run the image's default entrypoint")

	env := map[string]string{}
	for _, e := range c.Env {
		env[e.Name] = e.Value
	}
	assert.Equal(t, "_candidate_rel_123", env["DBT_TARGET_SCHEMA"])
	assert.Equal(t, "ensure_schema", env["VALIDATION_OP"])
	assert.NotContains(t, env, "TABLE_NAME")
	assert.NotContains(t, env, "CANDIDATE_SQL_URI")
	assert.NotContains(t, env, "AWS_ACCESS_KEY_ID")

	require.Len(t, c.EnvFrom, 1)
	assert.Equal(t, "continuo-warehouse-validation", c.EnvFrom[0].SecretRef.Name)

	// Distinct label so k8s-controller's validation watcher never routes these.
	assert.Equal(t, "continuo-schema-op", job.Labels["app"])
	assert.NotEqual(t, events.ModeValidation, job.Labels["mode"])
	assert.Equal(t, "ensure_schema", job.Labels["schema-op"])
}

func TestSchemaOpJob_MissingImageErrorsPermanently(t *testing.T) {
	t.Setenv("VALIDATION_IMAGE", "")
	_, err := schemaOpJob(schemaOpDrop, "_candidate_x", "drop-schema-candidate-x", "default")
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
	assert.Contains(t, err.Error(), "VALIDATION_IMAGE not configured")
}

func TestSchemaOpJobName_DeterministicAndDNSSafe(t *testing.T) {
	ensure := schemaOpJobName(schemaOpEnsure, "_candidate_rel_123")
	drop := schemaOpJobName(schemaOpDrop, "_candidate_rel_123")
	assert.Equal(t, "ensure-schema-candidate-rel-123", ensure)
	assert.Equal(t, "drop-schema-candidate-rel-123", drop)
	// Same inputs → same name (idempotent scheduling on redelivery).
	assert.Equal(t, ensure, schemaOpJobName(schemaOpEnsure, "_candidate_rel_123"))
	// No underscores/uppercase reach the Job name; never starts or ends with a dash.
	for _, name := range []string{ensure, drop} {
		assert.NotContains(t, name, "_")
		assert.Equal(t, name, strings.ToLower(name), "Job names must be lowercase")
		assert.NotEqual(t, byte('-'), name[0])
		assert.NotEqual(t, byte('-'), name[len(name)-1])
	}
}

func TestCandidateSchemaRunner_RefusesNonCandidateSchema(t *testing.T) {
	client := newValidationTestClient()
	creator := NewCandidateSchemaCreator(client, "default", testLogger())

	err := creator.EnsureCandidateSchema(context.Background(), "public")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing")

	// Nothing was scheduled.
	jobs, listErr := client.clientset.BatchV1().Jobs("default").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, listErr)
	assert.Empty(t, jobs.Items)
}

func TestSubmitSchemaOpJob_CreatesJobWhenAbsent(t *testing.T) {
	client := newValidationTestClient()
	err := client.submitSchemaOpJob(context.Background(), schemaOpEnsure, "_candidate_x", "ensure-schema-candidate-x", "default")
	require.NoError(t, err)

	job := fetchJob(t, client, "default", "ensure-schema-candidate-x")
	assert.Equal(t, "ensure_schema", job.Spec.Template.Spec.Containers[0].Env[1].Value)
}

func TestSubmitSchemaOpJob_ClearsTerminalJobAndRecreates(t *testing.T) {
	stale := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "ensure-schema-candidate-x", Namespace: "default"},
		Status:     batchv1.JobStatus{Failed: 1},
	}
	client := newValidationTestClient(stale)

	err := client.submitSchemaOpJob(context.Background(), schemaOpEnsure, "_candidate_x", "ensure-schema-candidate-x", "default")
	require.NoError(t, err)

	// The failed leftover was cleared and a fresh (non-terminal) Job took its place.
	job := fetchJob(t, client, "default", "ensure-schema-candidate-x")
	assert.Zero(t, job.Status.Failed)
	assert.Zero(t, job.Status.Succeeded)
}

func TestWaitForSchemaOpJob_SucceededReturnsNil(t *testing.T) {
	done := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "drop-schema-candidate-x", Namespace: "default"},
		Status:     batchv1.JobStatus{Succeeded: 1},
	}
	client := newValidationTestClient(done)
	assert.NoError(t, client.waitForSchemaOpJob(context.Background(), "default", "drop-schema-candidate-x", schemaOpDrop))
}

func TestWaitForSchemaOpJob_FailedReturnsError(t *testing.T) {
	failed := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "ensure-schema-candidate-x", Namespace: "default"},
		Status:     batchv1.JobStatus{Failed: 1},
	}
	client := newValidationTestClient(failed)
	err := client.waitForSchemaOpJob(context.Background(), "default", "ensure-schema-candidate-x", schemaOpEnsure)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed")
}
