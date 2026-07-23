package k8s

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
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

// A 40-char commit-SHA release id yields `_candidate_<sha>` → an untruncated
// `ensure-schema-candidate-<sha>` is 64 chars, over the 63-char limit Kubernetes
// applies to the pod-template job-name label. The name must be truncated while
// staying deterministic and unique per schema.
func TestSchemaOpJobName_TruncatesLongNamesPreservingUniqueness(t *testing.T) {
	shaA := "_candidate_" + strings.Repeat("a", 39) + "b"
	shaB := "_candidate_" + strings.Repeat("a", 39) + "c" // same truncated head, different schema

	nameA := schemaOpJobName(schemaOpEnsure, shaA)
	nameB := schemaOpJobName(schemaOpEnsure, shaB)

	for _, name := range []string{nameA, nameB} {
		assert.LessOrEqual(t, len(name), maxSchemaOpJobNameLen)
		assert.Regexp(t, `^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`, name, "must stay a valid DNS-1123 label")
	}
	// Deterministic (idempotent scheduling on redelivery) and unique per schema.
	assert.Equal(t, nameA, schemaOpJobName(schemaOpEnsure, shaA))
	assert.NotEqual(t, nameA, nameB, "schemas sharing a truncated head must map to distinct Jobs")
	// Short names remain fully readable, untouched by truncation.
	assert.Equal(t, "ensure-schema-candidate-rel-1", schemaOpJobName(schemaOpEnsure, "_candidate_rel_1"))
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

// A hung DDL pod must not hold the Job Active forever: the Job's ActiveDeadlineSeconds
// matches the executor's wait timeout, so the kubelet kills the pod and the Job goes
// terminal (JobFailed/DeadlineExceeded) — a state submitSchemaOpJob can clear on retry.
func TestSchemaOpJob_SetsActiveDeadlineMatchingWaitTimeout(t *testing.T) {
	job, err := schemaOpJob(schemaOpEnsure, "_candidate_x", "ensure-schema-candidate-x", "default")
	require.NoError(t, err)
	require.NotNil(t, job.Spec.ActiveDeadlineSeconds)
	assert.Equal(t, int64(SchemaOpJobTimeout/time.Second), *job.Spec.ActiveDeadlineSeconds)
}

// A Job killed by ActiveDeadlineSeconds carries a JobFailed condition but can leave
// Status.Failed at zero; it must still count as terminal so a retry clears and
// recreates it instead of waiting on a Job that will never run again.
func TestSubmitSchemaOpJob_ClearsDeadlineExceededJobAndRecreates(t *testing.T) {
	stale := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "ensure-schema-candidate-x", Namespace: "default"},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded"},
			},
		},
	}
	client := newValidationTestClient(stale)

	err := client.submitSchemaOpJob(context.Background(), schemaOpEnsure, "_candidate_x", "ensure-schema-candidate-x", "default")
	require.NoError(t, err)

	job := fetchJob(t, client, "default", "ensure-schema-candidate-x")
	assert.Empty(t, job.Status.Conditions, "the deadline-killed leftover must be replaced by a fresh Job")
}

// Two concurrent triggers can both see NotFound and race Create; the loser's
// AlreadyExists is not an error — the Job is deterministic by name and the op
// idempotent, so the loser just waits on the winner's Job.
func TestSubmitSchemaOpJob_ToleratesLostCreateRace(t *testing.T) {
	client := newValidationTestClient()
	client.clientset.(*fake.Clientset).PrependReactor("create", "jobs",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewAlreadyExists(batchv1.Resource("jobs"), "ensure-schema-candidate-x")
		})

	err := client.submitSchemaOpJob(context.Background(), schemaOpEnsure, "_candidate_x", "ensure-schema-candidate-x", "default")
	assert.NoError(t, err)
}

// Schema-op Jobs are deleted only after a terminal state (delete-on-success by a
// concurrent waiter, or TTL cleanup), so a vanished Job means the op already
// completed — return success instead of polling into the timeout.
func TestWaitForSchemaOpJob_NotFoundReturnsNil(t *testing.T) {
	client := newValidationTestClient()
	assert.NoError(t, client.waitForSchemaOpJob(context.Background(), "default", "drop-schema-candidate-x", schemaOpDrop))
}

// A JobFailed condition with Status.Failed still zero (the DeadlineExceeded shape)
// must surface as a failure, not keep the waiter polling.
func TestWaitForSchemaOpJob_FailedConditionReturnsError(t *testing.T) {
	deadlineKilled := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "ensure-schema-candidate-x", Namespace: "default"},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded"},
			},
		},
	}
	client := newValidationTestClient(deadlineKilled)
	err := client.waitForSchemaOpJob(context.Background(), "default", "ensure-schema-candidate-x", schemaOpEnsure)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed")
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
