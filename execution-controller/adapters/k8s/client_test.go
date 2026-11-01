package k8s

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/adapters/commandcfg"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

func TestBuildPodSpec_CommandPerNodeType(t *testing.T) {
	tests := []struct {
		nodeType    pkg_model.NodeType
		tableName   string
		wantCommand []string
	}{
		{pkg_model.NodeTypeDbtModel, "orders", []string{"dbt", "run", "--select", "orders"}},
		{pkg_model.NodeTypeDbtSeed, "my_seed", []string{"dbt", "seed", "--select", "my_seed"}},
		{pkg_model.NodeTypeDbtSnapshot, "my_snap", []string{"dbt", "snapshot", "--select", "my_snap"}},
	}

	for _, tt := range tests {
		params := JobParams{
			NodeType:  tt.nodeType,
			TableName: tt.tableName,
			ImageTag:  "test-tag",
		}
		spec, err := buildPodSpec(params, tt.nodeType.Command(tt.tableName), "")
		require.NoError(t, err)
		require.Len(t, spec.Containers, 1)
		assert.Equal(t, tt.wantCommand, spec.Containers[0].Command,
			"NodeType %q should produce command %v", tt.nodeType, tt.wantCommand)
	}
}

func TestBuildPodSpec_ImageRef(t *testing.T) {
	t.Run("no DOCKERHUB_USERNAME uses service name directly", func(t *testing.T) {
		t.Setenv("DOCKERHUB_USERNAME", "")
		spec, err := buildPodSpec(JobParams{ServiceName: "service-1", ImageTag: "some-tag", NodeType: pkg_model.NodeTypeDbtModel, TableName: "t"}, pkg_model.NodeTypeDbtModel.Command("t"), "")
		require.NoError(t, err)
		require.Len(t, spec.Containers, 1)
		assert.Equal(t, "service-1:some-tag", spec.Containers[0].Image)
	})

	t.Run("with DOCKERHUB_USERNAME each service gets its own image", func(t *testing.T) {
		t.Setenv("DOCKERHUB_USERNAME", "carolsimone")
		specA, err := buildPodSpec(JobParams{ServiceName: "service-1", ImageTag: "latest", NodeType: pkg_model.NodeTypeDbtModel, TableName: "t"}, pkg_model.NodeTypeDbtModel.Command("t"), "")
		require.NoError(t, err)
		specB, err := buildPodSpec(JobParams{ServiceName: "service-2", ImageTag: "latest", NodeType: pkg_model.NodeTypeDbtModel, TableName: "t"}, pkg_model.NodeTypeDbtModel.Command("t"), "")
		require.NoError(t, err)
		require.Len(t, specA.Containers, 1)
		require.Len(t, specB.Containers, 1)
		assert.Equal(t, "carolsimone/service-1:latest", specA.Containers[0].Image)
		assert.Equal(t, "carolsimone/service-2:latest", specB.Containers[0].Image)
	})

	t.Run("with DOCKERHUB_USERNAME uses params.ImageTag", func(t *testing.T) {
		t.Setenv("DOCKERHUB_USERNAME", "carolsimone")
		spec, err := buildPodSpec(JobParams{ServiceName: "service-1", ImageTag: "v1.2.3", NodeType: pkg_model.NodeTypeDbtModel, TableName: "t"}, pkg_model.NodeTypeDbtModel.Command("t"), "")
		require.NoError(t, err)
		require.Len(t, spec.Containers, 1)
		assert.Equal(t, "carolsimone/service-1:v1.2.3", spec.Containers[0].Image)
	})
}

func TestBuildPodSpec_UsesImageTagFromParams(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	params := JobParams{
		ServiceName: "service-1",
		ImageTag:    "abc123-1714300000",
		NodeType:    pkg_model.NodeTypeDbtModel,
		TableName:   "users",
	}
	spec, err := buildPodSpec(params, params.NodeType.Command(params.TableName), "")
	require.NoError(t, err)
	require.Len(t, spec.Containers, 1)
	assert.Equal(t, "service-1:abc123-1714300000", spec.Containers[0].Image)
}

func TestBuildPodSpec_RefusesEmptyImageTag(t *testing.T) {
	params := JobParams{
		ServiceName: "service-1",
		ImageTag:    "",
		NodeType:    pkg_model.NodeTypeDbtModel,
		TableName:   "users",
	}
	_, err := buildPodSpec(params, params.NodeType.Command(params.TableName), "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "image_tag missing")
	// Wrapping events.ErrPermanent lets the outbox processor classify this
	// as non-retryable and short-circuit to terminal failure on attempt 1
	// instead of consuming the retry budget for a deterministically-bad
	// input.
	assert.True(t, errors.Is(err, events.ErrPermanent),
		"empty image_tag must wrap events.ErrPermanent so outbox processor classifies non-retryable")
}

// TestBuildPodSpec_NoCandidateSchemaEnv locks that the production query job does
// not carry DBT_TARGET_SCHEMA: that env var routes a model's output to the
// candidate schema and is validation-only. With it unset, generate_schema_name
// resolves to the production schema, leaving prod materialization byte-identical.
func TestBuildPodSpec_NoCandidateSchemaEnv(t *testing.T) {
	spec, err := buildPodSpec(JobParams{
		ServiceName: "service-1",
		ImageTag:    "tag",
		NodeType:    pkg_model.NodeTypeDbtModel,
		TableName:   "users",
	}, pkg_model.NodeTypeDbtModel.Command("users"), "")
	require.NoError(t, err)
	require.Len(t, spec.Containers, 1)
	assert.Empty(t, envByName(spec, "DBT_TARGET_SCHEMA"),
		"prod query job must not set the candidate output schema")
}

// testJobTTL is the finished-Job TTL of the clients the test helpers build,
// and testJobTTLSeconds the TTLSecondsAfterFinished their Jobs carry.
const (
	testJobTTL              = 30 * time.Minute
	testJobTTLSeconds int32 = 1800
)

// newQueryTestClient creates a K8sClient backed by a fake clientset for unit tests.
func newQueryTestClient() *K8sClient {
	return NewK8sClientFor(fake.NewSimpleClientset(), slog.New(slog.NewTextHandler(os.Stderr, nil)), commandcfg.Defaults(), testJobTTL)
}

func TestCreateJob_AlreadyExistsIsSuccess(t *testing.T) {
	existing := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "dbt-public-orders", Namespace: "default"}}
	c := &K8sClient{logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), commands: commandcfg.Defaults()}
	c.setClientsetForTest(fake.NewSimpleClientset(existing))
	require.NoError(t, c.CreateJob(context.Background(), existing.DeepCopy()),
		"a Job with the same deterministic name was created by an earlier launch")
}

// TestCreateQueryJob_NormalProduction_HasNoModeLabel verifies that a production
// dbt job carries NO "mode" label. Only the release legs (validation, seed-build,
// compile) set one, naming the leg on the Job; the observer settles a result by
// the deployment row's mode, and a query job belongs to the production lifecycle
// that owns retries, task executions, and status.
func TestCreateQueryJob_NormalProduction_HasNoModeLabel(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newQueryTestClient()
	ctx := context.Background()

	params := JobParams{
		JobName:      "prod-svc-analytics-orders-abc456",
		TaskID:       "task-2",
		RunID:        "sched-2",
		ScheduleName: "daily",
		ServiceName:  "svc",
		SchemaName:   "analytics",
		TableName:    "orders",
		Namespace:    "default",
		NodeType:     pkg_model.NodeTypeDbtModel,
		ImageTag:     "v1",
	}

	require.NoError(t, c.CreateQueryJob(ctx, params))

	job, err := c.clientset.BatchV1().Jobs("default").Get(ctx, params.JobName, metav1.GetOptions{})
	require.NoError(t, err)

	_, hasModeLabel := job.Labels["mode"]
	assert.False(t, hasModeLabel,
		"normal production job must have no mode label — production lifecycle must run unchanged")
	_, hasPodModeLabel := job.Spec.Template.Labels["mode"]
	assert.False(t, hasPodModeLabel,
		"pod template of normal production job must also have no mode label")
}

// TestCreateQueryJob_SetsTTLSecondsAfterFinished verifies that a production dbt
// Job carries the client's finished-Job TTL as its TTLSecondsAfterFinished, so
// Kubernetes deletes the Job (and its pod) that long after it finishes instead
// of leaving finished pods on the cluster.
func TestCreateQueryJob_SetsTTLSecondsAfterFinished(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newQueryTestClient()
	ctx := context.Background()

	params := JobParams{
		JobName:      "prod-svc-analytics-orders-abc456",
		TaskID:       "task-3",
		RunID:        "sched-3",
		ScheduleName: "daily",
		ServiceName:  "svc",
		SchemaName:   "analytics",
		TableName:    "orders",
		Namespace:    "default",
		NodeType:     pkg_model.NodeTypeDbtModel,
		ImageTag:     "v1",
	}

	require.NoError(t, c.CreateQueryJob(ctx, params))

	job, err := c.clientset.BatchV1().Jobs("default").Get(ctx, params.JobName, metav1.GetOptions{})
	require.NoError(t, err)

	require.NotNil(t, job.Spec.TTLSecondsAfterFinished, "job must set TTLSecondsAfterFinished")
	assert.Equal(t, testJobTTLSeconds, *job.Spec.TTLSecondsAfterFinished)
}

// A client built with a finished-Job TTL puts exactly that TTL, in seconds, on
// every kind of Job it creates: the task Job and the validation, seed-build
// and compile legs.
func TestEveryJobKindCarriesTheClientsJobTTL(t *testing.T) {
	t.Setenv("VALIDATION_IMAGE", "ghcr.io/x/runtime:v1")
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh")
	t.Setenv("S3_SIDECAR_IMAGE", "ghcr.io/x/sidecar:v1")
	cs := fake.NewSimpleClientset()
	c := NewK8sClientFor(cs, slog.New(slog.NewTextHandler(io.Discard, nil)), commandcfg.Defaults(), 45*time.Minute)
	ctx := context.Background()
	cand := ValidationJobParams{ReleaseID: "r1", NodeID: "public.orders", ServiceName: "svc", SchemaName: "public",
		TableName: "orders", NodeType: "dbt-model", ImageTag: "v1", CandidateSchema: "_cand_r1",
		CandidateArtifactURI: "s3://b/sql", ManifestS3URI: "s3://b/manifest.json", Namespace: "ns"}
	require.NoError(t, c.CreateQueryJob(ctx, JobParams{JobName: "task", TaskID: "t", RunID: "s", ServiceName: "svc",
		SchemaName: "public", TableName: "orders", NodeType: "dbt-model", ImageTag: "v1", Namespace: "ns"}))
	v := cand
	v.JobName = "validate"
	require.NoError(t, c.CreateValidationJob(ctx, v))
	sb := cand
	sb.JobName, sb.NodeType = "seed", "dbt-seed"
	require.NoError(t, c.CreateSeedBuildJob(ctx, sb))
	cp := cand
	cp.JobName = "compile"
	require.NoError(t, c.CreateCompileJob(ctx, cp))

	jobs, err := cs.BatchV1().Jobs("ns").List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, jobs.Items, 4)
	for _, j := range jobs.Items {
		require.NotNil(t, j.Spec.TTLSecondsAfterFinished, "Job %s sets no TTLSecondsAfterFinished", j.Name)
		assert.Equal(t, int32(2700), *j.Spec.TTLSecondsAfterFinished, "Job %s", j.Name)
	}
}

// The Job watch selects JobSelector, so every Job execution-controller
// creates, and every pod of one, must carry the label it matches.
func TestEveryJobKindCarriesTheLabelTheWatchSelects(t *testing.T) {
	t.Setenv("VALIDATION_IMAGE", "ghcr.io/x/runtime:v1")
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh")
	t.Setenv("S3_SIDECAR_IMAGE", "ghcr.io/x/sidecar:v1")
	sel, err := labels.Parse(JobSelector)
	require.NoError(t, err)
	cs := fake.NewSimpleClientset()
	c := &K8sClient{logger: slog.New(slog.NewTextHandler(io.Discard, nil)), commands: commandcfg.Defaults()}
	c.setClientsetForTest(cs)
	ctx := context.Background()
	cand := ValidationJobParams{ReleaseID: "r1", NodeID: "public.orders", ServiceName: "svc", SchemaName: "public",
		TableName: "orders", NodeType: "dbt-model", ImageTag: "v1", CandidateSchema: "_cand_r1",
		CandidateArtifactURI: "s3://b/sql", ManifestS3URI: "s3://b/manifest.json", Namespace: "ns"}
	require.NoError(t, c.CreateQueryJob(ctx, JobParams{JobName: "task", TaskID: "t", RunID: "s", ServiceName: "svc",
		SchemaName: "public", TableName: "orders", NodeType: "dbt-model", ImageTag: "v1", Namespace: "ns"}))
	v := cand
	v.JobName = "validate"
	require.NoError(t, c.CreateValidationJob(ctx, v))
	sb := cand
	sb.JobName, sb.NodeType = "seed", "dbt-seed"
	require.NoError(t, c.CreateSeedBuildJob(ctx, sb))
	cp := cand
	cp.JobName = "compile"
	require.NoError(t, c.CreateCompileJob(ctx, cp))

	jobs, err := cs.BatchV1().Jobs("ns").List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, jobs.Items, 4)
	for _, j := range jobs.Items {
		assert.True(t, sel.Matches(labels.Set(j.Labels)), "Job %s", j.Name)
		assert.True(t, sel.Matches(labels.Set(j.Spec.Template.Labels)), "pod template of Job %s", j.Name)
	}
}

func TestRateLimit_AppliesToTheRestConfig(t *testing.T) {
	rc := &rest.Config{}
	RateLimit{QPS: 50, Burst: 100}.apply(rc)
	assert.InDelta(t, 50, rc.QPS, 0)
	assert.Equal(t, 100, rc.Burst)
}
