package k8s

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/adapters/commandcfg"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
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

// newQueryTestClient creates a K8sClient backed by a fake clientset for unit tests.
func newQueryTestClient() *K8sClient {
	cs := fake.NewSimpleClientset()
	c := &K8sClient{logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), commands: commandcfg.Defaults()}
	c.setClientsetForTest(cs)
	return c
}

// TestCreateQueryJob_NormalProduction_HasNoModeLabel verifies that a production
// dbt job carries NO "mode" label. k8s-controller routes a terminal Job by that
// label, and only the release legs (validation, seed-build, compile) set one; a
// query job must fall through to the production lifecycle that owns retries,
// task executions, and status.
func TestCreateQueryJob_NormalProduction_HasNoModeLabel(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newQueryTestClient()
	ctx := context.Background()

	params := JobParams{
		JobName:      "prod-svc-analytics-orders-abc456",
		TaskID:       "task-2",
		ScheduleID:   "sched-2",
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
// Job carries a TTLSecondsAfterFinished so Kubernetes garbage-collects the Job
// (and its pod) some time after it terminates, instead of leaving failed pods on
// the cluster indefinitely.
func TestCreateQueryJob_SetsTTLSecondsAfterFinished(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newQueryTestClient()
	ctx := context.Background()

	params := JobParams{
		JobName:      "prod-svc-analytics-orders-abc456",
		TaskID:       "task-3",
		ScheduleID:   "sched-3",
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
	assert.Equal(t, jobTTLSecondsAfterFinished, *job.Spec.TTLSecondsAfterFinished)
}
