package k8s

import (
	"context"
	"testing"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// pythonParams returns JobParams for a python-model run task.
func pythonParams() JobParams {
	return JobParams{
		JobName:     "run-py-probe",
		TaskID:      "task-1",
		ScheduleID:  "sched-1",
		ServiceName: "svc-py",
		SchemaName:  "analytics",
		TableName:   "py_probe",
		Namespace:   "default",
		NodeType:    pkg_model.NodeTypePythonModel,
		ImageTag:    "ghcr.io/acme/marketing-py:12-abc1234",
	}
}

// TestBuildPythonPodSpec_InjectsExactlyTheNormativeEnv pins the boundary
// contract's env table as a closed set. It is asserted exactly, not as a
// subset: every variable the executor injects becomes a surface a domain image
// can read, and therefore one we can never remove.
func TestBuildPythonPodSpec_InjectsExactlyTheNormativeEnv(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "warehouse-conn")

	spec, err := buildPythonPodSpec(pythonParams())
	require.NoError(t, err)
	require.Len(t, spec.Containers, 1)

	assert.Equal(t, map[string]string{
		"NODE_ID":       "analytics.py_probe",
		"TABLE_NAME":    "py_probe",
		"TARGET_SCHEMA": "analytics",
	}, envMap(spec.Containers[0].Env),
		"the python pod env is exactly the three normative variables")
}

// TestBuildPythonPodSpec_OmitsDbtEnvNames is the negative half of the contract.
// The harness requires TARGET_SCHEMA and recognizes no fallback, so injecting
// the dbt names would silently misroute a node's output; CONTRACT_DIR and
// APP_ROOT are declared by the image, which owns its own layout.
func TestBuildPythonPodSpec_OmitsDbtEnvNames(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "warehouse-conn")

	spec, err := buildPythonPodSpec(pythonParams())
	require.NoError(t, err)

	env := envMap(spec.Containers[0].Env)
	for _, name := range []string{
		"SCHEMA", "DBT_TARGET_SCHEMA", "CONTRACT_DIR", "APP_ROOT",
		"SERVICE_NAME", "JOB_NAME", "TASK_ID", "SCHEDULE_ID", "SCHEDULE_NAME",
	} {
		assert.NotContains(t, env, name, "%s must not be injected into a python pod", name)
	}
}

// TestBuildPythonPodSpec_ImageIsVerbatim verifies the release's image_tag is a
// complete registry reference used as-is — never composed with the service name
// and never prefixed, which is how dbt team images are resolved.
func TestBuildPythonPodSpec_ImageIsVerbatim(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "warehouse-conn")
	t.Setenv("DOCKERHUB_USERNAME", "carolsimone")

	spec, err := buildPythonPodSpec(pythonParams())
	require.NoError(t, err)

	assert.Equal(t, "ghcr.io/acme/marketing-py:12-abc1234", spec.Containers[0].Image)
}

// TestBuildPythonPodSpec_RunsTheImageEntrypoint verifies no command is set: the
// base image's entrypoint is the harness the whole contract is built around,
// and overriding it would bypass the sole write sink.
func TestBuildPythonPodSpec_RunsTheImageEntrypoint(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "warehouse-conn")

	spec, err := buildPythonPodSpec(pythonParams())
	require.NoError(t, err)

	assert.Nil(t, spec.Containers[0].Command)
	assert.Nil(t, spec.Containers[0].Args)
}

// TestBuildPythonPodSpec_OmitsDbtPlumbing verifies none of the dbt Job's pod
// plumbing leaks onto a python pod. S3_BUCKET is set deliberately: that is
// exactly what makes the dbt path attach a parse-cache initContainer.
func TestBuildPythonPodSpec_OmitsDbtPlumbing(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "warehouse-conn")
	t.Setenv("S3_BUCKET", "continuo-artifacts")
	t.Setenv("AWS_ACCESS_KEY_ID", "key")
	t.Setenv("S3_ENDPOINT_URL", "http://localstack:4566")

	spec, err := buildPythonPodSpec(pythonParams())
	require.NoError(t, err)

	assert.Empty(t, spec.InitContainers, "no parse-cache hydrate initContainer")
	assert.Empty(t, spec.Volumes)
	assert.Empty(t, spec.Containers[0].VolumeMounts)

	env := envMap(spec.Containers[0].Env)
	for _, name := range []string{
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "S3_ENDPOINT_URL", "AWS_DEFAULT_REGION",
	} {
		assert.NotContains(t, env, name, "%s must not reach a domain image", name)
	}
}

// TestBuildPythonPodSpec_AttachesWarehouseSecret verifies the pod gets its
// warehouse connection from the same operator-owned Secret dbt Jobs attach —
// its engine-native keys are what the image's baked adapter reads.
func TestBuildPythonPodSpec_AttachesWarehouseSecret(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "warehouse-conn")

	spec, err := buildPythonPodSpec(pythonParams())
	require.NoError(t, err)

	require.Len(t, spec.Containers[0].EnvFrom, 1)
	require.NotNil(t, spec.Containers[0].EnvFrom[0].SecretRef)
	assert.Equal(t, "warehouse-conn", spec.Containers[0].EnvFrom[0].SecretRef.Name)
}

// TestBuildPythonPodSpec_MissingWarehouseSecretIsPermanent verifies an
// unconfigured Secret fails the node with an actionable reason rather than
// launching a pod that can only error.
func TestBuildPythonPodSpec_MissingWarehouseSecretIsPermanent(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "")

	_, err := buildPythonPodSpec(pythonParams())

	require.Error(t, err)
	assert.ErrorIs(t, err, pkgevents.ErrPermanent)
}

// TestBuildPythonPodSpec_ImageTagValidation verifies an unusable image
// reference is refused permanently. An untagged reference resolves to :latest
// implicitly, which makes the code that actually ran unidentifiable from the
// release that promoted it — refused for python for the same reason it is
// refused for dbt images.
func TestBuildPythonPodSpec_ImageTagValidation(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "warehouse-conn")

	const digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	for _, tc := range []struct {
		name     string
		imageTag string
		wantErr  bool
	}{
		{"empty", "", true},
		{"untagged", "ghcr.io/acme/marketing-py", true},
		{"registry port only, untagged", "registry.local:5000/marketing-py", true},
		{"tagged", "ghcr.io/acme/marketing-py:12-abc1234", false},
		{"tagged with registry port", "registry.local:5000/marketing-py:v1", false},
		{"digest", "ghcr.io/acme/marketing-py@" + digest, false},
		{"bare name with tag (side-loaded)", "continuo-e2e-py-probe:latest", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := pythonParams()
			p.ImageTag = tc.imageTag

			spec, err := buildPythonPodSpec(p)

			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, pkgevents.ErrPermanent)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.imageTag, spec.Containers[0].Image)
		})
	}
}

// TestBuildPythonPodSpec_Operations verifies the allowlist. build dispatches
// identically to run — a python node declares no tests, so build's test half is
// vacuous, exactly as `dbt build` on a testless model is `dbt run`. Anything
// else is refused rather than silently treated as a materialization.
func TestBuildPythonPodSpec_Operations(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "warehouse-conn")

	runSpec, err := buildPythonPodSpec(pythonParams())
	require.NoError(t, err)

	buildParams := pythonParams()
	buildParams.Operation = pkg_model.OperationBuild
	buildSpec, err := buildPythonPodSpec(buildParams)
	require.NoError(t, err)
	assert.Equal(t, runSpec, buildSpec, "build must dispatch identically to run")

	for _, op := range []pkg_model.Operation{pkg_model.OperationTest, pkg_model.Operation("snapshot")} {
		testParams := pythonParams()
		testParams.Operation = op
		_, err := buildPythonPodSpec(testParams)
		require.Error(t, err, "operation %q must be refused", op)
		assert.ErrorIs(t, err, pkgevents.ErrPermanent)
	}
}

// TestBuildPythonPodSpec_SecurityContext verifies the domain image is hardened
// like a team dbt image and NOT forced to a fixed uid: the runtime base is
// python:3.14-slim with no USER directive, so forcing runAsUser would make
// every domain image fail to start.
func TestBuildPythonPodSpec_SecurityContext(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "warehouse-conn")

	spec, err := buildPythonPodSpec(pythonParams())
	require.NoError(t, err)

	sc := spec.Containers[0].SecurityContext
	require.NotNil(t, sc)
	require.NotNil(t, sc.AllowPrivilegeEscalation)
	assert.False(t, *sc.AllowPrivilegeEscalation)
	assert.Nil(t, sc.RunAsUser, "the domain image chooses its own user")

	assert.Equal(t, corev1.RestartPolicyNever, spec.RestartPolicy)
	require.NotNil(t, spec.SecurityContext)
	require.NotNil(t, spec.SecurityContext.SeccompProfile)
	assert.Equal(t, corev1.SeccompProfileTypeRuntimeDefault, spec.SecurityContext.SeccompProfile.Type)
}

// TestCreateQueryJob_PythonModel_UsesPythonPodSpec verifies the dispatch branch
// reaches the Kubernetes API: the created Job carries the shared production
// labels (so k8s-controller routes it through the production lifecycle and the
// concurrency cap counts it), plus the runtime label, and its pod is the python
// one.
func TestCreateQueryJob_PythonModel_UsesPythonPodSpec(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "warehouse-conn")
	client := newValidationTestClient()

	require.NoError(t, client.CreateQueryJob(context.Background(), pythonParams()))

	job := fetchJob(t, client, "default", "run-py-probe")
	assert.Equal(t, "dbt-job", job.Labels["app"],
		"the executor job selector drives the concurrency cap and must not change")
	assert.Equal(t, "python", job.Labels["runtime"])
	assert.NotContains(t, job.Labels, "mode", "a production run Job carries no mode label")
	for k, v := range job.Labels {
		assert.Empty(t, validation.IsValidLabelValue(v), "label %s=%q must be a valid label value", k, v)
	}

	podSpec := job.Spec.Template.Spec
	require.Len(t, podSpec.Containers, 1)
	assert.Equal(t, "ghcr.io/acme/marketing-py:12-abc1234", podSpec.Containers[0].Image)
	assert.Equal(t, "analytics.py_probe", envByName(podSpec, "NODE_ID"))
	assert.Empty(t, podSpec.InitContainers)
}

// TestCreateQueryJob_PythonModel_IsIdempotent verifies a redelivered command
// does not duplicate the Job.
func TestCreateQueryJob_PythonModel_IsIdempotent(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "warehouse-conn")
	client := newValidationTestClient()

	require.NoError(t, client.CreateQueryJob(context.Background(), pythonParams()))
	require.NoError(t, client.CreateQueryJob(context.Background(), pythonParams()))
}

// TestCreateQueryJob_DbtModel_PodSpecUnchanged pins the branch as inert for
// dbt: a dbt run Job's pod spec must equal what buildPodSpec produces directly,
// including the parse-cache initContainer S3_BUCKET triggers.
func TestCreateQueryJob_DbtModel_PodSpecUnchanged(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "warehouse-conn")
	t.Setenv("S3_BUCKET", "continuo-artifacts")
	client := newValidationTestClient()

	params := pythonParams()
	params.JobName = "run-dbt-orders"
	params.NodeType = pkg_model.NodeTypeDbtModel
	params.ServiceName = "service-1"
	params.TableName = "orders"
	params.ImageTag = "abc123"

	require.NoError(t, client.CreateQueryJob(context.Background(), params))

	want, err := buildPodSpec(params,
		client.commands.NodeCommand(params.ServiceName, params.Operation, params.NodeType, params.TableName),
		client.commands.PartialParsePath(params.ServiceName))
	require.NoError(t, err)

	job := fetchJob(t, client, "default", "run-dbt-orders")
	assert.Equal(t, want, job.Spec.Template.Spec)
	assert.Len(t, job.Spec.Template.Spec.InitContainers, 1,
		"the dbt parse-cache initContainer must still be attached")
	assert.NotContains(t, job.Labels, "runtime", "dbt Jobs carry no runtime label")
}
