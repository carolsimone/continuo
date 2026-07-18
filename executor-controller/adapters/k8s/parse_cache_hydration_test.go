package k8s

import (
	"testing"

	"github.com/carolsimone/continuo/executor-controller/domain/deploy"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

// envVarsByName returns a name->value map for a container's Env, for
// order-independent assertions.
func envVarsByName(env []corev1.EnvVar) map[string]string {
	out := make(map[string]string, len(env))
	for _, e := range env {
		out[e.Name] = e.Value
	}
	return out
}

// TestBuildPodSpec_HydratesProdParseCache pins the production run Job's
// hydrate-parse-cache initContainer: image, command, env, volume wiring, and
// that the team container gains only the parse-cache mount — its env/command
// stay exactly what they were before this feature.
func TestBuildPodSpec_HydratesProdParseCache(t *testing.T) {
	t.Setenv("S3_BUCKET", "continuo")
	t.Setenv("DOCKERHUB_USERNAME", "")
	t.Setenv("S3_ENDPOINT_URL", "http://minio:9000")
	t.Setenv("AWS_ACCESS_KEY_ID", "key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_DEFAULT_REGION", "us-east-1")

	params := JobParams{
		ServiceName: "service-1",
		ImageTag:    "abc123",
		NodeType:    pkg_model.NodeTypeDbtModel,
		TableName:   "orders",
	}
	command := params.NodeType.Command(params.TableName)
	wantCommand := append([]string(nil), command...)
	// Team container env/command must stay exactly what buildPodSpec produced
	// before this feature: the JobParams-derived vars plus dbtConnectionEnvVars().
	wantEnv := append([]corev1.EnvVar{
		{Name: "TASK_ID", Value: params.TaskID},
		{Name: "SCHEDULE_ID", Value: params.ScheduleID},
		{Name: "SCHEDULE_NAME", Value: params.ScheduleName},
		{Name: "SERVICE_NAME", Value: params.ServiceName},
		{Name: "SCHEMA", Value: params.SchemaName},
		{Name: "TABLE_NAME", Value: params.TableName},
		{Name: "JOB_NAME", Value: params.JobName},
	}, dbtConnectionEnvVars()...)

	spec, err := buildPodSpec(params, command, "/project/target/partial_parse.msgpack")
	require.NoError(t, err)

	require.Len(t, spec.InitContainers, 1, "expected exactly one initContainer")
	init := spec.InitContainers[0]
	assert.Equal(t, "hydrate-parse-cache", init.Name)
	assert.Equal(t, s3SidecarImage(), init.Image)
	assert.Equal(t, []string{"python", "/parse_cache_fetcher.py"}, init.Command)

	wantURI := deploy.ParseCacheProdURI("continuo", "service-1", "abc123")
	gotInitEnv := envVarsByName(init.Env)
	assert.Equal(t, wantURI, gotInitEnv["PARSE_CACHE_S3_URI"])
	assert.Equal(t, "/parse-cache/partial_parse.msgpack", gotInitEnv["PARSE_CACHE_DEST"])
	for _, e := range s3CredEnvVars() {
		assert.Equal(t, e.Value, gotInitEnv[e.Name], "init container must forward s3CredEnvVars()")
	}
	// S3 creds must NEVER land on the team container.
	teamEnv := envVarsByName(spec.Containers[0].Env)
	for _, e := range s3CredEnvVars() {
		_, present := teamEnv[e.Name]
		assert.False(t, present, "team container must not receive S3 credential %s", e.Name)
	}

	require.Len(t, spec.Volumes, 1)
	vol := spec.Volumes[0]
	assert.Equal(t, "parse-cache", vol.Name)
	require.NotNil(t, vol.EmptyDir)

	require.Len(t, init.VolumeMounts, 1)
	assert.Equal(t, "parse-cache", init.VolumeMounts[0].Name)
	assert.Equal(t, "/parse-cache", init.VolumeMounts[0].MountPath)

	require.Len(t, spec.Containers, 1)
	require.Len(t, spec.Containers[0].VolumeMounts, 1)
	assert.Equal(t, "parse-cache", spec.Containers[0].VolumeMounts[0].Name)
	assert.Equal(t, "/project/target", spec.Containers[0].VolumeMounts[0].MountPath)

	// Team container command/env unchanged from today's pre-feature spec.
	assert.Equal(t, wantCommand, spec.Containers[0].Command)
	assert.Equal(t, wantEnv, spec.Containers[0].Env)
}

// TestBuildPodSpec_NoHydrationWithoutBucket pins that with S3_BUCKET unset
// (or empty) the spec is byte-identical to the pre-feature spec: no
// initContainers, no volumes, no extra mounts.
func TestBuildPodSpec_NoHydrationWithoutBucket(t *testing.T) {
	t.Setenv("S3_BUCKET", "")

	params := JobParams{
		ServiceName: "service-1",
		ImageTag:    "abc123",
		NodeType:    pkg_model.NodeTypeDbtModel,
		TableName:   "orders",
	}
	spec, err := buildPodSpec(params, params.NodeType.Command(params.TableName), "/project/target/partial_parse.msgpack")
	require.NoError(t, err)

	assert.Empty(t, spec.InitContainers)
	assert.Empty(t, spec.Volumes)
	require.Len(t, spec.Containers, 1)
	assert.Empty(t, spec.Containers[0].VolumeMounts)
}

// TestBuildSeedBuildPodSpec_HydratesCandidateParseCache mirrors the prod test
// for the seed-build Job, whose cache key is the release ID rather than the
// image tag.
func TestBuildSeedBuildPodSpec_HydratesCandidateParseCache(t *testing.T) {
	t.Setenv("S3_BUCKET", "continuo")
	t.Setenv("DOCKERHUB_USERNAME", "")
	t.Setenv("S3_ENDPOINT_URL", "http://minio:9000")
	t.Setenv("AWS_ACCESS_KEY_ID", "key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_DEFAULT_REGION", "us-east-1")

	p := validationParams()
	p.ReleaseID = "rel123"
	p.ServiceName = "service-1"

	spec, err := buildSeedBuildPodSpec(p, []string{"dbt", "seed", "--select", p.TableName}, "/project/target/partial_parse.msgpack")
	require.NoError(t, err)

	require.Len(t, spec.InitContainers, 1)
	init := spec.InitContainers[0]
	assert.Equal(t, "hydrate-parse-cache", init.Name)
	assert.Equal(t, s3SidecarImage(), init.Image)
	assert.Equal(t, []string{"python", "/parse_cache_fetcher.py"}, init.Command)

	wantURI := deploy.ParseCacheCandidateURI("continuo", "service-1", "rel123")
	gotInitEnv := envVarsByName(init.Env)
	assert.Equal(t, wantURI, gotInitEnv["PARSE_CACHE_S3_URI"])
	assert.Equal(t, "/parse-cache/partial_parse.msgpack", gotInitEnv["PARSE_CACHE_DEST"])

	require.Len(t, spec.Volumes, 1)
	assert.Equal(t, "parse-cache", spec.Volumes[0].Name)
	require.NotNil(t, spec.Volumes[0].EmptyDir)

	require.Len(t, spec.Containers, 1)
	require.Len(t, spec.Containers[0].VolumeMounts, 1)
	assert.Equal(t, "parse-cache", spec.Containers[0].VolumeMounts[0].Name)
	assert.Equal(t, "/project/target", spec.Containers[0].VolumeMounts[0].MountPath)
}

// TestBuildSeedBuildPodSpec_NoHydrationWithoutBucket mirrors the prod
// no-bucket test for the seed-build Job.
func TestBuildSeedBuildPodSpec_NoHydrationWithoutBucket(t *testing.T) {
	t.Setenv("S3_BUCKET", "")
	p := validationParams()
	spec, err := buildSeedBuildPodSpec(p, []string{"dbt", "seed"}, "/project/target/partial_parse.msgpack")
	require.NoError(t, err)
	assert.Empty(t, spec.InitContainers)
	assert.Empty(t, spec.Volumes)
	require.Len(t, spec.Containers, 1)
	assert.Empty(t, spec.Containers[0].VolumeMounts)
}

// TestDbtConnectionEnvDriftGuard pins the spec §6 invariant: the rehearsal
// containers (buildCompilePodSpec's parse-prod/parse-candidate legs) build
// their dbt connection env from the exact same function as the run pods
// (buildPodSpec, buildSeedBuildPodSpec). If any of the four builders diverge
// from dbtConnectionEnvVars(), a hydrated partial-parse cache silently stops
// matching the run pod's actual connection and this test catches it.
func TestDbtConnectionEnvDriftGuard(t *testing.T) {
	want := dbtConnectionEnvVars()

	assertSubset := func(t *testing.T, label string, env []corev1.EnvVar) {
		t.Helper()
		got := envVarsByName(env)
		for _, e := range want {
			gotVal, present := got[e.Name]
			require.True(t, present, "%s: missing dbtConnectionEnvVars() var %s", label, e.Name)
			assert.Equal(t, e.Value, gotVal, "%s: value drift for %s", label, e.Name)
		}
	}

	runSpec, err := buildPodSpec(JobParams{
		ServiceName: "service-1",
		ImageTag:    "abc123",
		NodeType:    pkg_model.NodeTypeDbtModel,
		TableName:   "orders",
	}, pkg_model.NodeTypeDbtModel.Command("orders"), "/project/target/partial_parse.msgpack")
	require.NoError(t, err)
	assertSubset(t, "buildPodSpec team container", runSpec.Containers[0].Env)

	seedSpec, err := buildSeedBuildPodSpec(validationParams(), []string{"dbt", "seed"}, "/project/target/partial_parse.msgpack")
	require.NoError(t, err)
	assertSubset(t, "buildSeedBuildPodSpec team container", seedSpec.Containers[0].Env)

	p := validationParams()
	p.ManifestS3URI = "s3://bucket/svc/rel/manifest.json"
	compileSpec, err := buildCompilePodSpec(p, []string{"dbt", "compile"}, "target/manifest.json",
		[]string{"dbt", "parse"}, "target/partial_parse.msgpack")
	require.NoError(t, err)
	require.Len(t, compileSpec.InitContainers, 3, "expected compile + parse-prod + parse-candidate")
	var prod, candidate corev1.Container
	for _, c := range compileSpec.InitContainers {
		switch c.Name {
		case "parse-prod":
			prod = c
		case "parse-candidate":
			candidate = c
		}
	}
	require.NotEmpty(t, prod.Name, "parse-prod initContainer must exist when CandidateSchema is set")
	require.NotEmpty(t, candidate.Name, "parse-candidate initContainer must exist when CandidateSchema is set")
	assertSubset(t, "buildCompilePodSpec parse-prod", prod.Env)
	assertSubset(t, "buildCompilePodSpec parse-candidate", candidate.Env)
}
