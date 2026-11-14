package k8s

import (
	"slices"
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

// envVarByName returns the env var of c named name.
func envVarByName(c corev1.Container, name string) (corev1.EnvVar, bool) {
	for _, e := range c.Env {
		if e.Name == name {
			return e, true
		}
	}
	return corev1.EnvVar{}, false
}

// assertS3KeyFromJobSecret fails unless c carries name as a reference to the
// key of the same name in the Secret continuo-job-s3, and never as a value.
func assertS3KeyFromJobSecret(t *testing.T, c corev1.Container, name string) {
	t.Helper()
	e, ok := envVarByName(c, name)
	require.True(t, ok, name)
	assert.Empty(t, e.Value, name)
	require.NotNil(t, e.ValueFrom, name)
	require.NotNil(t, e.ValueFrom.SecretKeyRef, name)
	assert.Equal(t, "continuo-job-s3", e.ValueFrom.SecretKeyRef.Name, name)
	assert.Equal(t, name, e.ValueFrom.SecretKeyRef.Key, name)
}

// jobPodSpecKinds builds the pod spec of every Job kind execution-controller
// creates, with every S3 setting present so each container that can carry the
// S3 credentials does.
func jobPodSpecKinds(t *testing.T) map[string]corev1.PodSpec {
	t.Helper()
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "warehouse-conn")
	t.Setenv("VALIDATION_IMAGE", "ghcr.io/x/continuo-python-runtime-postgres:test")
	t.Setenv("S3_BUCKET", "continuo-artifacts")
	t.Setenv("S3_ENDPOINT_URL", "http://minio:9000")
	t.Setenv("AWS_DEFAULT_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "plain-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "plain-secret-key")
	t.Setenv("JOB_S3_SECRET_NAME", "continuo-job-s3")

	specs := map[string]corev1.PodSpec{}
	add := func(kind string, spec corev1.PodSpec, err error) {
		t.Helper()
		require.NoError(t, err, kind)
		specs[kind] = spec
	}

	dbt := jobParamsFromSpec(attemptSpec(pkg_model.NodeTypeDbtModel, "v42"), pkg_model.NodeTypeDbtModel, "default")
	spec, err := buildPodSpec(dbt, pkg_model.NodeTypeDbtModel.Command(dbt.TableName), "/project/target/partial_parse.msgpack")
	add("task dbt", spec, err)

	spec, err = buildPythonPodSpec(pythonParams())
	add("task python-node", spec, err)
	spec, err = buildPythonPodSpec(csvParams())
	add("task python-csv", spec, err)

	for _, op := range pkg_model.ValidationOps() {
		p := validationParams()
		p.ValidationOp = string(op)
		spec, err = buildValidationPodSpec(p)
		add("validation "+string(op), spec, err)
	}

	spec, err = buildSeedBuildPodSpec(seedOverlayParams(),
		[]string{"dbt", "seed", "--select", "fx"}, "/project/target/partial_parse.msgpack")
	add("seed-build", spec, err)

	compile := ValidationJobParams{
		ServiceName:         "core",
		ImageTag:            "abc123",
		ManifestS3URI:       "s3://continuo/core/rel-1/manifest.json",
		CandidateSchema:     "_candidate_rel1",
		ParseProdS3URI:      "s3://continuo/core/rel-1/parse/prod.msgpack",
		ParseCandidateS3URI: "s3://continuo/core/rel-1/parse/candidate.msgpack",
		SourceOverlayURI:    "s3://continuo/core/rel-1/source-overlay.tar.gz",
	}
	spec, err = buildCompilePodSpec(compile, []string{"dbt", "compile"}, "target/manifest.json",
		[]string{"dbt", "parse"}, "target/partial_parse.msgpack")
	add("compile", spec, err)

	spec, err = buildSchemaOpPodSpec(schemaOpEnsure, "_candidate_rel1")
	add("schema op", spec, err)
	return specs
}

// No container of any Job carries a credential as a plain value: a Job object
// is readable by anyone who can read Jobs, so the S3 key reaches a pod only
// by Secret reference.
func TestJobContainersCarryNoPlainCredentials(t *testing.T) {
	for kind, spec := range jobPodSpecKinds(t) {
		t.Run(kind, func(t *testing.T) {
			for _, c := range slices.Concat(spec.InitContainers, spec.Containers) {
				for _, e := range c.Env {
					if slices.Contains(pkgconfig.CredentialVars, e.Name) {
						assert.Empty(t, e.Value, "container %s carries %s as a plain value", c.Name, e.Name)
					}
				}
			}
		})
	}
}

// The S3 key of a Job pod is a reference to the Secret JOB_S3_SECRET_NAME
// names, under the keys the chart sets.
func TestS3KeysComeFromTheJobSecret(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "plain-access-key")
	t.Setenv("JOB_S3_SECRET_NAME", "continuo-job-s3")

	t.Run("default keys", func(t *testing.T) {
		t.Setenv("JOB_S3_ACCESS_KEY_ID_KEY", "")
		t.Setenv("JOB_S3_SECRET_ACCESS_KEY_KEY", "")
		c := corev1.Container{Env: s3CredEnvVars()}
		for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"} {
			e, ok := envVarByName(c, name)
			require.True(t, ok, name)
			require.NotNil(t, e.ValueFrom, name)
			assert.Equal(t, &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "continuo-job-s3"},
				Key:                  name,
			}, e.ValueFrom.SecretKeyRef, name)
			assert.Empty(t, e.Value, name)
		}
	})

	t.Run("keys named by the environment", func(t *testing.T) {
		t.Setenv("JOB_S3_ACCESS_KEY_ID_KEY", "access")
		t.Setenv("JOB_S3_SECRET_ACCESS_KEY_KEY", "secret")
		c := corev1.Container{Env: s3CredEnvVars()}
		id, _ := envVarByName(c, "AWS_ACCESS_KEY_ID")
		secret, _ := envVarByName(c, "AWS_SECRET_ACCESS_KEY")
		require.NotNil(t, id.ValueFrom)
		require.NotNil(t, secret.ValueFrom)
		assert.Equal(t, "access", id.ValueFrom.SecretKeyRef.Key)
		assert.Equal(t, "secret", secret.ValueFrom.SecretKeyRef.Key)
	})
}

// Without a Job Secret a Job pod carries no S3 key and uses the SDK's default
// credential chain; the endpoint and region stay plain values.
func TestNoS3KeysWithoutAJobSecret(t *testing.T) {
	t.Setenv("JOB_S3_SECRET_NAME", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "plain-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "plain-secret-key")
	t.Setenv("S3_ENDPOINT_URL", "http://minio:9000")
	t.Setenv("AWS_DEFAULT_REGION", "us-east-2")

	c := corev1.Container{Env: s3CredEnvVars()}
	_, hasID := envVarByName(c, "AWS_ACCESS_KEY_ID")
	_, hasSecret := envVarByName(c, "AWS_SECRET_ACCESS_KEY")
	assert.False(t, hasID)
	assert.False(t, hasSecret)
	endpoint, _ := envVarByName(c, "S3_ENDPOINT_URL")
	region, _ := envVarByName(c, "AWS_DEFAULT_REGION")
	assert.Equal(t, "http://minio:9000", endpoint.Value)
	assert.Equal(t, "us-east-2", region.Value)
}
