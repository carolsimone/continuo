package k8s

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func assertBaseHardening(t *testing.T, sc *corev1.SecurityContext) {
	t.Helper()
	require.NotNil(t, sc)
	require.NotNil(t, sc.AllowPrivilegeEscalation)
	assert.False(t, *sc.AllowPrivilegeEscalation)
	require.NotNil(t, sc.Capabilities)
	assert.Equal(t, []corev1.Capability{"ALL"}, sc.Capabilities.Drop)
}

func assertNonRoot(t *testing.T, sc *corev1.SecurityContext) {
	t.Helper()
	assertBaseHardening(t, sc)
	require.NotNil(t, sc.RunAsNonRoot)
	assert.True(t, *sc.RunAsNonRoot)
	require.NotNil(t, sc.RunAsUser)
	assert.Equal(t, int64(65532), *sc.RunAsUser)
}

func assertPodHardening(t *testing.T, spec corev1.PodSpec) {
	t.Helper()
	require.NotNil(t, spec.SecurityContext)
	require.NotNil(t, spec.SecurityContext.SeccompProfile)
	assert.Equal(t, corev1.SeccompProfileTypeRuntimeDefault, spec.SecurityContext.SeccompProfile.Type)
}

func TestBuildPodSpecHardening(t *testing.T) {
	spec, err := buildPodSpec(JobParams{
		JobName: "j", TaskID: "t", ServiceName: "svc", SchemaName: "s",
		TableName: "tbl", Namespace: "default", ImageTag: "abc",
	}, []string{"dbt", "run"})
	require.NoError(t, err)
	assertPodHardening(t, spec)
	// Team image: hardened, but the user is the team's choice (dbt image
	// contract) — runAsNonRoot must NOT be forced.
	assertBaseHardening(t, spec.Containers[0].SecurityContext)
	assert.Nil(t, spec.Containers[0].SecurityContext.RunAsNonRoot)
}

func TestBuildSeedBuildPodSpecHardening(t *testing.T) {
	p := validationParams()
	spec, err := buildSeedBuildPodSpec(p, []string{"dbt", "seed"})
	require.NoError(t, err)
	assertPodHardening(t, spec)
	assertBaseHardening(t, spec.Containers[0].SecurityContext)
	assert.Nil(t, spec.Containers[0].SecurityContext.RunAsNonRoot)
}

func TestBuildValidationPodSpecHardening(t *testing.T) {
	p := validationParams()
	p.ValidationOp = "clone_from_prod"
	spec, err := buildValidationPodSpec(p)
	require.NoError(t, err)
	assertPodHardening(t, spec)
	// validation-runner is continuo-owned and built non-root (uid 65532).
	assertNonRoot(t, spec.Containers[0].SecurityContext)
}

func TestBuildCompilePodSpecHardening(t *testing.T) {
	p := validationParams()
	p.ManifestS3URI = "s3://bucket/svc/rel/manifest.json"
	spec, err := buildCompilePodSpec(p, []string{"dbt", "compile"}, "target/manifest.json")
	require.NoError(t, err)
	assertPodHardening(t, spec)
	// init "compile" runs the team image: base hardening only.
	assertBaseHardening(t, spec.InitContainers[0].SecurityContext)
	assert.Nil(t, spec.InitContainers[0].SecurityContext.RunAsNonRoot)
	// main "upload" runs the continuo-owned s3-sidecar: non-root forced.
	assertNonRoot(t, spec.Containers[0].SecurityContext)
}
