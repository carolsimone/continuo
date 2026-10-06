package config

import (
	"os"
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_MaxConcurrentJobsDefault(t *testing.T) {
	os.Setenv("K8S_NAMESPACE", "default")
	os.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh-secret")
	os.Unsetenv("MAX_CONCURRENT_JOBS")
	defer func() { os.Unsetenv("K8S_NAMESPACE"); os.Unsetenv("VALIDATION_WAREHOUSE_SECRET") }()

	cfg := Load(&pkgconfig.Validator{})
	if cfg.MaxConcurrentJobs != 50 {
		t.Fatalf("want default 50, got %d", cfg.MaxConcurrentJobs)
	}
}

func TestLoad_MaxConcurrentJobsFromEnv(t *testing.T) {
	os.Setenv("K8S_NAMESPACE", "default")
	os.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh-secret")
	os.Setenv("MAX_CONCURRENT_JOBS", "12")
	defer func() {
		os.Unsetenv("K8S_NAMESPACE")
		os.Unsetenv("VALIDATION_WAREHOUSE_SECRET")
		os.Unsetenv("MAX_CONCURRENT_JOBS")
	}()

	cfg := Load(&pkgconfig.Validator{})
	if cfg.MaxConcurrentJobs != 12 {
		t.Fatalf("want 12, got %d", cfg.MaxConcurrentJobs)
	}
}

func TestLoad_UnionOfDispatchAndObserveSettings(t *testing.T) {
	for k, v := range map[string]string{
		"REDIS_HOST": "r", "REDIS_PORT": "6379", "REDIS_PASSWORD": "p",
		"POSTGRES_HOST": "h", "POSTGRES_DB": "d", "POSTGRES_USER": "u", "POSTGRES_PASSWORD": "pw",
		"S3_ENDPOINT_URL": "http://minio:9000", "S3_BUCKET": "b", "AWS_DEFAULT_REGION": "us-east-1",
		"K8S_NAMESPACE": "ns", "VALIDATION_WAREHOUSE_SECRET": "sec", "DB_SSLMODE": "require",
	} {
		t.Setenv(k, v)
	}
	v := &pkgconfig.Validator{}
	cfg := Load(v)
	require.Empty(t, v.Missing())
	require.Equal(t, 8084, cfg.HTTPPort)
	require.Equal(t, 50, cfg.MaxConcurrentJobs)
	require.Equal(t, 10, cfg.K8sCheckDelaySeconds)
	require.Equal(t, 1, cfg.K8sFirstCheckDelaySeconds)
	require.Equal(t, 50, cfg.LogTailLines)
	require.Equal(t, 4096, cfg.ErrorMessageMaxLength)
	require.Equal(t, "b", cfg.S3.Bucket)
	require.Equal(t, "require", cfg.Postgres.SSLMode)
}

// Each task's retry budget arrives on its query.model:v1 dispatch. Load names
// DEFAULT_TASK_MAX_RETRIES when it is set, so main can warn that it is ignored.
func TestLoad_NamesTheIgnoredRetryKey(t *testing.T) {
	cases := []struct {
		name, maxRetries string
		want             []string
	}{
		{name: "not set"},
		{name: "set", maxRetries: "3", want: []string{"DEFAULT_TASK_MAX_RETRIES"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DEFAULT_TASK_MAX_RETRIES", tc.maxRetries)
			cfg := Load(&pkgconfig.Validator{})
			assert.Equal(t, tc.want, cfg.IgnoredRetryKeys)
		})
	}
}
