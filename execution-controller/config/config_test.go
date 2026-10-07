package config

import (
	"os"
	"strings"
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_RetentionSweeperSettings(t *testing.T) {
	t.Setenv("K8S_NAMESPACE", "ns")
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "sec")
	t.Setenv("RETENTION_DAYS", "")
	t.Setenv("RETENTION_SWEEP_INTERVAL_MINUTES", "")
	cfg := Load(&pkgconfig.Validator{})
	require.Equal(t, 7, cfg.RetentionDays)
	require.Equal(t, 60, cfg.RetentionSweepIntervalMin)
	t.Setenv("RETENTION_DAYS", "45")
	t.Setenv("RETENTION_SWEEP_INTERVAL_MINUTES", "15")
	cfg = Load(&pkgconfig.Validator{})
	require.Equal(t, 45, cfg.RetentionDays)
	require.Equal(t, 15, cfg.RetentionSweepIntervalMin)
}

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

// setRequiredEnv sets every variable Load requires, so a test sees only the
// validation failures it provokes.
func setRequiredEnv(t *testing.T) {
	t.Helper()
	for k, v := range map[string]string{
		"REDIS_HOST": "r", "REDIS_PORT": "6379", "REDIS_PASSWORD": "p",
		"POSTGRES_HOST": "h", "POSTGRES_DB": "d", "POSTGRES_USER": "u", "POSTGRES_PASSWORD": "pw",
		"S3_ENDPOINT_URL": "http://minio:9000", "S3_BUCKET": "b", "AWS_DEFAULT_REGION": "us-east-1",
		"K8S_NAMESPACE": "ns", "VALIDATION_WAREHOUSE_SECRET": "sec",
	} {
		t.Setenv(k, v)
	}
}

func TestLoad_MaxConcurrentJobs(t *testing.T) {
	cases := []struct {
		raw     string
		want    int
		invalid bool
	}{
		{"", 50, false},
		{"7", 7, false},
		{"0", 0, true},
		{"-3", 0, true},
		{"fifty", 0, true},
	}
	for _, c := range cases {
		t.Run(c.raw, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("MAX_CONCURRENT_JOBS", c.raw)
			v := &pkgconfig.Validator{}
			cfg := Load(v)
			if c.invalid {
				require.NotEmpty(t, v.Missing(), "a cap the service cannot honour stops it at boot")
				assert.Contains(t, strings.Join(v.Missing(), ","), "MAX_CONCURRENT_JOBS")
				return
			}
			require.Empty(t, v.Missing())
			assert.Equal(t, c.want, cfg.MaxConcurrentJobs)
		})
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
