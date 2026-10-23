package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	require.Equal(t, 15*time.Minute, cfg.JobStartTimeout)
	require.Equal(t, 50, cfg.LogTailLines)
	require.Equal(t, 4096, cfg.ErrorMessageMaxLength)
	require.Equal(t, "b", cfg.S3.Bucket)
	require.Equal(t, "require", cfg.Postgres.SSLMode)
}

// Validation, seed-build and compile Jobs run up to TASK_TIMEOUT_SECONDS; unset
// selects the chart's default.
func TestLoad_TaskTimeout(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("TASK_TIMEOUT_SECONDS", "")
	v := &pkgconfig.Validator{}
	require.Equal(t, 24*time.Hour, Load(v).TaskTimeout)
	t.Setenv("TASK_TIMEOUT_SECONDS", "7200")
	require.Equal(t, 2*time.Hour, Load(v).TaskTimeout)
	t.Setenv("TASK_TIMEOUT_SECONDS", "60")
	Load(v)
	assert.Contains(t, strings.Join(v.Missing(), " "), "TASK_TIMEOUT_SECONDS")
}

func TestLoad_JobStartTimeout(t *testing.T) {
	cases := []struct {
		raw     string
		want    time.Duration
		invalid bool
	}{
		{"", 15 * time.Minute, false},
		{"180", 3 * time.Minute, false},
		{"60", time.Minute, false},
		{"7200", 2 * time.Hour, false},
		{"59", 0, true},
		{"7201", 0, true},
		{"3m", 0, true},
		// 18446744674 s is 2^64 ns plus about ten minutes: refused, though its
		// nanoseconds would wrap into the range.
		{"18446744674", 0, true},
	}
	for _, c := range cases {
		t.Run(c.raw, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("JOB_START_TIMEOUT_SECONDS", c.raw)
			v := &pkgconfig.Validator{}
			cfg := Load(v)
			if c.invalid {
				require.NotEmpty(t, v.Missing(), "a start timeout the service cannot honour stops it at boot")
				assert.Contains(t, strings.Join(v.Missing(), ","), "JOB_START_TIMEOUT_SECONDS")
				return
			}
			require.Empty(t, v.Missing())
			assert.Equal(t, c.want, cfg.JobStartTimeout)
		})
	}
}

// The chart's execution.startTimeoutMinutes is the value the service accepts,
// in minutes: a value the chart allows but the service refused would render
// and then stop execution-controller at startup.
func TestJobStartTimeoutRangeMatchesTheChart(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "continuo", "values.schema.json"))
	require.NoError(t, err)
	var schema struct {
		Properties struct {
			Execution struct {
				Properties struct {
					StartTimeoutMinutes struct {
						Minimum int `json:"minimum"`
						Maximum int `json:"maximum"`
					} `json:"startTimeoutMinutes"`
				} `json:"properties"`
			} `json:"execution"`
		} `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(raw, &schema))
	got := schema.Properties.Execution.Properties.StartTimeoutMinutes
	assert.Equal(t, MinJobStartTimeout, time.Duration(got.Minimum)*time.Minute)
	assert.Equal(t, MaxJobStartTimeout, time.Duration(got.Maximum)*time.Minute)
}

func TestKubeClientRateLimit(t *testing.T) {
	cases := []struct {
		name        string
		qps, burst  string
		wantQPS     int
		wantBurst   int
		wantInvalid []string
	}{
		{name: "unset selects the defaults", wantQPS: 50, wantBurst: 100},
		{name: "both set", qps: "20", burst: "40", wantQPS: 20, wantBurst: 40},
		{name: "lowest values", qps: "1", burst: "1", wantQPS: 1, wantBurst: 1},
		{name: "highest values", qps: "1000", burst: "2000", wantQPS: 1000, wantBurst: 2000},
		{name: "burst equal to qps", qps: "30", burst: "30", wantQPS: 30, wantBurst: 30},
		{name: "qps zero", qps: "0", burst: "100", wantInvalid: []string{"K8S_CLIENT_QPS"}},
		{name: "qps above the range", qps: "1001", burst: "2000", wantInvalid: []string{"K8S_CLIENT_QPS"}},
		{name: "qps not a whole number", qps: "2.5", burst: "100", wantInvalid: []string{"K8S_CLIENT_QPS"}},
		{name: "qps not a number", qps: "fast", burst: "100", wantInvalid: []string{"K8S_CLIENT_QPS"}},
		{name: "burst zero", qps: "10", burst: "0", wantInvalid: []string{"K8S_CLIENT_BURST"}},
		{name: "burst above the range", qps: "10", burst: "2001", wantInvalid: []string{"K8S_CLIENT_BURST"}},
		{name: "burst below qps", qps: "50", burst: "49", wantInvalid: []string{"K8S_CLIENT_BURST"}},
		// An explicit qps above the default burst needs an explicit burst.
		{name: "qps above the default burst", qps: "200", wantInvalid: []string{"K8S_CLIENT_BURST"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("K8S_CLIENT_QPS", c.qps)
			t.Setenv("K8S_CLIENT_BURST", c.burst)
			v := &pkgconfig.Validator{}
			cfg := Load(v)
			if len(c.wantInvalid) > 0 {
				require.NotEmpty(t, v.Missing(), "a rate limit the service cannot honour stops it at boot")
				for _, key := range c.wantInvalid {
					assert.Contains(t, strings.Join(v.Missing(), ","), key)
				}
				return
			}
			require.Empty(t, v.Missing())
			assert.Equal(t, c.wantQPS, cfg.K8sClientQPS)
			assert.Equal(t, c.wantBurst, cfg.K8sClientBurst)
		})
	}
}

// The chart's execution.kubeClientQPS and execution.kubeClientBurst accept
// the ranges the service accepts: a value the chart allows but the service
// refused would render and then stop execution-controller at startup.
func TestKubeClientRateLimitRangesMatchTheChart(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "continuo", "values.schema.json"))
	require.NoError(t, err)
	type bounds struct {
		Minimum int `json:"minimum"`
		Maximum int `json:"maximum"`
	}
	var schema struct {
		Properties struct {
			Execution struct {
				Properties struct {
					KubeClientQPS   bounds `json:"kubeClientQPS"`
					KubeClientBurst bounds `json:"kubeClientBurst"`
				} `json:"properties"`
			} `json:"execution"`
		} `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(raw, &schema))
	qps := schema.Properties.Execution.Properties.KubeClientQPS
	burst := schema.Properties.Execution.Properties.KubeClientBurst
	assert.Equal(t, MinK8sClientQPS, qps.Minimum)
	assert.Equal(t, MaxK8sClientQPS, qps.Maximum)
	assert.Equal(t, MinK8sClientBurst, burst.Minimum)
	assert.Equal(t, MaxK8sClientBurst, burst.Maximum)
}
