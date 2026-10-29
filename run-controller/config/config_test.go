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

// run-controller sizes its pool from DB_MAX_OPEN_CONNS / DB_MAX_IDLE_CONNS. Load names
// the set pool keys it does not read, so main can warn about them.
func TestLoad_NamesTheIgnoredPoolKeys(t *testing.T) {
	cases := []struct {
		name, poolSize, maxOverflow string
		want                        []string
	}{
		{name: "neither set"},
		{name: "pool size set", poolSize: "20", want: []string{"DB_POOL_SIZE"}},
		{name: "both set", poolSize: "20", maxOverflow: "10", want: []string{"DB_POOL_SIZE", "DB_MAX_OVERFLOW"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DB_POOL_SIZE", tc.poolSize)
			t.Setenv("DB_MAX_OVERFLOW", tc.maxOverflow)
			cfg := Load(&pkgconfig.Validator{})
			assert.Equal(t, tc.want, cfg.IgnoredPoolKeys)
		})
	}
}

func TestLoad_ReadsMaintenance(t *testing.T) {
	t.Setenv("MAINTENANCE_ENABLED", "true")
	v := &pkgconfig.Validator{}
	cfg := Load(v)
	assert.True(t, cfg.Maintenance)
}

func TestLoad_RequiresMaintenance(t *testing.T) {
	t.Setenv("MAINTENANCE_ENABLED", "yes")
	v := &pkgconfig.Validator{}
	Load(v)
	assert.Contains(t, strings.Join(v.Missing(), ","), "MAINTENANCE_ENABLED")
}

// run-controller reads topology artifacts from object storage, so it refuses to start
// without the bucket's endpoint, name and region.
func TestLoad_RequiresS3(t *testing.T) {
	t.Setenv("S3_ENDPOINT_URL", "")
	t.Setenv("S3_BUCKET", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	v := &pkgconfig.Validator{}
	Load(v)
	missing := strings.Join(v.Missing(), ",")
	for _, key := range []string{"S3_ENDPOINT_URL", "S3_BUCKET", "AWS_DEFAULT_REGION"} {
		assert.Contains(t, missing, key)
	}
}

// The access keys are optional: left empty, the client uses the SDK's default
// credential chain.
func TestLoad_ReadsS3(t *testing.T) {
	t.Setenv("S3_ENDPOINT_URL", "http://minio:9000")
	t.Setenv("S3_BUCKET", "continuo")
	t.Setenv("AWS_DEFAULT_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	v := &pkgconfig.Validator{}
	cfg := Load(v)
	assert.Equal(t, pkgconfig.S3Config{EndpointURL: "http://minio:9000", Bucket: "continuo", Region: "us-east-1"}, cfg.S3)
	assert.NotContains(t, strings.Join(v.Missing(), ","), "AWS_ACCESS_KEY_ID")
}

func TestLoad_TaskMaxRetries(t *testing.T) {
	cases := []struct {
		raw   string
		want  int
		valid bool
	}{
		{raw: "", want: 2, valid: true},
		{raw: "0", want: 0, valid: true},
		{raw: "10", want: 10, valid: true},
		{raw: "-1"},
		{raw: "11"},
		{raw: "two"},
	}
	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			t.Setenv("TASK_MAX_RETRIES", tc.raw)
			v := &pkgconfig.Validator{}
			cfg := Load(v)
			invalid := strings.Contains(strings.Join(v.Missing(), " "), "TASK_MAX_RETRIES")
			if tc.valid {
				assert.False(t, invalid)
				assert.Equal(t, tc.want, cfg.TaskMaxRetries)
				return
			}
			assert.True(t, invalid, "TASK_MAX_RETRIES=%q must be refused", tc.raw)
		})
	}
}

func TestLoad_TaskTimeout(t *testing.T) {
	t.Setenv("TASK_TIMEOUT_SECONDS", "")
	assert.Equal(t, 24*time.Hour, Load(&pkgconfig.Validator{}).TaskTimeout)
	t.Setenv("TASK_TIMEOUT_SECONDS", "10800")
	assert.Equal(t, 3*time.Hour, Load(&pkgconfig.Validator{}).TaskTimeout)
}

// The chart's runs.taskMaxRetries is the range run-controller accepts: a value the chart
// allows but run-controller refused would render and then stop run-controller at startup.
func TestTaskMaxRetriesRangeMatchesTheChart(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "continuo", "values.schema.json"))
	require.NoError(t, err)
	var schema struct {
		Properties struct {
			Runs struct {
				Properties struct {
					TaskMaxRetries struct {
						Minimum int `json:"minimum"`
						Maximum int `json:"maximum"`
					} `json:"taskMaxRetries"`
				} `json:"properties"`
			} `json:"runs"`
		} `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(raw, &schema))
	got := schema.Properties.Runs.Properties.TaskMaxRetries
	assert.Equal(t, MinTaskMaxRetries, got.Minimum)
	assert.Equal(t, MaxTaskMaxRetries, got.Maximum)
}
