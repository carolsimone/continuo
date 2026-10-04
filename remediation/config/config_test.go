package config

import (
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setRequiredEnv sets every env var Load treats as required, so a test can
// isolate the value it cares about without tripping v.Missing().
func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("POSTGRES_HOST", "postgres")
	t.Setenv("POSTGRES_USER", "continuo_svc")
	t.Setenv("POSTGRES_PASSWORD", "continuo")
	t.Setenv("REDIS_HOST", "redis")
	t.Setenv("REDIS_PASSWORD", "continuo")
	t.Setenv("S3_ENDPOINT_URL", "http://minio:9000")
	t.Setenv("S3_BUCKET", "continuo")
	t.Setenv("AWS_DEFAULT_REGION", "us-east-1")
}

func TestLoad_HonoursSSLModeAndPool(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("POSTGRES_DB", "continuo_x")
	t.Setenv("DB_SSLMODE", "require")
	t.Setenv("DB_MAX_OPEN_CONNS", "9")

	v := &pkgconfig.Validator{}
	cfg := Load(v)

	require.Empty(t, v.Missing())
	assert.Contains(t, cfg.Postgres.DSN(), "sslmode=require")
	assert.Equal(t, 9, cfg.Postgres.Pool.MaxOpenConns)
}

func TestLoad_DefaultsTheDatabaseName(t *testing.T) {
	setRequiredEnv(t)

	v := &pkgconfig.Validator{}
	cfg := Load(v)

	require.Empty(t, v.Missing())
	assert.Equal(t, "continuo_remediation", cfg.Postgres.DB)
}
