package config

import (
	"os"
	"path/filepath"
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setRequiredEnv sets every env var Load treats as required, so a test can
// isolate the value it cares about without tripping v.Missing().
func setRequiredEnv(t *testing.T) {
	t.Helper()
	// The optional Postgres keys whose environment value would change what
	// Load returns are cleared; a test that needs one sets it.
	for _, key := range []string{"POSTGRES_PORT", "DB_MAX_OPEN_CONNS", "DB_MAX_IDLE_CONNS"} {
		t.Setenv(key, "")
	}
	t.Setenv("POSTGRES_HOST", "postgres")
	t.Setenv("POSTGRES_USER", "continuo_svc")
	t.Setenv("POSTGRES_PASSWORD", "continuo")
	for _, key := range []string{"REDIS_USERNAME", "REDIS_PASSWORD_FILE"} {
		t.Setenv(key, "")
	}
	t.Setenv("REDIS_HOST", "redis")
	t.Setenv("REDIS_PORT", "6379")
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
	dsn, err := cfg.Postgres.DSN()
	require.NoError(t, err)
	assert.Contains(t, dsn, "sslmode='require'")
	assert.Equal(t, 9, cfg.Postgres.Pool.MaxOpenConns)
}

func TestLoad_DefaultsTheDatabaseName(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("POSTGRES_DB", "")

	v := &pkgconfig.Validator{}
	cfg := Load(v)

	require.Empty(t, v.Missing())
	assert.Equal(t, "continuo_remediation", cfg.Postgres.DB)
}

// Load reads the Redis password through Secret: given as REDIS_PASSWORD_FILE,
// it is read from the file, and a missing password is reported.
func TestLoad_ReadsTheRedisPasswordFile(t *testing.T) {
	setRequiredEnv(t)
	file := filepath.Join(t.TempDir(), "REDIS_PASSWORD")
	require.NoError(t, os.WriteFile(file, []byte("from-file\n"), 0o600))
	t.Setenv("REDIS_PASSWORD", "")
	t.Setenv("REDIS_PASSWORD_FILE", file)

	v := &pkgconfig.Validator{}
	cfg := Load(v)
	require.Empty(t, v.Missing())
	assert.Equal(t, "redis:6379", cfg.Redis.Addr())
	pw, err := cfg.Redis.Password.Value()
	require.NoError(t, err)
	assert.Equal(t, "from-file", pw)

	t.Setenv("REDIS_PASSWORD_FILE", "")
	v = &pkgconfig.Validator{}
	Load(v)
	assert.Contains(t, v.Missing(), "REDIS_PASSWORD")
}
