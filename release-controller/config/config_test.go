package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setRequiredEnv sets every env var Load treats as required, so a test can
// isolate the default it cares about without tripping v.Missing().
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
	t.Setenv("MAINTENANCE_ENABLED", "false")
}

func TestLoad_AgentRemediationGRPCAddrDefault(t *testing.T) {
	setRequiredEnv(t)

	v := &pkgconfig.Validator{}
	cfg := Load(v)

	if missing := v.Missing(); len(missing) != 0 {
		t.Fatalf("unexpected missing required env vars: %v", missing)
	}
	if cfg.AgentRemediationGRPCAddr != "agent-remediation:50054" {
		t.Fatalf("want default %q, got %q", "agent-remediation:50054", cfg.AgentRemediationGRPCAddr)
	}
}

func TestLoad_AgentRemediationGRPCAddrOverride(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("AGENT_REMEDIATION_GRPC_ADDR", "agent-remediation.other-ns:9999")

	cfg := Load(&pkgconfig.Validator{})

	if cfg.AgentRemediationGRPCAddr != "agent-remediation.other-ns:9999" {
		t.Fatalf("want override %q, got %q", "agent-remediation.other-ns:9999", cfg.AgentRemediationGRPCAddr)
	}
}

func TestLoad_GRPCPort(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("RELEASE_CONTROLLER_GRPC_PORT", "")
	v := &pkgconfig.Validator{}
	assert.Equal(t, 50056, Load(v).GRPCPort)
	assert.Empty(t, v.Missing())

	t.Setenv("RELEASE_CONTROLLER_GRPC_PORT", "50999")
	assert.Equal(t, 50999, Load(&pkgconfig.Validator{}).GRPCPort)
}

func TestLoad_RefusesAnInvalidGRPCPort(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("RELEASE_CONTROLLER_GRPC_PORT", "not-a-port")
	v := &pkgconfig.Validator{}
	Load(v)
	require.Len(t, v.Missing(), 1)
	assert.Contains(t, v.Missing()[0], "RELEASE_CONTROLLER_GRPC_PORT")
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
	assert.Equal(t, "continuo_release", cfg.Postgres.DB)
}

func TestLoad_ReadsMaintenance(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MAINTENANCE_ENABLED", "true")
	v := &pkgconfig.Validator{}
	cfg := Load(v)
	assert.True(t, cfg.Maintenance)
	assert.Empty(t, v.Missing())
}

func TestLoad_RequiresMaintenance(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MAINTENANCE_ENABLED", "yes")
	v := &pkgconfig.Validator{}
	Load(v)
	assert.Contains(t, strings.Join(v.Missing(), ","), "MAINTENANCE_ENABLED")
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

// release-controller predicts seed reloads by the warehouse family of
// WAREHOUSE_ENGINE, and refuses to start on an engine it has no family for.
func TestLoad_WarehouseFamily(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("WAREHOUSE_ENGINE", "trino")
	v := &pkgconfig.Validator{}
	cfg := Load(v)
	assert.Equal(t, "trino", cfg.WarehouseEngine)
	assert.Equal(t, warehouse.LateBinding, cfg.WarehouseFamily)
	assert.NotContains(t, strings.Join(v.Missing(), ","), "WAREHOUSE_ENGINE")

	t.Setenv("WAREHOUSE_ENGINE", "")
	cfg = Load(&pkgconfig.Validator{})
	assert.Equal(t, "postgres", cfg.WarehouseEngine)
	assert.Equal(t, warehouse.SchemaBound, cfg.WarehouseFamily)

	t.Setenv("WAREHOUSE_ENGINE", "duckdb")
	v = &pkgconfig.Validator{}
	Load(v)
	assert.Contains(t, strings.Join(v.Missing(), ","), "WAREHOUSE_ENGINE (unsupported")
}
