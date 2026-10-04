package config

import (
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
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
	t.Setenv("REDIS_HOST", "redis")
	t.Setenv("S3_ENDPOINT_URL", "http://minio:9000")
	t.Setenv("S3_BUCKET", "continuo")
	t.Setenv("AWS_DEFAULT_REGION", "us-east-1")
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
	t.Setenv("POSTGRES_DB", "")

	v := &pkgconfig.Validator{}
	cfg := Load(v)

	require.Empty(t, v.Missing())
	assert.Equal(t, "continuo_release", cfg.Postgres.DB)
}
