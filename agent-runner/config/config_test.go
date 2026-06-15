package config

import (
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setBaseEnv(t *testing.T) {
	t.Setenv("POSTGRES_HOST", "localhost")
	t.Setenv("POSTGRES_PORT", "5432")
	t.Setenv("POSTGRES_DB", "continuo_agent")
	t.Setenv("POSTGRES_USER", "u")
	t.Setenv("POSTGRES_PASSWORD", "p")
	t.Setenv("LLM_PROVIDER", "anthropic")
	t.Setenv("LLM_API_KEY", "k")
	t.Setenv("LLM_MODEL", "claude-sonnet-4-6")
}

func TestLoad_DefaultsAndRequired(t *testing.T) {
	setBaseEnv(t)
	v := &pkgconfig.Validator{}
	cfg := Load(v)
	require.Empty(t, v.Missing())
	assert.Equal(t, 50053, cfg.GRPCPort)
	assert.Equal(t, 8091, cfg.HealthPort)
	assert.Equal(t, "anthropic", cfg.LLMProvider)
	assert.Equal(t, 10, cfg.MaxIterations)
	assert.Equal(t, 65536, cfg.ToolResultMaxBytes)
	assert.Equal(t, 30, cfg.RetentionDays)
	assert.False(t, cfg.RetentionArchiveS3)
}

func TestLoad_CompatibleRequiresBaseURL(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("LLM_PROVIDER", "openai-compatible")
	v := &pkgconfig.Validator{}
	Load(v)
	assert.Contains(t, v.Missing(), "LLM_BASE_URL")
}

func TestLoad_RejectsUnknownProvider(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("LLM_PROVIDER", "grok")
	v := &pkgconfig.Validator{}
	Load(v)
	assert.Contains(t, v.Missing(), "LLM_PROVIDER (must be anthropic|openai|openai-compatible)")
}
