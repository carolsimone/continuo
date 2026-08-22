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
	t.Setenv("POSTGRES_DB", "continuo_agent_chat")
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

func TestLoad_LLMAPIKeyOptional(t *testing.T) {
	setBaseEnv(t)
	// An empty LLM_API_KEY must NOT be a fatal missing-config error: agent-chat
	// boots and serves gRPC, and chat sessions fail only when they call the LLM.
	// This prevents a missing key from crashlooping the pod and timing out the
	// whole helm deploy.
	t.Setenv("LLM_API_KEY", "")
	v := &pkgconfig.Validator{}
	cfg := Load(v)
	assert.Empty(t, v.Missing(), "LLM_API_KEY must be optional; got missing=%v", v.Missing())
	assert.Equal(t, "", cfg.LLMAPIKey)
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

func TestLoad_UnsetProvider_SingleMissingEntry(t *testing.T) {
	setBaseEnv(t)
	// Override the base env to leave LLM_PROVIDER unset (empty string).
	t.Setenv("LLM_PROVIDER", "")
	v := &pkgconfig.Validator{}
	Load(v)
	missing := v.Missing()
	// Count occurrences of the plain "LLM_PROVIDER" key — must be exactly one.
	count := 0
	for _, m := range missing {
		if m == "LLM_PROVIDER" {
			count++
		}
	}
	assert.Equal(t, 1, count, "LLM_PROVIDER should appear exactly once in missing list, got %v", missing)
	// The descriptive "(must be ...)" variant must NOT appear when the var is simply unset.
	assert.NotContains(t, missing, "LLM_PROVIDER (must be anthropic|openai|openai-compatible)",
		"descriptive entry must not fire for an unset provider, got %v", missing)
}
