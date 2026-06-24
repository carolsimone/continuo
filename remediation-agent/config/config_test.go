package config

import (
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setBaseEnv populates all required env vars so Validator reports no missing entries.
func setBaseEnv(t *testing.T) {
	t.Helper()
	t.Setenv("POSTGRES_HOST", "localhost")
	t.Setenv("POSTGRES_USER", "u")
	t.Setenv("POSTGRES_PASSWORD", "p")
	t.Setenv("REDIS_HOST", "localhost")
	t.Setenv("REDIS_PORT", "6379")
	t.Setenv("REDIS_PASSWORD", "r")
	t.Setenv("S3_ENDPOINT_URL", "http://localhost:9000")
	t.Setenv("S3_BUCKET", "b")
	t.Setenv("AWS_DEFAULT_REGION", "us-east-1")
	t.Setenv("LLM_PROVIDER", "anthropic")
	t.Setenv("LLM_MODEL", "claude-sonnet-4-6")
}

func TestLoad_DefaultsAndRequired(t *testing.T) {
	setBaseEnv(t)
	v := &pkgconfig.Validator{}
	cfg := Load(v)
	require.Empty(t, v.Missing())
	assert.Equal(t, "8092", cfg.HTTPPort)
	assert.Equal(t, 3, cfg.MaxAttempts)
	assert.Equal(t, "orchestrator:50052", cfg.OrchestratorAddr)
}

// TestLoad_GitHubDefaults verifies that when neither GITHUB_TOKEN nor
// GITHUB_BASE_URL is set, Load returns the documented defaults.
func TestLoad_GitHubDefaults(t *testing.T) {
	setBaseEnv(t)
	// Ensure neither variable is inherited from the test environment.
	t.Setenv("GITHUB_TOKEN", "")
	t.Setenv("GITHUB_BASE_URL", "")
	v := &pkgconfig.Validator{}
	cfg := Load(v)
	require.Empty(t, v.Missing())
	assert.Equal(t, "", cfg.GitHubToken, "GitHubToken must default to empty string")
	assert.Equal(t, "https://api.github.com", cfg.GitHubBaseURL, "GitHubBaseURL must default to the public GitHub API")
}

// TestLoad_GitHubTokenRead verifies that GITHUB_TOKEN is read from the environment.
func TestLoad_GitHubTokenRead(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("GITHUB_TOKEN", "ghp_test123")
	v := &pkgconfig.Validator{}
	cfg := Load(v)
	require.Empty(t, v.Missing())
	assert.Equal(t, "ghp_test123", cfg.GitHubToken)
}

// TestLoad_GitHubBaseURLRead verifies that GITHUB_BASE_URL overrides the default.
func TestLoad_GitHubBaseURLRead(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("GITHUB_BASE_URL", "https://github.example.com/api/v3")
	v := &pkgconfig.Validator{}
	cfg := Load(v)
	require.Empty(t, v.Missing())
	assert.Equal(t, "https://github.example.com/api/v3", cfg.GitHubBaseURL)
}
