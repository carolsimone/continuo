package config

import (
	"os"
	"testing"
	"time"

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
	assert.Equal(t, time.Hour, cfg.LLMCacheTTL, "LLM_CACHE_TTL must default to 1h so no new required env var is introduced")
}

// TestLoad_LLMCacheTTLOverride verifies LLM_CACHE_TTL overrides the default.
func TestLoad_LLMCacheTTLOverride(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("LLM_CACHE_TTL", "1h30m")
	v := &pkgconfig.Validator{}
	cfg := Load(v)
	require.Empty(t, v.Missing())
	assert.Equal(t, 90*time.Minute, cfg.LLMCacheTTL)
}

// TestLoad_LLMCacheTTLNonPositiveFallsBackToDefault verifies that a zero or
// negative TTL is clamped back to the default. A zero SET expiration would make
// entries permanent, and a negative one would make every write error; either
// defeats the cache's memory self-bounding on the shared noeviction Redis.
func TestLoad_LLMCacheTTLNonPositiveFallsBackToDefault(t *testing.T) {
	for _, val := range []string{"0s", "-1s"} {
		t.Run(val, func(t *testing.T) {
			setBaseEnv(t)
			t.Setenv("LLM_CACHE_TTL", val)
			v := &pkgconfig.Validator{}
			cfg := Load(v)
			require.Empty(t, v.Missing())
			assert.Equal(t, time.Hour, cfg.LLMCacheTTL)
		})
	}
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

// TestLoad_ServiceRepoMapPath_Empty verifies that when SERVICE_REPO_MAP_PATH
// is unset, ServiceRepoPaths is an empty (non-nil) map and no error is raised.
func TestLoad_ServiceRepoMapPath_Empty(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("SERVICE_REPO_MAP_PATH", "")
	v := &pkgconfig.Validator{}
	cfg := Load(v)
	require.Empty(t, v.Missing())
	assert.NotNil(t, cfg.ServiceRepoPaths)
	assert.Empty(t, cfg.ServiceRepoPaths)
}

// TestLoad_ServiceRepoMapPath_LoadsYAML verifies that when SERVICE_REPO_MAP_PATH
// points to a valid YAML file, ServiceRepoPaths is populated correctly.
func TestLoad_ServiceRepoMapPath_LoadsYAML(t *testing.T) {
	setBaseEnv(t)

	tmp := t.TempDir()
	yamlPath := tmp + "/service_repos.yaml"
	content := `services:
  svc-a: services/svc-a
  svc-b: services/svc-b
`
	require.NoError(t, os.WriteFile(yamlPath, []byte(content), 0o644))
	t.Setenv("SERVICE_REPO_MAP_PATH", yamlPath)

	v := &pkgconfig.Validator{}
	cfg := Load(v)
	require.Empty(t, v.Missing())
	assert.Equal(t, "services/svc-a", cfg.ServiceRepoPaths["svc-a"])
	assert.Equal(t, "services/svc-b", cfg.ServiceRepoPaths["svc-b"])
}

// TestLoad_ServiceRepoMapPath_MissingFile verifies that a non-existent file
// produces an empty map without causing a startup failure.
func TestLoad_ServiceRepoMapPath_MissingFile(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("SERVICE_REPO_MAP_PATH", "/nonexistent/path/service_repos.yaml")
	v := &pkgconfig.Validator{}
	cfg := Load(v)
	require.Empty(t, v.Missing())
	assert.NotNil(t, cfg.ServiceRepoPaths)
	assert.Empty(t, cfg.ServiceRepoPaths)
}
