package config

import (
	"os"
	"path/filepath"
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
	for _, key := range []string{"REDIS_ADDR", "REDIS_USERNAME", "REDIS_PASSWORD", "REDIS_PASSWORD_FILE"} {
		t.Setenv(key, "")
	}
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

// With REDIS_ADDR unset agent-chat runs its in-memory rate limiter and needs
// no Redis password.
func TestLoad_RedisOffWithoutAddr(t *testing.T) {
	setBaseEnv(t)
	v := &pkgconfig.Validator{}
	cfg := Load(v)
	require.Empty(t, v.Missing())
	assert.False(t, cfg.RedisEnabled)
}

// With REDIS_ADDR set the shared limiter's Redis password is required, in
// either form; the file form is read on every new connection.
func TestLoad_RedisReadsThePasswordFile(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("REDIS_ADDR", "redis:6379")
	file := filepath.Join(t.TempDir(), "REDIS_PASSWORD")
	require.NoError(t, os.WriteFile(file, []byte("from-file\n"), 0o600))
	t.Setenv("REDIS_PASSWORD_FILE", file)

	v := &pkgconfig.Validator{}
	cfg := Load(v)
	require.Empty(t, v.Missing())
	require.True(t, cfg.RedisEnabled)
	assert.Equal(t, "redis:6379", cfg.Redis.Addr())
	pw, err := cfg.Redis.Password.Value()
	require.NoError(t, err)
	assert.Equal(t, "from-file", pw)

	t.Setenv("REDIS_PASSWORD_FILE", "")
	v = &pkgconfig.Validator{}
	Load(v)
	assert.Contains(t, v.Missing(), "REDIS_PASSWORD")
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

// The address agent-chat hands the continuo CLI for run-controller is
// CONTINUO_RUN_CONTROLLER_ADDR, and defaults to the Service the chart and
// docker-compose both create.
func TestLoad_RunControllerAddr(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("CONTINUO_RUN_CONTROLLER_ADDR", "")
	assert.Equal(t, "run-controller:50051", Load(&pkgconfig.Validator{}).RunControllerAddr)

	t.Setenv("CONTINUO_RUN_CONTROLLER_ADDR", "run-controller.ops.svc:50051")
	assert.Equal(t, "run-controller.ops.svc:50051", Load(&pkgconfig.Validator{}).RunControllerAddr)
}

func TestLoad_DeadLetterAddr(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("CONTINUO_DEAD_LETTER_ADDR", "dead-letter-controller:50055")
	cfg := Load(&pkgconfig.Validator{})
	assert.Equal(t, "dead-letter-controller:50055", cfg.DeadLetterAddr)
}

// An unset dead-letter address stays empty so the CLI keeps its own default
// instead of receiving a blank override.
func TestLoad_DeadLetterAddrDefaultsEmpty(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("CONTINUO_DEAD_LETTER_ADDR", "")
	cfg := Load(&pkgconfig.Validator{})
	assert.Equal(t, "", cfg.DeadLetterAddr)
}

func TestLoad_ReleaseAddr(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("CONTINUO_RELEASE_CONTROLLER_ADDR", "release-controller:50056")
	cfg := Load(&pkgconfig.Validator{})
	assert.Equal(t, "release-controller:50056", cfg.ReleaseAddr)
}

// An unset release-controller address stays empty so the CLI keeps its own
// default instead of receiving a blank override.
func TestLoad_ReleaseAddrDefaultsEmpty(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("CONTINUO_RELEASE_CONTROLLER_ADDR", "")
	cfg := Load(&pkgconfig.Validator{})
	assert.Equal(t, "", cfg.ReleaseAddr)
}

func TestLoad_BaseURLForEveryProvider(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("LLM_PROVIDER", "anthropic")
	t.Setenv("LLM_BASE_URL", "https://llm-gateway.corp.example/")
	v := &pkgconfig.Validator{}
	cfg := Load(v)
	assert.Empty(t, v.Missing())
	assert.Equal(t, "https://llm-gateway.corp.example", cfg.LLMBaseURL)
}

func TestLoad_RejectsBaseURLEmptyAfterTrim(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("LLM_PROVIDER", "anthropic")
	t.Setenv("LLM_BASE_URL", "/")
	v := &pkgconfig.Validator{}
	Load(v)
	assert.Contains(t, v.Missing(), "LLM_BASE_URL (must be an http:// or https:// server root with no user:password@, ?query or #fragment, without a trailing /v1)")
}

func TestLoad_RejectsBaseURLEndingInV1(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("LLM_PROVIDER", "openai-compatible")
	t.Setenv("LLM_BASE_URL", "https://api.mistral.ai/v1")
	v := &pkgconfig.Validator{}
	Load(v)
	assert.Contains(t, v.Missing(), "LLM_BASE_URL (must be an http:// or https:// server root with no user:password@, ?query or #fragment, without a trailing /v1)")
}

func TestLLMKeyFromFile(t *testing.T) {
	setBaseEnv(t)
	keyFile := filepath.Join(t.TempDir(), "LLM_API_KEY")
	require.NoError(t, os.WriteFile(keyFile, []byte("key-from-file\n"), 0o600))
	t.Setenv("LLM_API_KEY", "")
	t.Setenv("LLM_API_KEY_FILE", keyFile)

	v := &pkgconfig.Validator{}
	cfg := Load(v)
	require.Empty(t, v.Missing())
	assert.Equal(t, "key-from-file", cfg.LLMAPIKey)
}

func TestLLMKeyGivenTwiceIsRefused(t *testing.T) {
	setBaseEnv(t)
	keyFile := filepath.Join(t.TempDir(), "LLM_API_KEY")
	require.NoError(t, os.WriteFile(keyFile, []byte("key-from-file"), 0o600))
	t.Setenv("LLM_API_KEY_FILE", keyFile)

	v := &pkgconfig.Validator{}
	Load(v)
	assert.Contains(t, v.Missing(), "LLM_API_KEY: set LLM_API_KEY or LLM_API_KEY_FILE, not both")
}
