package config

import (
	"log/slog"
	"os"
	"path/filepath"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"gopkg.in/yaml.v3"
)

// Config is the full remediation-agent runtime configuration, read once at boot.
type Config struct {
	Postgres pkgconfig.PostgresConfig
	Redis    pkgconfig.RedisConfig
	S3       pkgconfig.S3Config

	LLMProvider string // anthropic | openai | openai-compatible
	LLMAPIKey   string
	LLMModel    string
	LLMBaseURL  string // required for openai-compatible

	// LLMCacheTTL is how long a cached LLM propose result stays valid. The cache
	// only needs to survive the same-trigger redelivery window (a Redis PEL sweep
	// or outbox re-emit, seconds to minutes), so a short TTL both covers that and
	// bounds memory: the shared Redis runs noeviction and co-hosts the event
	// streams and OIDC sessions, so the cache must self-bound via its TTL rather
	// than rely on eviction. Defaults to 1h; a non-positive value falls back to
	// that default so a misconfigured TTL can never disable expiry.
	LLMCacheTTL time.Duration

	GitHubToken   string // personal access token for GitHub API calls; empty = unauthenticated
	GitHubBaseURL string // GitHub REST API base URL; defaults to the public API

	// PRPollInterval is how often the PR-outcome reconciler polls GitHub for
	// proposals with an open PR. Non-positive values fall back to the default
	// so a misconfigured interval can never produce a hot loop.
	PRPollInterval time.Duration

	// PROpeningGracePeriod bounds how long a proposal can sit claimed for PR
	// creation (pr_state='opening') with no matching pull request on GitHub
	// before the reconciler's opening sweep releases it back to 'failed' for
	// retry. Non-positive values fall back to the default.
	PROpeningGracePeriod time.Duration

	HTTPPort         string
	GRPCPort         string
	MaxAttempts      int
	OrchestratorAddr string

	// ServiceRepoMapPath is the path to the service→repo YAML file. When empty
	// or missing, ServiceRepoPaths is an empty map and Step-2 source resolution
	// degrades gracefully for all services.
	ServiceRepoMapPath string
	// ServiceRepoPaths maps a dbt service_name to its project root within the
	// source repo. Loaded from ServiceRepoMapPath at startup.
	ServiceRepoPaths map[string]string
}

// serviceReposFile is the on-disk structure of config/service_repos.yaml.
type serviceReposFile struct {
	Services map[string]string `yaml:"services"`
}

// defaultLLMCacheTTL bounds how long a cached LLM propose result lives. It only
// needs to cover the same-trigger redelivery window, and it also caps the memory
// the cache adds to the shared, noeviction Redis instance.
const defaultLLMCacheTTL = time.Hour

// defaultPRPollInterval paces the PR-outcome reconciler; one minute keeps the
// proposal table at most one poll behind GitHub without pressuring rate limits.
const defaultPRPollInterval = time.Minute

// defaultPROpeningGracePeriod bounds how long the opening sweep waits before
// releasing a stuck 'opening' claim with no matching GitHub PR. Ten minutes
// comfortably exceeds the seconds-scale S3-fetch-then-create-PR round trip a
// healthy claim completes in, so the sweep never races an in-flight creation.
const defaultPROpeningGracePeriod = 10 * time.Minute

// Load reads configuration from env vars, recording missing/invalid required
// values on v so main can fail fast with a complete list.
func Load(v *pkgconfig.Validator) Config {
	cfg := Config{
		Postgres: pkgconfig.PostgresConfig{
			Host:     v.Require("POSTGRES_HOST"),
			Port:     pkgconfig.EnvIntOrDefault("POSTGRES_PORT", 5432),
			DB:       pkgconfig.EnvOrDefault("POSTGRES_DB", "continuo_remediation_agent"),
			User:     v.Require("POSTGRES_USER"),
			Password: v.Require("POSTGRES_PASSWORD"),
			SSLMode:  pkgconfig.EnvOrDefault("DB_SSLMODE", "disable"),
		},
		Redis:                pkgconfig.LoadRedis(v),
		S3:                   pkgconfig.LoadS3(v),
		LLMProvider:          v.Require("LLM_PROVIDER"),
		LLMAPIKey:            pkgconfig.EnvOrDefault("LLM_API_KEY", ""),
		LLMModel:             v.Require("LLM_MODEL"),
		LLMBaseURL:           pkgconfig.EnvOrDefault("LLM_BASE_URL", ""),
		LLMCacheTTL:          pkgconfig.EnvDurationOrDefault("LLM_CACHE_TTL", defaultLLMCacheTTL),
		GitHubToken:          pkgconfig.EnvOrDefault("GITHUB_TOKEN", ""),
		GitHubBaseURL:        pkgconfig.EnvOrDefault("GITHUB_BASE_URL", "https://api.github.com"),
		PRPollInterval:       pkgconfig.EnvDurationOrDefault("REMEDIATION_PR_POLL_INTERVAL", defaultPRPollInterval),
		PROpeningGracePeriod: pkgconfig.EnvDurationOrDefault("REMEDIATION_PR_OPENING_GRACE_PERIOD", defaultPROpeningGracePeriod),
		HTTPPort:             pkgconfig.EnvOrDefault("REMEDIATION_AGENT_HTTP_PORT", "8092"),
		GRPCPort:             pkgconfig.EnvOrDefault("REMEDIATION_AGENT_GRPC_PORT", "50054"),
		MaxAttempts:          pkgconfig.EnvIntOrDefault("REMEDIATION_AGENT_MAX_ATTEMPTS", 3),
		OrchestratorAddr:     pkgconfig.EnvOrDefault("CONTINUO_ORCHESTRATOR_ADDR", "orchestrator:50052"),
		ServiceRepoMapPath:   pkgconfig.EnvOrDefault("SERVICE_REPO_MAP_PATH", ""),
	}
	switch cfg.LLMProvider {
	case "anthropic", "openai":
		// valid, no additional requirements
	case "openai-compatible":
		if cfg.LLMBaseURL == "" {
			v.Add("LLM_BASE_URL")
		}
	default:
		if cfg.LLMProvider != "" {
			v.Add("LLM_PROVIDER (must be anthropic|openai|openai-compatible)")
		}
	}
	// A non-positive TTL would either disable expiry (Redis treats a zero SET
	// expiration as "never expire") or make every write error; either defeats the
	// cache's memory self-bounding, so clamp it back to the default.
	if cfg.LLMCacheTTL <= 0 {
		cfg.LLMCacheTTL = defaultLLMCacheTTL
	}
	if cfg.PRPollInterval <= 0 {
		cfg.PRPollInterval = defaultPRPollInterval
	}
	if cfg.PROpeningGracePeriod <= 0 {
		cfg.PROpeningGracePeriod = defaultPROpeningGracePeriod
	}
	cfg.ServiceRepoPaths = loadServiceRepos(cfg.ServiceRepoMapPath)
	return cfg
}

// loadServiceRepos reads the service→repo mapping from path. When path is
// empty or the file does not exist, it returns an empty map so the caller
// degrades gracefully. Parse errors are logged and also produce an empty map.
func loadServiceRepos(path string) map[string]string {
	if path == "" {
		slog.Info("SERVICE_REPO_MAP_PATH not set; service→repo mapping disabled")
		return map[string]string{}
	}
	// path is an operator-supplied deploy-time setting (SERVICE_REPO_MAP_PATH env
	// var), never derived from a request or other runtime input; Clean it before
	// reading purely to normalize traversal segments defensively.
	data, err := os.ReadFile(filepath.Clean(path)) //nolint:gosec // G304: trusted operator config path, not user input.
	if err != nil {
		if os.IsNotExist(err) {
			slog.Warn("service repo map file not found; service→repo mapping disabled", "path", path)
		} else {
			slog.Warn("failed to read service repo map; service→repo mapping disabled", "path", path, "error", err)
		}
		return map[string]string{}
	}
	var f serviceReposFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		slog.Warn("failed to parse service repo map; service→repo mapping disabled", "path", path, "error", err)
		return map[string]string{}
	}
	if f.Services == nil {
		return map[string]string{}
	}
	return f.Services
}
