package config

import (
	"log/slog"
	"os"

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

	GitHubToken   string // personal access token for GitHub API calls; empty = unauthenticated
	GitHubBaseURL string // GitHub REST API base URL; defaults to the public API

	HTTPPort         string
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
		Redis:              pkgconfig.LoadRedis(v),
		S3:                 pkgconfig.LoadS3(v),
		LLMProvider:        v.Require("LLM_PROVIDER"),
		LLMAPIKey:          pkgconfig.EnvOrDefault("LLM_API_KEY", ""),
		LLMModel:           v.Require("LLM_MODEL"),
		LLMBaseURL:         pkgconfig.EnvOrDefault("LLM_BASE_URL", ""),
		GitHubToken:        pkgconfig.EnvOrDefault("GITHUB_TOKEN", ""),
		GitHubBaseURL:      pkgconfig.EnvOrDefault("GITHUB_BASE_URL", "https://api.github.com"),
		HTTPPort:           pkgconfig.EnvOrDefault("REMEDIATION_AGENT_HTTP_PORT", "8092"),
		MaxAttempts:        pkgconfig.EnvIntOrDefault("REMEDIATION_AGENT_MAX_ATTEMPTS", 3),
		OrchestratorAddr:   pkgconfig.EnvOrDefault("CONTINUO_ORCHESTRATOR_ADDR", "orchestrator:50052"),
		ServiceRepoMapPath: pkgconfig.EnvOrDefault("SERVICE_REPO_MAP_PATH", ""),
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
	data, err := os.ReadFile(path)
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
