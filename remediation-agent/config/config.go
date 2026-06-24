package config

import (
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
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
		Redis:            pkgconfig.LoadRedis(v),
		S3:               pkgconfig.LoadS3(v),
		LLMProvider:      v.Require("LLM_PROVIDER"),
		LLMAPIKey:        pkgconfig.EnvOrDefault("LLM_API_KEY", ""),
		LLMModel:         v.Require("LLM_MODEL"),
		LLMBaseURL:       pkgconfig.EnvOrDefault("LLM_BASE_URL", ""),
		GitHubToken:      pkgconfig.EnvOrDefault("GITHUB_TOKEN", ""),
		GitHubBaseURL:    pkgconfig.EnvOrDefault("GITHUB_BASE_URL", "https://api.github.com"),
		HTTPPort:         pkgconfig.EnvOrDefault("REMEDIATION_AGENT_HTTP_PORT", "8092"),
		MaxAttempts:      pkgconfig.EnvIntOrDefault("REMEDIATION_AGENT_MAX_ATTEMPTS", 3),
		OrchestratorAddr: pkgconfig.EnvOrDefault("CONTINUO_ORCHESTRATOR_ADDR", "orchestrator:50052"),
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
	return cfg
}
