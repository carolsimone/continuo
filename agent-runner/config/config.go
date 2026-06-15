package config

import (
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// Config is the full agent-runner runtime configuration, read once at boot.
type Config struct {
	GRPCPort   int
	HealthPort int
	Postgres   pkgconfig.PostgresConfig

	LLMProvider string // anthropic | openai | openai-compatible
	LLMAPIKey   string
	LLMModel    string
	LLMBaseURL  string // required for openai-compatible

	CLIPath             string
	StateAddr           string
	OrchestratorAddr    string
	SystemPrompt        string
	MaxIterations       int
	MaxTurnTokens       int
	WindowTokens        int
	ConfirmTTL          time.Duration
	ToolTimeout         time.Duration
	ToolResultMaxBytes  int
	RateLimitPerMinute  int
	RetentionDays       int
	RetentionSweepEvery time.Duration
	RetentionArchiveS3  bool
	ShutdownGrace       time.Duration
}

const defaultSystemPrompt = "You answer questions about continuo schedules for an end user. " +
	"Use only the provided tools to gather facts. Reply concisely in plain language; " +
	"never show raw JSON. Before a mutating action the user will be asked to confirm; " +
	"if they deny, acknowledge and do not retry."

// Load reads configuration from env vars, recording missing/invalid required
// values on v so main can fail fast with a complete list.
func Load(v *pkgconfig.Validator) Config {
	cfg := Config{
		GRPCPort:            pkgconfig.EnvIntOrDefault("GRPC_PORT", 50053),
		HealthPort:          pkgconfig.EnvIntOrDefault("HEALTH_PORT", 8091),
		Postgres:            pkgconfig.LoadPostgres(v),
		LLMProvider:         v.Require("LLM_PROVIDER"),
		LLMAPIKey:           v.Require("LLM_API_KEY"),
		LLMModel:            v.Require("LLM_MODEL"),
		LLMBaseURL:          pkgconfig.EnvOrDefault("LLM_BASE_URL", ""),
		CLIPath:             pkgconfig.EnvOrDefault("CONTINUO_CLI_PATH", "continuo"),
		StateAddr:           pkgconfig.EnvOrDefault("CONTINUO_STATE_ADDR", "state:50051"),
		OrchestratorAddr:    pkgconfig.EnvOrDefault("CONTINUO_ORCHESTRATOR_ADDR", "orchestrator:50052"),
		SystemPrompt:        pkgconfig.EnvOrDefault("CHAT_SYSTEM_PROMPT", defaultSystemPrompt),
		MaxIterations:       pkgconfig.EnvIntOrDefault("CHAT_MAX_ITERATIONS", 10),
		MaxTurnTokens:       pkgconfig.EnvIntOrDefault("CHAT_MAX_TURN_TOKENS", 30000),
		WindowTokens:        pkgconfig.EnvIntOrDefault("CHAT_WINDOW_TOKENS", 20000),
		ConfirmTTL:          pkgconfig.EnvDurationOrDefault("CHAT_CONFIRM_TTL", 5*time.Minute),
		ToolTimeout:         pkgconfig.EnvDurationOrDefault("TOOL_TIMEOUT", 30*time.Second),
		ToolResultMaxBytes:  pkgconfig.EnvIntOrDefault("TOOL_RESULT_MAX_BYTES", 65536),
		RateLimitPerMinute:  pkgconfig.EnvIntOrDefault("CHAT_RATE_LIMIT_PER_MINUTE", 20),
		RetentionDays:       pkgconfig.EnvIntOrDefault("RETENTION_DAYS", 30),
		RetentionSweepEvery: pkgconfig.EnvDurationOrDefault("RETENTION_SWEEP_EVERY", 24*time.Hour),
		RetentionArchiveS3:  pkgconfig.EnvOrDefault("RETENTION_ARCHIVE_S3", "false") == "true",
		ShutdownGrace:       pkgconfig.EnvDurationOrDefault("SHUTDOWN_GRACE", 10*time.Second),
	}
	switch cfg.LLMProvider {
	case "anthropic", "openai":
		// valid, no additional requirements
	case "openai-compatible":
		if cfg.LLMBaseURL == "" {
			v.Add("LLM_BASE_URL")
		}
	default:
		v.Add("LLM_PROVIDER (must be anthropic|openai|openai-compatible)")
	}
	return cfg
}
