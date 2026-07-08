// Package config resolves CLI configuration from flags, environment, and defaults.
package config

import "time"

const (
	defaultStateEndpoint        = "localhost:50051"
	defaultOrchestratorEndpoint = "localhost:50052"
	defaultTimeout              = 10 * time.Second
)

// Config is the resolved configuration a command uses at runtime.
type Config struct {
	StateEndpoint        string
	OrchestratorEndpoint string
	Timeout              time.Duration
	Human                bool
	// Actor labels who is performing an action (e.g. schedule cancel's
	// cancelled_by). Sourced from CONTINUO_ACTOR; empty means "let the server
	// record its system identity". Env-only: there is no flag for it.
	Actor string
}

// Inputs carries raw strings from flags and environment. Empty strings mean "not set".
type Inputs struct {
	FlagEndpoint             string
	FlagOrchestratorEndpoint string
	FlagTimeout              string
	FlagHuman                bool
	EnvStateAddr             string
	EnvOrchestratorAddr      string
	EnvTimeout               string
	EnvActor                 string
}

// Resolve applies precedence: flag > env > default. Invalid durations fall back silently to the default.
func Resolve(in Inputs) Config {
	cfg := Config{
		StateEndpoint:        defaultStateEndpoint,
		OrchestratorEndpoint: defaultOrchestratorEndpoint,
		Timeout:              defaultTimeout,
		Human:                in.FlagHuman,
	}
	if in.EnvStateAddr != "" {
		cfg.StateEndpoint = in.EnvStateAddr
	}
	if in.FlagEndpoint != "" {
		cfg.StateEndpoint = in.FlagEndpoint
	}
	if in.EnvOrchestratorAddr != "" {
		cfg.OrchestratorEndpoint = in.EnvOrchestratorAddr
	}
	if in.FlagOrchestratorEndpoint != "" {
		cfg.OrchestratorEndpoint = in.FlagOrchestratorEndpoint
	}
	if d, err := time.ParseDuration(in.EnvTimeout); err == nil {
		cfg.Timeout = d
	}
	if d, err := time.ParseDuration(in.FlagTimeout); err == nil {
		cfg.Timeout = d
	}
	cfg.Actor = in.EnvActor
	return cfg
}
