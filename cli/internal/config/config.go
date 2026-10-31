// Package config resolves CLI configuration from flags, environment, and defaults.
package config

import "time"

const (
	defaultRunControllerEndpoint = "localhost:50051"
	defaultCatalogEndpoint       = "localhost:50052"
	defaultDeadLetterEndpoint    = "localhost:50055"
	defaultTimeout               = 10 * time.Second
)

// Config is the resolved configuration a command uses at runtime.
type Config struct {
	RunControllerEndpoint string
	CatalogEndpoint       string
	DeadLetterEndpoint    string
	Timeout               time.Duration
	Human                 bool
	// Actor labels who is performing an action (e.g. schedule cancel's
	// cancelled_by). Sourced from CONTINUO_ACTOR; empty means "let the server
	// record its system identity". Env-only: there is no flag for it.
	Actor string
}

// Inputs carries raw strings from flags and environment. Empty strings mean "not set".
type Inputs struct {
	FlagEndpoint           string
	FlagCatalogEndpoint    string
	FlagDeadLetterEndpoint string
	FlagTimeout            string
	FlagHuman              bool
	EnvRunControllerAddr   string
	EnvCatalogAddr         string
	EnvDeadLetterAddr      string
	EnvTimeout             string
	EnvActor               string
}

// Resolve applies precedence: flag > env > default. Invalid durations fall back silently to the default.
func Resolve(in Inputs) Config {
	cfg := Config{
		RunControllerEndpoint: defaultRunControllerEndpoint,
		CatalogEndpoint:       defaultCatalogEndpoint,
		DeadLetterEndpoint:    defaultDeadLetterEndpoint,
		Timeout:               defaultTimeout,
		Human:                 in.FlagHuman,
	}
	if in.EnvRunControllerAddr != "" {
		cfg.RunControllerEndpoint = in.EnvRunControllerAddr
	}
	if in.FlagEndpoint != "" {
		cfg.RunControllerEndpoint = in.FlagEndpoint
	}
	if in.EnvCatalogAddr != "" {
		cfg.CatalogEndpoint = in.EnvCatalogAddr
	}
	if in.FlagCatalogEndpoint != "" {
		cfg.CatalogEndpoint = in.FlagCatalogEndpoint
	}
	if in.EnvDeadLetterAddr != "" {
		cfg.DeadLetterEndpoint = in.EnvDeadLetterAddr
	}
	if in.FlagDeadLetterEndpoint != "" {
		cfg.DeadLetterEndpoint = in.FlagDeadLetterEndpoint
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
