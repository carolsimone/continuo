// Package config loads dead-letter-controller's configuration from the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

// ServiceName names this service as the producer of the dead letters its
// stream consumers and outbox write.
const ServiceName = "dead-letter-controller"

const defaultShutdownGrace = 15 * time.Second

const (
	// defaultTrimEnabled turns the stream trim loop on when STREAM_TRIM_ENABLED is unset.
	defaultTrimEnabled = true
	// defaultStreamRetention is how long a stream entry that a lagging consumer
	// group still needs is kept before it is quarantined, when STREAM_RETENTION is unset.
	defaultStreamRetention = 72 * time.Hour
)

// Config is dead-letter-controller's configuration.
type Config struct {
	Redis         pkgconfig.RedisConfig
	Postgres      pkgconfig.PostgresConfig
	GRPCPort      int
	HTTPPort      string
	MetricsPort   int
	ShutdownGrace time.Duration

	// Maintenance is MAINTENANCE_ENABLED: while true, RedriveDeadLetters is
	// refused. Listing, showing and the stream trim loop keep working.
	Maintenance bool

	// TrimEnabled turns the stream trim loop on (STREAM_TRIM_ENABLED).
	TrimEnabled bool
	// StreamRetention is the age past which a stream entry a lagging group
	// still needs is quarantined as a dead letter and then trimmed (STREAM_RETENTION).
	StreamRetention time.Duration
	// ConfigErr is the first trim setting that could not be parsed. Validate
	// returns it, so the service refuses to start rather than fall back to a default.
	ConfigErr error
}

// Load reads the configuration; v accumulates missing required variables. A
// trim setting that is set but unparseable is recorded in Config.ConfigErr.
func Load(v *pkgconfig.Validator) Config {
	trimEnabled, enabledErr := parseBool("STREAM_TRIM_ENABLED", defaultTrimEnabled)
	retention, retentionErr := parseDuration("STREAM_RETENTION", defaultStreamRetention)
	cfgErr := enabledErr
	if cfgErr == nil {
		cfgErr = retentionErr
	}
	return Config{
		Redis:           pkgconfig.LoadRedis(v),
		Postgres:        pkgconfig.LoadPostgres(v),
		GRPCPort:        pkgconfig.EnvIntOrDefault("DEAD_LETTER_GRPC_PORT", 50055),
		HTTPPort:        pkgconfig.EnvOrDefault("DEAD_LETTER_HTTP_PORT", "8096"),
		MetricsPort:     pkgconfig.LoadMetricsPort(v),
		ShutdownGrace:   pkgconfig.EnvDurationOrDefault("SHUTDOWN_GRACE", defaultShutdownGrace),
		Maintenance:     pkgconfig.LoadMaintenance(v),
		TrimEnabled:     trimEnabled,
		StreamRetention: retention,
		ConfigErr:       cfgErr,
	}
}

// Validate reports a setting the service cannot honour: an unparseable trim
// setting, or a retention outside (0, model.ReplayHorizon).
func (c Config) Validate() error {
	if c.ConfigErr != nil {
		return c.ConfigErr
	}
	if c.StreamRetention <= 0 || c.StreamRetention >= model.ReplayHorizon {
		return fmt.Errorf("STREAM_RETENTION must be in (0, %dh): an entry quarantined at the replay horizon is already past its redrive window", int(model.ReplayHorizon.Hours()))
	}
	return nil
}

// RequireTrimPoolCapacity rejects a pool too small for the trim loop. The loop
// holds one pooled connection for its advisory lock for the whole run, and
// quarantining needs a second one; with a single connection the insert would
// wait forever for the connection the lock holds.
func RequireTrimPoolCapacity(trimEnabled bool, maxOpenConns int) error {
	if trimEnabled && maxOpenConns < 2 {
		return fmt.Errorf("STREAM_TRIM_ENABLED requires DB_MAX_OPEN_CONNS >= 2 (got %d): the trim loop holds one connection for its advisory lock while quarantining", maxOpenConns)
	}
	return nil
}

func parseBool(key string, fallback bool) (bool, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	b, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback, fmt.Errorf("%s: invalid boolean %q", key, raw)
	}
	return b, nil
}

func parseDuration(key string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return fallback, fmt.Errorf("%s: invalid duration %q", key, raw)
	}
	return d, nil
}
