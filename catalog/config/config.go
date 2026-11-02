package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	croninfra "github.com/carolsimone/continuo/catalog/adapters/cron"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// ServiceName names this service as the producer of the dead letters its
// stream consumers write.
const ServiceName = "catalog"

// defaultShutdownGrace bounds the graceful-shutdown sequence: the in-flight
// drain plus the infra-close handlers. It is a safe default so no required env
// var is introduced; override with SHUTDOWN_GRACE (e.g. "30s").
const defaultShutdownGrace = 15 * time.Second

// Neo4jConfig holds Neo4j connection parameters.
type Neo4jConfig struct {
	URI      string
	User     string
	Password string
}

// BackupConfig is the nightly export of each tenant's graph to object
// storage, from the chart's catalog.backup.* values.
type BackupConfig struct {
	// Enabled runs the export on Schedule (CATALOG_BACKUP_ENABLED, default
	// true).
	Enabled bool
	// Schedule is when the export runs, in UTC: five cron fields or an @
	// descriptor robfig/cron's standard parser reads, with no timezone
	// prefix (CATALOG_BACKUP_SCHEDULE, default "0 2 * * *").
	Schedule string
	// Keep is how many of each tenant's newest exports are kept, at least 1
	// (CATALOG_BACKUP_KEEP, default 7).
	Keep int
}

// The environment keys of the backup and their defaults, the chart's.
const (
	backupEnabledKey  = "CATALOG_BACKUP_ENABLED"
	backupScheduleKey = "CATALOG_BACKUP_SCHEDULE"
	backupKeepKey     = "CATALOG_BACKUP_KEEP"

	defaultBackupSchedule = "0 2 * * *"
	defaultBackupKeep     = 7
)

// Config holds all configuration for the catalog service.
type Config struct {
	Redis pkgconfig.RedisConfig
	Neo4j Neo4jConfig

	// S3 — the version-ingestion consumer reads each release's code-bundle
	// document from object storage. Required: a catalog that boots without
	// it would silently stop recording code history.
	S3 pkgconfig.S3Config

	// gRPC
	GRPCPort int
	HTTPPort int

	// RETENTION_DAYS and RETENTION_SWEEP_INTERVAL_MINUTES: how long the marker
	// sweeper keeps a processed-message marker, and how often it runs. A marker
	// is kept at least the replay horizon, so a RetentionDays below it has no
	// effect. Both knobs have safe defaults so no configuration is required.
	RetentionDays             int
	RetentionSweepIntervalMin int

	// ShutdownGrace bounds the graceful-shutdown drain + infra teardown.
	ShutdownGrace time.Duration

	// MetricsPort is the port the Prometheus /metrics listener binds (METRICS_PORT).
	MetricsPort int

	// Backup is the nightly export of each tenant's graph.
	Backup BackupConfig
}

// Load reads configuration from environment variables.
// v accumulates missing required vars; check v.Missing() after calling.
func Load(v *pkgconfig.Validator) Config {
	return Config{
		Redis: pkgconfig.LoadRedis(v),
		Neo4j: LoadNeo4j(v),
		S3:    pkgconfig.LoadS3(v),

		GRPCPort: envInt("GRPC_PORT", 50052),
		HTTPPort: envInt("HTTP_PORT", 8087),

		RetentionDays:             envInt("RETENTION_DAYS", 7),
		RetentionSweepIntervalMin: envInt("RETENTION_SWEEP_INTERVAL_MINUTES", 60),

		ShutdownGrace: pkgconfig.EnvDurationOrDefault("SHUTDOWN_GRACE", defaultShutdownGrace),

		MetricsPort: pkgconfig.LoadMetricsPort(v),

		Backup: loadBackup(v),
	}
}

// LoadNeo4j reads the Neo4j connection settings, NEO4J_URI, NEO4J_USER and
// NEO4J_PASSWORD, all required: a missing one is recorded on v.
func LoadNeo4j(v *pkgconfig.Validator) Neo4jConfig {
	return Neo4jConfig{
		URI:      v.Require("NEO4J_URI"),
		User:     v.Require("NEO4J_USER"),
		Password: v.Require("NEO4J_PASSWORD"),
	}
}

// loadBackup reads the backup settings. A value catalog cannot honour is
// recorded on v, so catalog refuses to start: an enabled flag that is not a
// boolean, a schedule checkBackupSchedule refuses, or a keep that is not a
// whole number of at least 1. The values are checked whether or not the
// backup is enabled, as the chart checks them.
func loadBackup(v *pkgconfig.Validator) BackupConfig {
	b := BackupConfig{Enabled: true, Schedule: defaultBackupSchedule, Keep: LoadBackupKeep(v)}
	if raw := os.Getenv(backupEnabledKey); raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			v.Add(fmt.Sprintf("%s (invalid value %q: expected true or false)", backupEnabledKey, raw))
		} else {
			b.Enabled = enabled
		}
	}
	if raw := os.Getenv(backupScheduleKey); raw != "" {
		if err := checkBackupSchedule(raw); err != nil {
			v.Add(fmt.Sprintf("%s (invalid value %q: %v)", backupScheduleKey, raw, err))
		} else {
			b.Schedule = raw
		}
	}
	return b
}

// LoadBackupKeep reads how many of each tenant's newest exports are kept,
// CATALOG_BACKUP_KEEP (default 7). A value that is not a whole number of at
// least 1 is recorded on v and the default is returned. An export run by hand
// prunes with the same keep as the scheduled one.
func LoadBackupKeep(v *pkgconfig.Validator) int {
	raw := os.Getenv(backupKeepKey)
	if raw == "" {
		return defaultBackupKeep
	}
	keep, err := strconv.Atoi(raw)
	if err != nil || keep < 1 {
		v.Add(fmt.Sprintf("%s (invalid value %q: expected a whole number of at least 1)", backupKeepKey, raw))
		return defaultBackupKeep
	}
	return keep
}

// checkBackupSchedule reports why spec cannot be a backup schedule, or nil.
func checkBackupSchedule(spec string) error {
	_, err := croninfra.BackupScheduleParser{}.Parse(spec)
	return err
}

func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
