package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/contractversion"
	"github.com/carolsimone/continuo/catalog/service/ports"
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

// The environment keys of the contracts install settings, the chart's
// contracts.publish.server.* and catalog.contractHistory.* values, and the
// warehouse engine the servers entry's type derives from.
const (
	warehouseEngineKey          = "WAREHOUSE_ENGINE"
	contractsServerHostKey      = "CONTRACTS_SERVER_HOST"
	contractsServerPortKey      = "CONTRACTS_SERVER_PORT"
	contractsServerDatabaseKey  = "CONTRACTS_SERVER_DATABASE"
	contractHistoryRetentionKey = "CONTRACT_HISTORY_RETENTION_DAYS"
	ontologyBaseURLKey          = "ONTOLOGY_BASE_URL"

	// defaultWarehouseEngine is the engine an install that never chose one runs.
	defaultWarehouseEngine = "postgres"

	// maxContractHistoryRetentionDays is the largest CONTRACT_HISTORY_RETENTION_DAYS,
	// 100 years. The retention is a time.Duration of that many days, which
	// overflows from 106752 days, and pkg/retention reads a retention below 1ns
	// as its 7-day default, so a larger value would prune where "keep
	// everything" was meant.
	maxContractHistoryRetentionDays = 36500
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

	// WarehouseEngine is WAREHOUSE_ENGINE, the install's warehouse engine
	// (postgres or trino; an unset value is postgres). Catalog derives the
	// published servers entry's type from it and stops at boot on an engine it
	// cannot map.
	WarehouseEngine string

	// ContractsServer is the warehouse entry the published contracts carry in
	// their servers list, from CONTRACTS_SERVER_HOST, CONTRACTS_SERVER_PORT and
	// CONTRACTS_SERVER_DATABASE. It is nil when no host is set: the published
	// files then carry no servers entry.
	ContractsServer *contractversion.Server

	// ContractHistoryRetentionDays is CONTRACT_HISTORY_RETENTION_DAYS: how many
	// days of contract versions the retention sweeper keeps, from 0 to 36500.
	// 0 keeps every version.
	ContractHistoryRetentionDays int

	// OntologyBaseURL is ONTOLOGY_BASE_URL with one trailing slash removed: the
	// absolute http or https URL a published contract's ontology links start
	// with, in place of the `continuo-ontology:` prefix a stored contract
	// carries. It is empty when the install sets none, and the links are then
	// published as stored.
	OntologyBaseURL string
}

// Load reads configuration from environment variables.
// v accumulates missing required vars; check v.Missing() after calling.
func Load(v *pkgconfig.Validator, schedules ports.BackupScheduleParser) Config {
	engine, server := loadContractsServer(v)
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

		Backup: loadBackup(v, schedules),

		WarehouseEngine:              engine,
		ContractsServer:              server,
		ContractHistoryRetentionDays: loadContractHistoryRetentionDays(v),
		OntologyBaseURL:              loadOntologyBaseURL(v),
	}
}

// LoadNeo4j reads the Neo4j connection settings, NEO4J_URI, NEO4J_USER and
// NEO4J_PASSWORD, all required: a missing one is recorded on v.
func LoadNeo4j(v *pkgconfig.Validator) Neo4jConfig {
	return Neo4jConfig{
		URI:      v.Require("NEO4J_URI"),
		User:     v.Require("NEO4J_USER"),
		Password: v.RequireSecretValue("NEO4J_PASSWORD"),
	}
}

// loadBackup reads the backup settings. A value catalog cannot honour is
// recorded on v, so catalog refuses to start: an enabled flag that is not a
// boolean, a schedule the parser refuses, or a keep that is not a
// whole number of at least 1. The values are checked whether or not the
// backup is enabled, as the chart checks them.
func loadBackup(v *pkgconfig.Validator, schedules ports.BackupScheduleParser) BackupConfig {
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
		if _, err := schedules.Parse(raw); err != nil {
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

// loadContractsServer reads WAREHOUSE_ENGINE and the servers entry's three
// settings. An engine with no server type is recorded on v, and the entry is
// then nil. A host that is blank after trimming is no host: the entry is nil
// and the port and database are not read. With a host, a port outside 1 to
// 65535 or a database that is blank after trimming is recorded on v and the
// entry is nil, so catalog refuses to start rather than publish an entry that
// binds a contract to the wrong table. The host and database are published
// trimmed.
func loadContractsServer(v *pkgconfig.Validator) (engine string, server *contractversion.Server) {
	engine = os.Getenv(warehouseEngineKey)
	if engine == "" {
		engine = defaultWarehouseEngine
	}
	serverType, err := contractversion.ServerType(engine)
	if err != nil {
		v.Add(fmt.Sprintf("%s (unsupported %q: expected postgres or trino)", warehouseEngineKey, engine))
		return engine, nil
	}
	host := strings.TrimSpace(os.Getenv(contractsServerHostKey))
	if host == "" {
		return engine, nil
	}
	ok := true
	port, err := strconv.Atoi(os.Getenv(contractsServerPortKey))
	if err != nil || port < 1 || port > 65535 {
		v.Add(fmt.Sprintf("%s (invalid value %q: expected a whole number from 1 to 65535 when %s is set)",
			contractsServerPortKey, os.Getenv(contractsServerPortKey), contractsServerHostKey))
		ok = false
	}
	database := strings.TrimSpace(os.Getenv(contractsServerDatabaseKey))
	if database == "" {
		v.Add(fmt.Sprintf("%s (required when %s is set)", contractsServerDatabaseKey, contractsServerHostKey))
		ok = false
	}
	if !ok {
		return engine, nil
	}
	return engine, &contractversion.Server{Type: serverType, Host: host, Port: port, Database: database}
}

// loadContractHistoryRetentionDays reads CONTRACT_HISTORY_RETENTION_DAYS
// (default 0, which keeps every version). A value that is not a whole number
// from 0 to maxContractHistoryRetentionDays is recorded on v and 0 is
// returned.
func loadContractHistoryRetentionDays(v *pkgconfig.Validator) int {
	raw := os.Getenv(contractHistoryRetentionKey)
	if raw == "" {
		return 0
	}
	days, err := strconv.Atoi(raw)
	if err != nil || days < 0 || days > maxContractHistoryRetentionDays {
		v.Add(fmt.Sprintf("%s (invalid value %q: expected a whole number from 0 to %d)",
			contractHistoryRetentionKey, raw, maxContractHistoryRetentionDays))
		return 0
	}
	return days
}

// loadOntologyBaseURL reads ONTOLOGY_BASE_URL (default empty). A value that is
// not an absolute http or https URL with a host, no query and no fragment is
// recorded on v and "" is returned, so catalog refuses to start rather than
// publish links that do not open. One trailing slash is removed, so a link
// joins the URL with a single slash.
func loadOntologyBaseURL(v *pkgconfig.Validator) string {
	raw := os.Getenv(ontologyBaseURLKey)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	valid := err == nil &&
		(u.Scheme == "http" || u.Scheme == "https") &&
		u.Host != "" &&
		!strings.ContainsAny(raw, "?#")
	if !valid {
		v.Add(fmt.Sprintf("%s (invalid value %q: expected an absolute http or https URL with a host, no query and no fragment)",
			ontologyBaseURLKey, raw))
		return ""
	}
	return strings.TrimSuffix(raw, "/")
}

func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
