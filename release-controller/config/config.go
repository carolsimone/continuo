package config

import (
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/warehouse"
)

// ServiceName names this service as the producer of the dead letters its
// stream consumers write.
const ServiceName = "release-controller"

// Config holds all configuration for the release-controller service.
type Config struct {
	Postgres              pkgconfig.PostgresConfig
	Redis                 pkgconfig.RedisConfig
	S3                    pkgconfig.S3Config
	HTTPPort              string
	ParseTimeout          string
	ParseHardTimeout      string
	ValidationTimeout     string
	ValidationHardTimeout string
	JanitorInterval       string
	RetentionDays         string
	RecoverStuckInterval  string
	// AgentRemediationGRPCAddr is agent-remediation's in-cluster gRPC address,
	// dialed to read a release's remediation attempts before starting a retry round.
	AgentRemediationGRPCAddr string

	// Maintenance is MAINTENANCE_ENABLED (see handlers.Deps.Maintenance).
	Maintenance bool

	// GRPCPort is the port the release.v1.ReleaseQuery gRPC server binds
	// (RELEASE_CONTROLLER_GRPC_PORT).
	GRPCPort int

	// MetricsPort is the port the Prometheus /metrics listener binds (METRICS_PORT).
	MetricsPort int

	// WarehouseEngine is WAREHOUSE_ENGINE, the install's warehouse engine
	// (postgres when unset), named in the reasons of seed reload notices.
	WarehouseEngine string

	// WarehouseFamily is the family of WarehouseEngine: what a seed's load
	// drops, and so what each release's seed reload notices predict, depends
	// on it. An engine without a family is recorded on the validator, so
	// release-controller refuses to start.
	WarehouseFamily warehouse.Family
}

// Load reads configuration from environment variables.
// v accumulates missing required vars; check v.Missing() after calling.
func Load(v *pkgconfig.Validator) Config {
	engine, family := pkgconfig.LoadWarehouse(v)
	return Config{
		Postgres:              pkgconfig.LoadPostgresWithDefaultDB(v, "continuo_release"),
		Redis:                 pkgconfig.LoadRedis(v),
		S3:                    pkgconfig.LoadS3(v),
		HTTPPort:              pkgconfig.EnvOrDefault("RELEASE_CONTROLLER_HTTP_PORT", "8088"),
		ParseTimeout:          pkgconfig.EnvOrDefault("RELEASE_PARSE_TIMEOUT", "10m"),
		ParseHardTimeout:      pkgconfig.EnvOrDefault("RELEASE_PARSE_HARD_TIMEOUT", "1h"),
		ValidationTimeout:     pkgconfig.EnvOrDefault("RELEASE_VALIDATION_TIMEOUT", "30m"),
		ValidationHardTimeout: pkgconfig.EnvOrDefault("RELEASE_VALIDATION_HARD_TIMEOUT", "2h"),
		JanitorInterval:       pkgconfig.EnvOrDefault("RELEASE_JANITOR_INTERVAL", "24h"),
		RetentionDays:         pkgconfig.EnvOrDefault("RELEASE_RETENTION_DAYS", "90"),
		RecoverStuckInterval:  pkgconfig.EnvOrDefault("RECOVER_STUCK_INTERVAL", "1m"),
		AgentRemediationGRPCAddr: pkgconfig.EnvOrDefault(
			"AGENT_REMEDIATION_GRPC_ADDR", "agent-remediation:50054"),

		Maintenance: pkgconfig.LoadMaintenance(v),

		GRPCPort: v.PortOrDefault("RELEASE_CONTROLLER_GRPC_PORT", 50056),

		MetricsPort: pkgconfig.LoadMetricsPort(v),

		WarehouseEngine: engine,
		WarehouseFamily: family,
	}
}
