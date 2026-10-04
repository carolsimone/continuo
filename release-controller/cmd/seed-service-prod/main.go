// seed-service-prod is a one-time migration command that populates the
// service_prod table from the existing global current_prod snapshot. Run it
// once after deploying the per-service release feature so that each service has
// a production pointer for the first incremental release cycle.
//
// The --keys flag accepts a JSON object mapping service names to their existing
// manifest S3 keys, e.g.:
//
//	--keys '{"service-1":"s3://bucket/svc1/manifest.json","service-2":"s3://bucket/svc2/manifest.json"}'
//
// Manifest keys are recorded verbatim — no S3 objects are moved or copied.
// The command is idempotent: re-running it upserts the same rows.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	var keysJSON string
	flag.StringVar(&keysJSON, "keys", "", `JSON map of service_name -> manifest S3 key, e.g. '{"svc":"s3://..."}'`)
	flag.Parse()

	if keysJSON == "" {
		logger.Error("--keys is required")
		os.Exit(1)
	}

	var existingKeys map[string]string
	if err := json.Unmarshal([]byte(keysJSON), &existingKeys); err != nil {
		logger.Error("failed to parse --keys JSON", "error", err)
		os.Exit(1)
	}

	pgCfg, err := loadPostgresConfig()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()
	db, err := pkgdb.Open(ctx, pgCfg, pkgconfig.PoolConfig{MaxOpenConns: 2, MaxIdleConns: 1})
	if err != nil {
		logger.Error("Failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	cpRepo := postgres.NewCurrentProdRepository(db)
	cp, err := cpRepo.Get(ctx)
	if err != nil {
		logger.Error("read current_prod", "error", err)
		os.Exit(1)
	}
	if cp == nil || cp.ReleaseID() == "" {
		logger.Error("current_prod is empty — nothing to seed from")
		os.Exit(1)
	}

	spRepo := postgres.NewServiceProdRepository(db)

	n, err := handlers.SeedServiceProd(ctx, cp, existingKeys, spRepo, time.Now().UTC())
	if err != nil {
		logger.Error("seed failed", "error", err)
		os.Exit(1)
	}

	logger.Info("seed complete", "services_seeded", n, "release_id", cp.ReleaseID())
	fmt.Printf("seeded %d service(s) from release %s\n", n, cp.ReleaseID())
}

// loadPostgresConfig reads the connection settings from the environment.
// POSTGRES_HOST, POSTGRES_USER and POSTGRES_PASSWORD are required. POSTGRES_PORT
// defaults to 5432, POSTGRES_DB to continuo_release and DB_SSLMODE to disable.
// It returns an error naming every key that is missing or holds a value it
// cannot use, so the command never connects to an endpoint nobody asked for.
func loadPostgresConfig() (pkgconfig.PostgresConfig, error) {
	v := &pkgconfig.Validator{}
	cfg := pkgconfig.PostgresConfig{
		Host:     v.Require("POSTGRES_HOST"),
		Port:     v.PortOrDefault("POSTGRES_PORT", 5432),
		User:     v.Require("POSTGRES_USER"),
		Password: v.Require("POSTGRES_PASSWORD"),
		DB:       pkgconfig.EnvOrDefault("POSTGRES_DB", "continuo_release"),
		SSLMode:  pkgconfig.EnvOrDefault("DB_SSLMODE", "disable"),
	}
	if missing := v.Missing(); len(missing) > 0 {
		return pkgconfig.PostgresConfig{}, fmt.Errorf("missing or invalid env vars: %s", strings.Join(missing, ", "))
	}
	return cfg, nil
}
