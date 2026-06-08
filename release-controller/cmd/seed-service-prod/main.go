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
	"time"

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

	db, err := postgres.NewDB(postgres.Config{
		Host:     requireEnv("POSTGRES_HOST"),
		Port:     envOrDefault("POSTGRES_PORT", "5432"),
		User:     requireEnv("POSTGRES_USER"),
		Password: requireEnv("POSTGRES_PASSWORD"),
		DB:       envOrDefault("POSTGRES_DB", "continuo_release"),
	})
	if err != nil {
		logger.Error("postgres connect", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	ctx := context.Background()

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

func requireEnv(name string) string {
	v := os.Getenv(name)
	if v == "" {
		_, _ = fmt.Fprintf(os.Stderr, "missing required env var %s\n", name)
		os.Exit(1)
	}
	return v
}

func envOrDefault(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}
