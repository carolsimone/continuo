// seed-production-pointers writes a production pointer for every project of
// the live release's topology. Run it on an install whose live release lists
// projects that have no production pointer: release activation stays blocked
// until each of them has one. It rewrites the pointer of every project the
// topology lists, a project that already has one included, to name the live
// release, the image tag of the project's nodes and the manifest key --keys
// gives, so --keys names every project.
//
// The --keys flag accepts a JSON object mapping project names to their existing
// manifest S3 keys: a dbt project's manifest.json, and a python project's
// odcs-bundle.json (a python project whose key ends in anything else is
// refused), e.g.:
//
//	--keys '{"service-1":"s3://bucket/svc1/manifest.json","service-py":"s3://bucket/service-py/<release>/odcs-bundle.json"}'
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
	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
	s3adapter "github.com/carolsimone/continuo/release-controller/adapters/s3"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	var keysJSON string
	flag.StringVar(&keysJSON, "keys", "", `JSON map of project_name -> manifest S3 key (a python project's key ends in odcs-bundle.json), e.g. '{"svc":"s3://..."}'`)
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

	liveReleaseRepo := postgres.NewLiveReleaseRepository(db)
	live, err := liveReleaseRepo.Get(ctx)
	if err != nil {
		logger.Error("read the live release", "error", err)
		os.Exit(1)
	}
	if live == nil || live.ReleaseID() == "" {
		logger.Error("nothing has been promoted, so there is no live release to seed from")
		os.Exit(1)
	}

	if live.Topology().IsZero() {
		logger.Error("the live release references no topology artifact", "release_id", live.ReleaseID())
		os.Exit(1)
	}
	s3Cfg, err := loadS3Config()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	objectStore, err := objectstore.New(ctx, objectstore.ConfigFromS3(s3Cfg))
	if err != nil {
		logger.Error("connect to object storage", "error", err)
		os.Exit(1)
	}
	topo, err := s3adapter.NewTopologyArtifactStore(objectStore, s3Cfg.Bucket, 1).Load(ctx, live.Topology())
	if err != nil {
		logger.Error("load the live release's topology", "error", err)
		os.Exit(1)
	}

	pointerRepo := postgres.NewProductionPointerRepository(db)

	n, err := handlers.SeedProductionPointers(ctx, live.ReleaseID(), topo, existingKeys, pointerRepo, time.Now().UTC())
	if err != nil {
		logger.Error("seed failed", "error", err)
		os.Exit(1)
	}

	logger.Info("seed complete", "projects_seeded", n, "release_id", live.ReleaseID())
	fmt.Printf("seeded %d project(s) from release %s\n", n, live.ReleaseID())
}

// loadPostgresConfig reads the connection settings from the environment.
// POSTGRES_HOST, POSTGRES_USER and POSTGRES_PASSWORD (or
// POSTGRES_PASSWORD_FILE) are required. POSTGRES_PORT
// defaults to 5432, POSTGRES_DB to continuo_release and DB_SSLMODE to disable.
// It returns an error naming every key that is missing or holds a value it
// cannot use, so the command never connects to an endpoint nobody asked for.
func loadPostgresConfig() (pkgconfig.PostgresConfig, error) {
	v := &pkgconfig.Validator{}
	cfg := pkgconfig.PostgresConfig{
		Host:     v.Require("POSTGRES_HOST"),
		Port:     v.PortOrDefault("POSTGRES_PORT", 5432),
		User:     v.Require("POSTGRES_USER"),
		Password: v.RequireSecret("POSTGRES_PASSWORD"),
		DB:       pkgconfig.EnvOrDefault("POSTGRES_DB", "continuo_release"),
		SSLMode:  pkgconfig.EnvOrDefault("DB_SSLMODE", "disable"),
	}
	if missing := v.Missing(); len(missing) > 0 {
		return pkgconfig.PostgresConfig{}, fmt.Errorf("missing or invalid env vars: %s", strings.Join(missing, ", "))
	}
	return cfg, nil
}

// loadS3Config reads the object-storage settings the topology artifact lives
// behind: S3_ENDPOINT_URL, S3_BUCKET and AWS_DEFAULT_REGION are required;
// AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are optional (empty uses the
// IAM role). It names every required key that is missing.
func loadS3Config() (pkgconfig.S3Config, error) {
	v := &pkgconfig.Validator{}
	cfg := pkgconfig.LoadS3(v)
	if missing := v.Missing(); len(missing) > 0 {
		return pkgconfig.S3Config{}, fmt.Errorf("missing or invalid env vars: %s", strings.Join(missing, ", "))
	}
	return cfg, nil
}
