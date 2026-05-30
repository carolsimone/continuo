// Command bootstrap-current-prod is a one-time utility run at the Phase 3
// cutover. It seeds the current_prod row with the current Neo4j topology
// snapshot paired with the operator-supplied release_id.
//
// Usage:
//
//	bootstrap-current-prod \
//	  --release-id bootstrap-2026-05-28 \
//	  --topology-file /path/to/topology.json \
//	  --pg-dsn 'host=... user=... password=... dbname=continuo_release sslmode=disable'
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"time"

	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func main() {
	releaseID := flag.String("release-id", "", "release_id to seed (e.g. bootstrap-YYYY-MM-DD)")
	topoFile := flag.String("topology-file", "", "path to topology JSON (array of nodes)")
	dsn := flag.String("pg-dsn", "", "Postgres DSN for continuo_release")
	flag.Parse()

	if *releaseID == "" || *topoFile == "" || *dsn == "" {
		log.Fatalf("all of --release-id, --topology-file, --pg-dsn are required")
	}

	raw, err := os.ReadFile(*topoFile)
	if err != nil {
		log.Fatalf("read topology: %v", err)
	}

	var topo release.Topology
	if err := json.Unmarshal(raw, &topo); err != nil {
		log.Fatalf("parse topology: %v", err)
	}

	// release-controller derives the validation seed set by diffing each
	// candidate node's content_hash against this seeded snapshot. If the seed
	// topology omits content_hash, every node of the first real release will
	// differ from an empty hash and be validated (safe, but it over-validates
	// once until the first promotion rewrites the snapshot with real hashes).
	// Prefer generating this file from a manifest-controller candidate emission
	// so the seed already carries hashes.
	var missingHash int
	for _, n := range topo {
		if n.ContentHash == "" {
			missingHash++
		}
	}
	if missingHash > 0 {
		log.Printf("WARNING: %d/%d seed nodes have no content_hash — the first release will validate every node until the snapshot is rewritten on first promotion", missingHash, len(topo))
	}

	db, err := sqlx.Connect("postgres", *dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer db.Close()

	repo := postgres.NewCurrentProdRepository(db)
	cp := release.RehydrateCurrentProd(*releaseID, topo, time.Now().UTC())

	if err := repo.Upsert(context.Background(), cp); err != nil {
		log.Fatalf("upsert: %v", err)
	}

	log.Printf("bootstrap complete: current_prod = %s (%d nodes)", *releaseID, len(topo))
}
