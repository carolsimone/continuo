//go:build integration

package neo4jinfra

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// graphTestLockPath is a fixed host-local path whose exclusive flock serialises
// every integration test binary that mutates the shared, global Neo4j :Table
// graph. All such binaries run on the same host under one `go test` invocation,
// so a file lock is a reliable cross-process mutex.
func graphTestLockPath() string {
	return filepath.Join(os.TempDir(), "continuo-orchestrator-neo4j-graph.lock")
}

// AcquireGraphTestLock takes an exclusive, cross-process advisory lock and
// returns a release function.
//
// The orchestrator's release-promotion path is a truncate-and-load over the
// entire :Table label: it retires (active=false) every :Table node outside the
// promoted topology and then deletes the unreferenced ones. A test binary
// exercising that path therefore destroys :Table fixtures created by any OTHER
// test binary running at the same time. `go test ./...` runs package test
// binaries concurrently, so the neo4j-adapter package and the handlers package
// would otherwise clobber each other's nodes ("node not found", wrong topology).
//
// Holding this lock for a package's whole test run makes the graph-mutating
// packages mutually exclusive while leaving unrelated packages parallel. The
// lock is host-local and integration-tagged, so it is compiled only into the
// `-tags integration` test binaries and never into the production binary.
func AcquireGraphTestLock() (func(), error) {
	f, err := os.OpenFile(graphTestLockPath(), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}

// ResetGraphForTest deletes every node in the shared Neo4j so a graph-mutating
// test package starts and ends on an empty graph. Call it inside the region
// guarded by AcquireGraphTestLock (before and after the package's tests) so the
// package neither inherits residue from a sibling package nor leaves any behind.
// It reads the same NEO4J_* env the test helpers use and is a no-op when Neo4j
// is unreachable (the tests skip themselves in that case).
func ResetGraphForTest() {
	uri := envOr("NEO4J_URI", "bolt://neo4j:7687")
	user := envOr("NEO4J_USER", "neo4j")
	password := envOr("NEO4J_PASSWORD", "atlas_password")

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client, err := NewNeo4jClient(uri, user, password, logger)
	if err != nil {
		return
	}
	defer func() { _ = client.Close(context.Background()) }()

	ctx := context.Background()
	session := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)
	_, _ = session.Run(ctx, `MATCH (n) DETACH DELETE n`, nil)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
