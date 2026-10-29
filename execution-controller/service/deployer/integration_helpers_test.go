//go:build integration

package deployer_test

import (
	"log/slog"
	"os"
	"testing"

	ectest "github.com/carolsimone/continuo/execution-controller/test"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// setupPostgres connects to the externally migrated, empty execution-controller test database.
func setupPostgres(t *testing.T) (*sqlx.DB, func()) {
	return ectest.SetupPostgres(t)
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}
