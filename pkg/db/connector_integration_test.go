//go:build integration

package db_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/pkg/testdeps"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The pool reads POSTGRES_PASSWORD_FILE each time it opens a connection: a
// rotated file reaches the next new connection without a restart, and a
// connection the pool already holds is not affected.
func TestOpenReadsThePasswordFileForEveryNewConnection(t *testing.T) {
	const (
		password      = "continuo"
		wrongPassword = "not-the-password"
	)
	file := filepath.Join(t.TempDir(), "POSTGRES_PASSWORD")
	writePassword := func(pw string) {
		t.Helper()
		require.NoError(t, os.WriteFile(file, []byte(pw+"\n"), 0o600))
	}
	writePassword(password)
	t.Setenv("POSTGRES_PASSWORD", "")
	t.Setenv("POSTGRES_PASSWORD_FILE", file)
	t.Setenv("DB_MAX_OPEN_CONNS", "")
	t.Setenv("DB_MAX_IDLE_CONNS", "")

	v := &pkgconfig.Validator{}
	cfg := pkgconfig.LoadPostgres(v)
	if cfg.Host == "" {
		testdeps.Unavailable(t, "POSTGRES_HOST not set; run `make test-go SERVICE=pkg`")
	}
	require.Empty(t, v.Missing())

	ctx := context.Background()
	db, err := pkgdb.Open(ctx, cfg, pkgconfig.PoolConfig{MaxOpenConns: 2, MaxIdleConns: 1})
	require.NoError(t, err)
	defer db.Close()

	var one int
	require.NoError(t, db.GetContext(ctx, &one, `SELECT 1`), "the file's password signs in")

	// held is a connection opened before the rotation; it stays open across it.
	held, err := db.Conn(ctx)
	require.NoError(t, err)
	defer held.Close()
	require.NoError(t, held.QueryRowContext(ctx, `SELECT 1`).Scan(&one))

	// Lowering the idle limit to zero closes every idle connection, so the
	// next query opens a new one with whatever the file holds then.
	writePassword(wrongPassword)
	db.SetMaxIdleConns(0)
	err = db.GetContext(ctx, &one, `SELECT 1`)
	require.Error(t, err, "a new connection signs in with the rotated file")
	assert.Contains(t, err.Error(), "password authentication failed")
	assert.NotContains(t, err.Error(), wrongPassword, "the error does not carry the password")

	require.NoError(t, held.QueryRowContext(ctx, `SELECT 1`).Scan(&one),
		"a connection opened before the rotation still answers")
	require.NoError(t, held.Close())

	writePassword(password)
	require.NoError(t, db.GetContext(ctx, &one, `SELECT 1`), "the restored file signs in again")
}
