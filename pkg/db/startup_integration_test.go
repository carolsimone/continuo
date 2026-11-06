//go:build integration

package db_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/pkg/startupwait"
	"github.com/carolsimone/continuo/pkg/testdeps"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/lib/pq/pqerror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var startupTestPool = pkgconfig.PoolConfig{MaxOpenConns: 2, MaxIdleConns: 1}

// warningCounter is a slog handler that counts the warnings it receives.
type warningCounter struct{ warnings atomic.Int32 }

func (c *warningCounter) Enabled(context.Context, slog.Level) bool { return true }
func (c *warningCounter) WithAttrs([]slog.Attr) slog.Handler       { return c }
func (c *warningCounter) WithGroup(string) slog.Handler            { return c }

func (c *warningCounter) Handle(_ context.Context, r slog.Record) error {
	if r.Level == slog.LevelWarn {
		c.warnings.Add(1)
	}
	return nil
}

func loadStartupTestPostgres(t *testing.T) pkgconfig.PostgresConfig {
	t.Helper()
	t.Setenv("DB_MAX_OPEN_CONNS", "")
	t.Setenv("DB_MAX_IDLE_CONNS", "")
	cfg := pkgconfig.LoadPostgres(&pkgconfig.Validator{})
	if cfg.Host == "" {
		testdeps.Unavailable(t, "POSTGRES_HOST not set; run `make test-go SERVICE=pkg`")
	}
	return cfg
}

// forward listens on addr and copies every connection to and from upstream until
// the test ends.
func forward(t *testing.T, addr, upstream string) {
	t.Helper()
	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err, "take the port the service is waiting on")
	t.Cleanup(func() { _ = lis.Close() })
	go func() {
		for {
			client, err := lis.Accept()
			if err != nil {
				return
			}
			server, err := net.Dial("tcp", upstream)
			if err != nil {
				_ = client.Close()
				continue
			}
			go func() {
				_, _ = io.Copy(server, client)
				_ = server.Close()
			}()
			go func() {
				_, _ = io.Copy(client, server)
				_ = client.Close()
			}()
		}
	}()
}

// Connect keeps waiting while every connection is refused, as a new pod's are
// until the NetworkPolicy in front of Postgres admits its IP, and returns a
// working pool once the real server can be reached.
func TestConnect_WaitsForARealPostgresThatBecomesReachable(t *testing.T) {
	cfg := loadStartupTestPostgres(t)
	upstream := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))

	free, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := free.Addr().(*net.TCPAddr).Port
	require.NoError(t, free.Close())
	waiting := cfg
	waiting.Host = "127.0.0.1"
	waiting.Port = port

	logs := &warningCounter{}
	type connected struct {
		db  *sqlx.DB
		err error
	}
	done := make(chan connected, 1)
	policy := startupwait.Policy{Timeout: time.Minute, InitialDelay: 50 * time.Millisecond, MaxDelay: 200 * time.Millisecond}
	go func() {
		db, err := pkgdb.Connect(context.Background(), waiting, startupTestPool, policy, slog.New(logs))
		done <- connected{db: db, err: err}
	}()

	require.Eventually(t, func() bool { return logs.warnings.Load() >= 2 }, 30*time.Second, 10*time.Millisecond,
		"Connect must log a warning for each refused attempt and keep waiting")
	select {
	case got := <-done:
		t.Fatalf("Connect returned while Postgres was unreachable: %v", got.err)
	default:
	}

	forward(t, net.JoinHostPort(waiting.Host, strconv.Itoa(port)), upstream)

	select {
	case got := <-done:
		require.NoError(t, got.err)
		t.Cleanup(func() { _ = got.db.Close() })
		var database string
		require.NoError(t, got.db.GetContext(context.Background(), &database, `SELECT current_database()`))
		assert.Equal(t, cfg.DB, database)
	case <-time.After(30 * time.Second):
		t.Fatal("Connect did not return after Postgres became reachable")
	}
}

// An answer from the real server that waiting cannot change fails on the first
// attempt: no retry warning, and nowhere near the budget.
func TestConnect_FailsAtOnceOnARealServersRejection(t *testing.T) {
	cases := []struct {
		name  string
		env   string
		value string
		codes []pqerror.Code
	}{
		{name: "wrong password", env: "POSTGRES_PASSWORD", value: "not-the-password", codes: []pqerror.Code{pqerror.InvalidPassword}},
		{name: "missing database", env: "POSTGRES_DB", value: "continuo_no_such_database", codes: []pqerror.Code{pqerror.InvalidCatalogName}},
		// Password authentication reports a missing role as a failed password, so
		// it cannot reveal which roles exist; other methods report 28000.
		{name: "missing role", env: "POSTGRES_USER", value: "continuo_no_such_role", codes: []pqerror.Code{pqerror.InvalidPassword, pqerror.InvalidAuthorizationSpecification}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.env, tc.value)
			if tc.env == "POSTGRES_PASSWORD" {
				t.Setenv("POSTGRES_PASSWORD_FILE", "")
			}
			cfg := loadStartupTestPostgres(t)

			logs := &warningCounter{}
			policy := startupwait.Policy{Timeout: time.Minute, InitialDelay: 50 * time.Millisecond, MaxDelay: 200 * time.Millisecond}
			start := time.Now()
			db, err := pkgdb.Connect(context.Background(), cfg, startupTestPool, policy, slog.New(logs))

			require.Error(t, err)
			assert.Nil(t, db)
			assert.NotNil(t, pq.As(err, tc.codes...), "want one of %v, got %v", tc.codes, err)
			assert.Zero(t, logs.warnings.Load(), "a rejection must not be retried")
			assert.Less(t, time.Since(start), 30*time.Second)
		})
	}
}
