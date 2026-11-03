package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lib/pq"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadPostgres_records_all_missing(t *testing.T) {
	setPostgresEnv(t, nil)
	v := &Validator{}
	LoadPostgres(v)
	if got := len(v.Missing()); got != 4 {
		t.Fatalf("want 4 missing vars, got %d: %v", got, v.Missing())
	}
}

func TestLoadPostgres_no_missing_when_all_set(t *testing.T) {
	setPostgresEnv(t, map[string]string{"POSTGRES_HOST": "dbhost", "POSTGRES_DB": "mydb", "POSTGRES_USER": "usr", "POSTGRES_PASSWORD": "pw"})
	v := &Validator{}
	cfg := LoadPostgres(v)
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing, got %v", v.Missing())
	}
	if cfg.Host != "dbhost" || cfg.DB != "mydb" || cfg.User != "usr" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	pw, err := cfg.Password.Value()
	require.NoError(t, err)
	assert.Equal(t, "pw", pw)
}

// envPassword returns the POSTGRES_PASSWORD Secret loaded from the
// environment variable set to value.
func envPassword(t *testing.T, value string) Secret {
	t.Helper()
	t.Setenv("POSTGRES_PASSWORD", value)
	t.Setenv("POSTGRES_PASSWORD_FILE", "")
	v := &Validator{}
	s := v.RequireSecret("POSTGRES_PASSWORD")
	require.Empty(t, v.Missing())
	return s
}

// Every value of the DSN is quoted, so a password holding a space, a quote, an
// at sign, a colon or a backslash reaches Postgres unchanged.
func TestDSNQuotesEveryValue(t *testing.T) {
	cfg := PostgresConfig{Host: "db host", Port: 5432, DB: "d", User: "u",
		Password: envPassword(t, `p w'@:\`), SSLMode: "require"}
	dsn, err := cfg.DSN()
	require.NoError(t, err)
	assert.Equal(t, `host='db host' port='5432' dbname='d' user='u' password='p w\'@:\\' sslmode='require'`, dsn)
	assert.Contains(t, dsn, `password='p w\'@:\\'`)
	assert.Equal(t, `host='db host' port='5432' dbname='d' user='u' sslmode='require'`, cfg.DSNWithoutPassword())
}

// lib/pq parses the quoted DSN back to the values it was built from.
func TestDSNRoundTripsThroughPq(t *testing.T) {
	cfg := PostgresConfig{Host: "db.example", Port: 6543, DB: "d b", User: `u'1`,
		Password: envPassword(t, `p w'@:\`)}
	dsn, err := cfg.DSN()
	require.NoError(t, err)
	parsed, err := pq.NewConfig(dsn)
	require.NoError(t, err)
	assert.Equal(t, `p w'@:\`, parsed.Password)
	assert.Equal(t, "db.example", parsed.Host)
	assert.Equal(t, uint16(6543), parsed.Port)
	assert.Equal(t, "d b", parsed.Database)
	assert.Equal(t, `u'1`, parsed.User)
	assert.Equal(t, pq.SSLMode("disable"), parsed.SSLMode)
}

// A password given as POSTGRES_PASSWORD_FILE is read again by every DSN call,
// so a rotated file reaches the next connection.
func TestDSNReadsThePasswordFileOnEveryCall(t *testing.T) {
	file := filepath.Join(t.TempDir(), "POSTGRES_PASSWORD")
	require.NoError(t, os.WriteFile(file, []byte("first\n"), 0o600))
	setPostgresEnv(t, map[string]string{"POSTGRES_HOST": "h", "POSTGRES_DB": "d", "POSTGRES_USER": "u"})
	t.Setenv("POSTGRES_PASSWORD_FILE", file)
	v := &Validator{}
	cfg := LoadPostgres(v)
	require.Empty(t, v.Missing())

	dsn, err := cfg.DSN()
	require.NoError(t, err)
	assert.Contains(t, dsn, "password='first'")

	require.NoError(t, os.WriteFile(file, []byte("second\n"), 0o600))
	dsn, err = cfg.DSN()
	require.NoError(t, err)
	assert.Contains(t, dsn, "password='second'")

	require.NoError(t, os.Remove(file))
	_, err = cfg.DSN()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "POSTGRES_PASSWORD_FILE")
}

func TestLoadPostgresPool(t *testing.T) {
	cases := []struct {
		name, open, idle string
		want             PoolConfig
		invalid          string
	}{
		{name: "unset keeps the service default", want: PoolConfig{}},
		{name: "both set", open: "25", idle: "5", want: PoolConfig{MaxOpenConns: 25, MaxIdleConns: 5}},
		{name: "only open", open: "8", want: PoolConfig{MaxOpenConns: 8}},
		{name: "zero is refused", open: "0", invalid: "DB_MAX_OPEN_CONNS"},
		{name: "negative is refused", idle: "-1", invalid: "DB_MAX_IDLE_CONNS"},
		{name: "text is refused", open: "lots", invalid: "DB_MAX_OPEN_CONNS"},
		{name: "idle above open is refused", open: "3", idle: "5", invalid: "DB_MAX_IDLE_CONNS"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DB_MAX_OPEN_CONNS", tc.open)
			t.Setenv("DB_MAX_IDLE_CONNS", tc.idle)
			v := &Validator{}
			got := LoadPostgresPool(v)
			if tc.invalid != "" {
				require.Len(t, v.Missing(), 1)
				assert.Contains(t, v.Missing()[0], tc.invalid)
				return
			}
			assert.Empty(t, v.Missing())
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestLoadPostgres_CarriesThePool(t *testing.T) {
	setPostgresEnv(t, map[string]string{"POSTGRES_HOST": "h", "POSTGRES_DB": "d", "POSTGRES_USER": "u",
		"POSTGRES_PASSWORD": "p", "DB_MAX_OPEN_CONNS": "12"})
	assert.Equal(t, 12, LoadPostgres(&Validator{}).Pool.MaxOpenConns)
}

func TestLoadPostgresWithDefaultDB(t *testing.T) {
	t.Run("POSTGRES_DB unset uses default", func(t *testing.T) {
		setPostgresEnv(t, map[string]string{"POSTGRES_HOST": "h", "POSTGRES_USER": "u", "POSTGRES_PASSWORD": "p"})
		v := &Validator{}
		got := LoadPostgresWithDefaultDB(v, "default_db")
		assert.Empty(t, v.Missing())
		assert.Equal(t, "default_db", got.DB)
	})

	t.Run("POSTGRES_DB set uses that value", func(t *testing.T) {
		setPostgresEnv(t, map[string]string{"POSTGRES_HOST": "h", "POSTGRES_DB": "custom_db", "POSTGRES_USER": "u", "POSTGRES_PASSWORD": "p"})
		v := &Validator{}
		got := LoadPostgresWithDefaultDB(v, "default_db")
		assert.Empty(t, v.Missing())
		assert.Equal(t, "custom_db", got.DB)
	})

	t.Run("POSTGRES_HOST unset still recorded missing", func(t *testing.T) {
		setPostgresEnv(t, map[string]string{"POSTGRES_USER": "u", "POSTGRES_PASSWORD": "p"})
		v := &Validator{}
		got := LoadPostgresWithDefaultDB(v, "default_db")
		require.Len(t, v.Missing(), 1)
		assert.Contains(t, v.Missing()[0], "POSTGRES_HOST")
		assert.Equal(t, "default_db", got.DB)
	})

	t.Run("DB_MAX_OPEN_CONNS honoured", func(t *testing.T) {
		setPostgresEnv(t, map[string]string{"POSTGRES_HOST": "h", "POSTGRES_USER": "u", "POSTGRES_PASSWORD": "p", "DB_MAX_OPEN_CONNS": "15"})
		v := &Validator{}
		got := LoadPostgresWithDefaultDB(v, "default_db")
		assert.Empty(t, v.Missing())
		assert.Equal(t, 15, got.Pool.MaxOpenConns)
		assert.Equal(t, "default_db", got.DB)
	})
}

// POSTGRES_PORT is optional, but a value that is not a port number stops the
// service instead of connecting to 5432.
func TestLoadPostgres_Port(t *testing.T) {
	cases := []struct {
		name, port string
		want       int
		invalid    bool
	}{
		{name: "unset defaults to 5432", want: 5432},
		{name: "set", port: "6543", want: 6543},
		{name: "text is refused", port: "543x", invalid: true},
		{name: "zero is refused", port: "0", invalid: true},
		{name: "negative is refused", port: "-1", invalid: true},
		{name: "above 65535 is refused", port: "65536", invalid: true},
	}
	for _, tc := range cases {
		for _, load := range []struct {
			name string
			fn   func(*Validator) PostgresConfig
		}{
			{"LoadPostgres", LoadPostgres},
			{"LoadPostgresWithDefaultDB", func(v *Validator) PostgresConfig { return LoadPostgresWithDefaultDB(v, "default_db") }},
		} {
			t.Run(load.name+"/"+tc.name, func(t *testing.T) {
				setPostgresEnv(t, map[string]string{"POSTGRES_HOST": "h", "POSTGRES_DB": "d", "POSTGRES_USER": "u",
					"POSTGRES_PASSWORD": "p", "POSTGRES_PORT": tc.port})
				v := &Validator{}
				got := load.fn(v)
				if tc.invalid {
					require.Len(t, v.Missing(), 1)
					assert.Contains(t, v.Missing()[0], "POSTGRES_PORT")
					return
				}
				assert.Empty(t, v.Missing())
				assert.Equal(t, tc.want, got.Port)
			})
		}
	}
}

// setPostgresEnv sets every key LoadPostgres reads, to its value in env or to
// empty, so a test does not depend on the environment it runs in.
func setPostgresEnv(t *testing.T, env map[string]string) {
	t.Helper()
	for _, key := range []string{"POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER",
		"POSTGRES_PASSWORD", "POSTGRES_PASSWORD_FILE", "DB_SSLMODE", "DB_MAX_OPEN_CONNS", "DB_MAX_IDLE_CONNS"} {
		t.Setenv(key, env[key])
	}
}
