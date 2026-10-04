package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadPostgres_records_all_missing(t *testing.T) {
	for _, key := range []string{"POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"} {
		t.Setenv(key, "")
	}
	v := &Validator{}
	LoadPostgres(v)
	if got := len(v.Missing()); got != 4 {
		t.Fatalf("want 4 missing vars, got %d: %v", got, v.Missing())
	}
}

func TestLoadPostgres_no_missing_when_all_set(t *testing.T) {
	t.Setenv("POSTGRES_HOST", "dbhost")
	t.Setenv("POSTGRES_DB", "mydb")
	t.Setenv("POSTGRES_USER", "usr")
	t.Setenv("POSTGRES_PASSWORD", "pw")
	v := &Validator{}
	cfg := LoadPostgres(v)
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing, got %v", v.Missing())
	}
	if cfg.Host != "dbhost" || cfg.DB != "mydb" || cfg.User != "usr" || cfg.Password != "pw" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
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
	env := map[string]string{"POSTGRES_HOST": "h", "POSTGRES_DB": "d", "POSTGRES_USER": "u",
		"POSTGRES_PASSWORD": "p", "DB_MAX_OPEN_CONNS": "12", "DB_MAX_IDLE_CONNS": ""}
	for k, val := range env {
		t.Setenv(k, val)
	}
	assert.Equal(t, 12, LoadPostgres(&Validator{}).Pool.MaxOpenConns)
}

func TestLoadPostgresWithDefaultDB(t *testing.T) {
	// Test: POSTGRES_DB unset → defaultDB and v.Missing() empty
	t.Run("POSTGRES_DB unset uses default", func(t *testing.T) {
		t.Setenv("POSTGRES_HOST", "h")
		t.Setenv("POSTGRES_DB", "")
		t.Setenv("POSTGRES_USER", "u")
		t.Setenv("POSTGRES_PASSWORD", "p")
		v := &Validator{}
		got := LoadPostgresWithDefaultDB(v, "default_db")
		assert.Empty(t, v.Missing())
		assert.Equal(t, "default_db", got.DB)
	})

	// Test: POSTGRES_DB set → that value
	t.Run("POSTGRES_DB set uses that value", func(t *testing.T) {
		t.Setenv("POSTGRES_HOST", "h")
		t.Setenv("POSTGRES_DB", "custom_db")
		t.Setenv("POSTGRES_USER", "u")
		t.Setenv("POSTGRES_PASSWORD", "p")
		v := &Validator{}
		got := LoadPostgresWithDefaultDB(v, "default_db")
		assert.Empty(t, v.Missing())
		assert.Equal(t, "custom_db", got.DB)
	})

	// Test: POSTGRES_HOST unset → still recorded missing
	t.Run("POSTGRES_HOST unset still recorded missing", func(t *testing.T) {
		t.Setenv("POSTGRES_HOST", "")
		t.Setenv("POSTGRES_DB", "")
		t.Setenv("POSTGRES_USER", "u")
		t.Setenv("POSTGRES_PASSWORD", "p")
		v := &Validator{}
		got := LoadPostgresWithDefaultDB(v, "default_db")
		require.Len(t, v.Missing(), 1)
		assert.Contains(t, v.Missing()[0], "POSTGRES_HOST")
		assert.Equal(t, "default_db", got.DB)
	})

	// Test: DB_MAX_OPEN_CONNS honoured (Pool carried)
	t.Run("DB_MAX_OPEN_CONNS honoured", func(t *testing.T) {
		t.Setenv("POSTGRES_HOST", "h")
		t.Setenv("POSTGRES_DB", "")
		t.Setenv("POSTGRES_USER", "u")
		t.Setenv("POSTGRES_PASSWORD", "p")
		t.Setenv("DB_MAX_OPEN_CONNS", "15")
		v := &Validator{}
		got := LoadPostgresWithDefaultDB(v, "default_db")
		assert.Empty(t, v.Missing())
		assert.Equal(t, 15, got.Pool.MaxOpenConns)
		assert.Equal(t, "default_db", got.DB)
	})
}
