package main

import (
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setConnectionEnv sets the required connection keys and clears the optional
// ones, so each test sets only what it checks.
func setConnectionEnv(t *testing.T) {
	t.Helper()
	t.Setenv("POSTGRES_HOST", "db.example")
	t.Setenv("POSTGRES_USER", "seed")
	t.Setenv("POSTGRES_PASSWORD", "secret")
	t.Setenv("POSTGRES_PORT", "")
	t.Setenv("POSTGRES_DB", "")
	t.Setenv("DB_SSLMODE", "")
}

func TestLoadPostgresConfig_Defaults(t *testing.T) {
	setConnectionEnv(t)
	cfg, err := loadPostgresConfig()
	require.NoError(t, err)
	assert.Equal(t, pkgconfig.PostgresConfig{
		Host: "db.example", Port: 5432, User: "seed", Password: "secret",
		DB: "continuo_release", SSLMode: "disable",
	}, cfg)
}

// A POSTGRES_PORT that is not a port fails the command before it connects,
// instead of sending its writes to the default port.
func TestLoadPostgresConfig_Port(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   string
		want    int
		wantErr bool
	}{
		{name: "unset", value: "", want: 5432},
		{name: "a port", value: "6543", want: 6543},
		{name: "not a number", value: "not-a-port", wantErr: true},
		{name: "zero", value: "0", wantErr: true},
		{name: "above the port range", value: "70000", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setConnectionEnv(t)
			t.Setenv("POSTGRES_PORT", tc.value)
			cfg, err := loadPostgresConfig()
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "POSTGRES_PORT")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, cfg.Port)
		})
	}
}

func TestLoadPostgresConfig_NamesEveryMissingKey(t *testing.T) {
	setConnectionEnv(t)
	t.Setenv("POSTGRES_HOST", "")
	t.Setenv("POSTGRES_USER", "")
	t.Setenv("POSTGRES_PASSWORD", "")
	_, err := loadPostgresConfig()
	require.Error(t, err)
	for _, key := range []string{"POSTGRES_HOST", "POSTGRES_USER", "POSTGRES_PASSWORD"} {
		assert.Contains(t, err.Error(), key)
	}
}

func TestLoadS3Config_NamesEveryMissingKey(t *testing.T) {
	t.Setenv("S3_ENDPOINT_URL", "")
	t.Setenv("S3_BUCKET", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	_, err := loadS3Config()
	require.Error(t, err)
	for _, key := range []string{"S3_ENDPOINT_URL", "S3_BUCKET", "AWS_DEFAULT_REGION"} {
		assert.Contains(t, err.Error(), key)
	}
}
