package db_test

import (
	"context"
	"database/sql"
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/pkg/testdeps"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEffectivePool(t *testing.T) {
	defaults := pkgconfig.PoolConfig{MaxOpenConns: 20, MaxIdleConns: 10}
	cases := []struct {
		name    string
		set     pkgconfig.PoolConfig
		want    pkgconfig.PoolConfig
		wantErr string
	}{
		{name: "nothing set", want: defaults},
		{name: "open set", set: pkgconfig.PoolConfig{MaxOpenConns: 40}, want: pkgconfig.PoolConfig{MaxOpenConns: 40, MaxIdleConns: 10}},
		{name: "default idle clamps to a smaller open", set: pkgconfig.PoolConfig{MaxOpenConns: 4}, want: pkgconfig.PoolConfig{MaxOpenConns: 4, MaxIdleConns: 4}},
		{name: "explicit idle above open fails", set: pkgconfig.PoolConfig{MaxIdleConns: 30}, wantErr: "DB_MAX_IDLE_CONNS"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := pkgdb.EffectivePool(tc.set, defaults)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestApplyPool_SetsTheOpenLimit(t *testing.T) {
	db, err := sql.Open("postgres", "host=unused")
	require.NoError(t, err)
	defer db.Close()
	pkgdb.ApplyPool(db, pkgconfig.PoolConfig{MaxOpenConns: 7, MaxIdleConns: 2})
	assert.Equal(t, 7, db.Stats().MaxOpenConnections)
}

func TestOpen_ConnectsWithTheMergedPool(t *testing.T) {
	cfg := pkgconfig.LoadPostgres(&pkgconfig.Validator{})
	if cfg.Host == "" {
		testdeps.Unavailable(t, "POSTGRES_HOST not set; run `make test-go SERVICE=pkg`")
	}
	db, err := pkgdb.Open(context.Background(), cfg, pkgconfig.PoolConfig{MaxOpenConns: 6, MaxIdleConns: 2})
	require.NoError(t, err)
	defer db.Close()
	assert.Equal(t, 6, db.Stats().MaxOpenConnections)
}
