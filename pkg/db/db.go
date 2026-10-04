// Package db opens the Postgres connection pool every Go service uses.
package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

const (
	// connMaxLifetime recycles every connection, so a failover or a replaced
	// server is picked up without a restart.
	connMaxLifetime = 30 * time.Minute
	// connMaxIdleTime closes connections a burst opened once it has passed.
	connMaxIdleTime = 5 * time.Minute
)

// Open connects to the database cfg names with cfg's DSN (which carries
// DB_SSLMODE), bounds the pool by cfg.Pool over the service's defaults, and
// pings it within ctx.
func Open(ctx context.Context, cfg pkgconfig.PostgresConfig, defaults pkgconfig.PoolConfig) (*sqlx.DB, error) {
	pool, err := EffectivePool(cfg.Pool, defaults)
	if err != nil {
		return nil, err
	}
	db, err := sqlx.Open("postgres", cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	ApplyPool(db.DB, pool)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect to postgres %s:%d/%s: %w", cfg.Host, cfg.Port, cfg.DB, err)
	}
	return db, nil
}

// EffectivePool takes each limit from set when it is non-zero and from
// defaults otherwise. An idle limit taken from defaults is lowered to the open
// limit; an idle limit set above the open limit is an error.
func EffectivePool(set, defaults pkgconfig.PoolConfig) (pkgconfig.PoolConfig, error) {
	p := defaults
	if set.MaxOpenConns > 0 {
		p.MaxOpenConns = set.MaxOpenConns
	}
	if set.MaxIdleConns > 0 {
		p.MaxIdleConns = set.MaxIdleConns
	}
	if p.MaxOpenConns < 1 {
		return p, fmt.Errorf("postgres pool: no maximum of open connections")
	}
	if p.MaxIdleConns > p.MaxOpenConns {
		if set.MaxIdleConns > 0 {
			return p, fmt.Errorf("postgres pool: DB_MAX_IDLE_CONNS %d exceeds the open-connection limit %d", p.MaxIdleConns, p.MaxOpenConns)
		}
		p.MaxIdleConns = p.MaxOpenConns
	}
	return p, nil
}

// ApplyPool sets db's connection limits from p.
func ApplyPool(db *sql.DB, p pkgconfig.PoolConfig) {
	db.SetMaxOpenConns(p.MaxOpenConns)
	db.SetMaxIdleConns(p.MaxIdleConns)
	db.SetConnMaxLifetime(connMaxLifetime)
	db.SetConnMaxIdleTime(connMaxIdleTime)
}
