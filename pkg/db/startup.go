package db

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"net"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/startupwait"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/lib/pq/pqerror"
)

// attemptTimeout bounds one connection attempt, so a dial that hangs or a server
// that accepts the connection and never answers is tried again rather than
// holding the whole wait. It applies to the wait only: the pool's connections
// are not limited by it.
const attemptTimeout = 5 * time.Second

// DefaultStartupPolicy gives Postgres the budget a service gives Redis. The first
// pauses are short because the usual cause clears within two seconds: a new
// pod's connections are refused until the NetworkPolicy in front of Postgres
// admits its IP.
func DefaultStartupPolicy() startupwait.Policy {
	return startupwait.Policy{
		Timeout:      5 * time.Minute,
		InitialDelay: 250 * time.Millisecond,
		MaxDelay:     10 * time.Second,
	}
}

// Connect is Open for a service starting up: it waits under policy until
// Postgres can be reached, so a refused connection delays the start instead of
// crashing the process, and Kubernetes never holds back its restart. An answer
// waiting cannot change (a wrong password, an unknown database) fails at once.
// When ctx is cancelled during the wait, the error wraps context.Canceled.
func Connect(ctx context.Context, cfg pkgconfig.PostgresConfig, defaults pkgconfig.PoolConfig, policy startupwait.Policy, logger *slog.Logger) (*sqlx.DB, error) {
	return connect(ctx, cfg, defaults, policy, attemptTimeout, logger)
}

func connect(ctx context.Context, cfg pkgconfig.PostgresConfig, defaults pkgconfig.PoolConfig, policy startupwait.Policy, perAttempt time.Duration, logger *slog.Logger) (*sqlx.DB, error) {
	if _, err := EffectivePool(cfg.Pool, defaults); err != nil {
		return nil, err
	}
	probe := newConnector(cfg)
	probe.connectTimeout = perAttempt
	err := startupwait.Wait(ctx, startupwait.Dependency{
		Name: "Postgres",
		Ping: func(ctx context.Context) error {
			conn, err := probe.Connect(ctx)
			if err != nil {
				return err
			}
			return conn.Close()
		},
		Retryable: isStartupRetryable,
	}, policy, logger)
	if err != nil {
		return nil, connectError(cfg, err)
	}
	return Open(ctx, cfg, defaults)
}

// isStartupRetryable reports whether err means Postgres cannot be reached yet.
// A connection that was refused, dropped or left unanswered is retried, and so
// are the replies a server sends while it cannot serve yet. Any other reply, and
// any failure before or outside the connection itself (an unreadable password
// file, a TLS setup the server refuses), cannot change by waiting.
func isStartupRetryable(err error) bool {
	var reply *pq.Error
	if errors.As(err, &reply) {
		return reply.Code == pqerror.CannotConnectNow || reply.Code == pqerror.TooManyConnections
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, driver.ErrBadConn) {
		return true
	}
	var network *net.OpError
	return errors.As(err, &network) && network.Op != "remote error"
}
