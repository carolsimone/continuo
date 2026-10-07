//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/adapters/postgres"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/stretchr/testify/require"
)

func requireWake(t *testing.T, w *outbox.PostgresWaker, why string) {
	t.Helper()
	select {
	case <-w.Wake():
	case <-time.After(5 * time.Second):
		t.Fatalf("no wake: %s", why)
	}
}

func requireNoWake(t *testing.T, w *outbox.PostgresWaker, why string) {
	t.Helper()
	select {
	case <-w.Wake():
		t.Fatalf("unexpected wake: %s", why)
	case <-time.After(700 * time.Millisecond):
	}
}

func TestDeploymentsNotify_WakesOnAcceptAndSlotReleaseOnly(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	waker, err := outbox.NewPostgresWaker(ctx, pkgconfig.LoadPostgres(&pkgconfig.Validator{}).DSN(), postgres.DeploymentsChannel, testLogger())
	require.NoError(t, err)
	defer func() { _ = waker.Close() }()
	select { // a waker may signal once when its listener connects
	case <-waker.Wake():
	case <-time.After(300 * time.Millisecond):
	}

	id := seedDue(t, db, time.Now())
	requireWake(t, waker, "an accepted deployment")

	exec := func(status string) {
		_, err := db.Exec(`UPDATE deployments SET status = $2 WHERE id = $1`, id, status)
		require.NoError(t, err)
	}
	exec("reserved")
	requireNoWake(t, waker, "taking a slot frees nothing")
	exec("starting")
	requireNoWake(t, waker, "moving between in-flight states frees nothing")
	exec("pending")
	requireWake(t, waker, "a deployment back in the queue")
	exec("reserved")
	requireNoWake(t, waker, "taking a slot frees nothing")
	exec("done")
	requireWake(t, waker, "a released slot")
}
