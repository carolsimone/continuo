package postgres

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// newTestDB opens a connection to the test Postgres instance. Package-local
// counterpart to scheduler_repository_test.go's helper of the same name in
// package postgres_test — this file lives in package postgres (it calls the
// unexported hydrateRun/dehydrateRun), so it cannot import that one.
func newTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	db, err := pkgdb.Open(context.Background(), pkgconfig.LoadPostgres(&pkgconfig.Validator{}), pkgconfig.PoolConfig{MaxOpenConns: 10, MaxIdleConns: 2})
	if err != nil {
		t.Fatalf("connect to test db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// discardLogger returns a logger that drops all output. Package-local
// counterpart to scheduler_repository_test.go's helper of the same name.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestRunRow_OperationRoundTrip persists a run created with a test
// operation and asserts the reloaded aggregate reports OperationTest.
func TestRunRow_OperationRoundTrip(t *testing.T) {
	db := newTestDB(t)
	// The brief's sample passes a nil logger; RunRepository.Create
	// logs unconditionally on its success path (r.logger.Info(...)), which
	// panics on a nil *slog.Logger. Use a discard logger instead, matching the
	// sibling helper in scheduler_repository_test.go.
	repo := NewRunRepository(db, discardLogger())

	r, err := run.NewPendingRun(pkgevents.DefaultTenantID, uuid.New(), "op-roundtrip", run.KindTrigger, nil, "user-1", nil, model.OperationTest, run.ReleaseStamp{}, time.Now())
	if err != nil {
		t.Fatalf("NewPendingRun: %v", err)
	}
	tr, err := dehydrateRun(r)
	if err != nil {
		t.Fatalf("dehydrateRun: %v", err)
	}
	if err := repo.Create(context.Background(), tr); err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() { db.Exec(`DELETE FROM runs WHERE run_id = $1`, r.RunID()) })

	got, err := repo.GetByID(context.Background(), r.TenantID(), r.RunID())
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	reloaded, err := hydrateRun(got)
	if err != nil {
		t.Fatalf("hydrateRun: %v", err)
	}
	if reloaded.Operation() != model.OperationTest {
		t.Fatalf("reloaded Operation() = %q, want test", reloaded.Operation())
	}
}
