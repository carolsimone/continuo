package postgres

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/database"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// newTestDB opens a connection to the test Postgres instance. Package-local
// counterpart to scheduler_repository_test.go's helper of the same name in
// package postgres_test — this file lives in package postgres (it calls the
// unexported hydrateRun/dehydrateRun), so it cannot import that one.
func newTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	db, err := database.NewConnection(pkgconfig.LoadPostgres(&pkgconfig.Validator{}))
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

// TestSchedulerTracker_OperationRoundTrip persists a run created with a test
// operation and asserts the reloaded aggregate reports OperationTest.
func TestSchedulerTracker_OperationRoundTrip(t *testing.T) {
	db := newTestDB(t)
	// The brief's sample passes a nil logger; schedulerTrackerRepository.Create
	// logs unconditionally on its success path (r.logger.Info(...)), which
	// panics on a nil *slog.Logger. Use a discard logger instead, matching the
	// sibling helper in scheduler_repository_test.go.
	repo := NewSchedulerTrackerRepository(db, discardLogger())

	r, _, err := run.NewPendingRun("op-roundtrip", run.KindTrigger, nil, "user-1", nil, model.OperationTest, time.Now())
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
	t.Cleanup(func() { db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, r.ScheduleID()) })

	got, err := repo.GetByID(context.Background(), r.ScheduleID())
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
