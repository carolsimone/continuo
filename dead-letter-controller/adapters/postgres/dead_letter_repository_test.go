//go:build integration

package postgres

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/dead-letter-controller/serialization"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/testdeps"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

func testDB(t *testing.T) *sqlx.DB {
	t.Helper()
	v := &pkgconfig.Validator{}
	cfg := pkgconfig.LoadPostgres(v)
	if len(v.Missing()) > 0 {
		testdeps.Unavailable(t, "POSTGRES_* not set — skipping Postgres integration test")
	}
	db, err := pkgdb.Open(context.Background(), cfg, pkgconfig.PoolConfig{MaxOpenConns: 4, MaxIdleConns: 2})
	if err != nil {
		testdeps.Unavailable(t, "Postgres unreachable: %v", err)
	}
	var exists bool
	if err := db.Get(&exists, `SELECT to_regclass('public.dead_letters') IS NOT NULL`); err != nil || !exists {
		t.Fatalf("dead_letters is missing: run the Flyway migrations (make test-deps-up)")
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM dead_letter_outbox WHERE aggregate_type = $1`, serialization.AggregateTypeDeadLetter)
		_, _ = db.Exec(`DELETE FROM dead_letters`)
		_ = db.Close()
	})
	_, _ = db.Exec(`DELETE FROM dead_letters`)
	return db
}

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func redriveEntry(dl deadletter.DeadLetter, body []byte) *outbox.Entry {
	return &outbox.Entry{ID: uuid.New(), AggregateType: serialization.AggregateTypeDeadLetter, AggregateID: dl.ID,
		EventType: serialization.EventTypeRedrive, Payload: body, StreamName: dl.Stream, Status: "pending"}
}

func sample(key string, original time.Time) deadletter.DeadLetter {
	return deadletter.DeadLetter{ID: uuid.New(), DedupKey: key, Source: deadletter.SourceConsumer,
		FailureKind: model.DeadLetterKindPermanent, Stream: "node.updated:v1", Group: "g", OriginalMessageID: "1-0",
		Producer: "state", Error: "boom", DeliveryCount: 1, Fields: map[string]string{"k": "v"}, Redrivable: true,
		OriginalAt: original.UTC().Truncate(time.Microsecond), RecordedAt: original.UTC().Truncate(time.Microsecond), Status: deadletter.StatusOpen}
}

func TestPostgresInsert_KeepsFirstPerDedupKey(t *testing.T) {
	repo := NewDeadLetterRepository(testDB(t))
	ctx := context.Background()
	first := sample("consumer|s|g|1-0", time.Now())
	ok, err := repo.Insert(ctx, first)
	if err != nil || !ok {
		t.Fatalf("first insert: %v %v", ok, err)
	}
	second := sample("consumer|s|g|1-0", time.Now())
	ok, err = repo.Insert(ctx, second)
	if err != nil || ok {
		t.Fatalf("duplicate insert reported inserted=%v err=%v", ok, err)
	}
	got, err := repo.Get(ctx, first.ID)
	if err != nil || got.Fields["k"] != "v" || !got.OriginalAt.Equal(first.OriginalAt) || got.Status != deadletter.StatusOpen {
		t.Fatalf("got %+v %v", got, err)
	}
}

func TestPostgresGet_UnknownIsNotFound(t *testing.T) {
	repo := NewDeadLetterRepository(testDB(t))
	if _, err := repo.Get(context.Background(), uuid.New()); err != deadletter.ErrNotFound {
		t.Fatalf("err = %v", err)
	}
}

func TestPostgresList_FiltersAndOrders(t *testing.T) {
	repo := NewDeadLetterRepository(testDB(t))
	ctx := context.Background()
	older := sample("a", time.Now().Add(-time.Hour))
	newer := sample("b", time.Now())
	other := sample("c", time.Now())
	other.Stream = "other:v1"
	for _, dl := range []deadletter.DeadLetter{older, newer, other} {
		if _, err := repo.Insert(ctx, dl); err != nil {
			t.Fatal(err)
		}
	}
	got, err := repo.List(ctx, deadletter.Filter{Stream: "node.updated:v1", Status: deadletter.StatusOpen, Limit: 10})
	if err != nil || len(got) != 2 || got[0].ID != newer.ID {
		t.Fatalf("got %v %v", got, err)
	}
	n, err := repo.CountOpen(ctx)
	if err != nil || n != 3 {
		t.Fatalf("open = %d %v", n, err)
	}
}

func TestPostgresRedrive_OutboxRowCommittedWithStatus(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	dl := sample("r", time.Now())
	if _, err := NewDeadLetterRepository(db).Insert(ctx, dl); err != nil {
		t.Fatal(err)
	}
	u := NewUnitOfWork(db, testLogger())
	if err := u.Begin(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := u.DeadLetters().LockForRedrive(ctx, []uuid.UUID{dl.ID, uuid.New()})
	if err != nil || len(rows) != 1 {
		t.Fatalf("lock: %v %d", err, len(rows))
	}
	rows[0].MarkRedriven("alice", "fixed", time.Now().UTC())
	if err := u.DeadLetters().SaveRedrive(ctx, rows[0]); err != nil {
		t.Fatal(err)
	}
	body, _ := serialization.EncodeRedrive(rows[0])
	if err := u.Outbox().Create(ctx, redriveEntry(rows[0], body)); err != nil {
		t.Fatal(err)
	}
	if err := u.Rollback(); err != nil {
		t.Fatal(err)
	}
	var outboxRows int
	_ = db.Get(&outboxRows, `SELECT count(*) FROM dead_letter_outbox WHERE aggregate_id = $1`, dl.ID)
	got, _ := NewDeadLetterRepository(db).Get(ctx, dl.ID)
	if outboxRows != 0 || got.Status != deadletter.StatusOpen {
		t.Fatalf("rollback leaked: outbox=%d status=%s", outboxRows, got.Status)
	}

	// The same work committed: the status and the outbox row land together.
	u = NewUnitOfWork(db, testLogger())
	if err := u.Begin(ctx); err != nil {
		t.Fatal(err)
	}
	rows, _ = u.DeadLetters().LockForRedrive(ctx, []uuid.UUID{dl.ID})
	rows[0].MarkRedriven("alice", "fixed", time.Now().UTC())
	if err := u.DeadLetters().SaveRedrive(ctx, rows[0]); err != nil {
		t.Fatal(err)
	}
	body, _ = serialization.EncodeRedrive(rows[0])
	if err := u.Outbox().Create(ctx, redriveEntry(rows[0], body)); err != nil {
		t.Fatal(err)
	}
	if err := u.Commit(); err != nil {
		t.Fatal(err)
	}
	_ = db.Get(&outboxRows, `SELECT count(*) FROM dead_letter_outbox WHERE aggregate_id = $1 AND status = 'pending'`, dl.ID)
	got, _ = NewDeadLetterRepository(db).Get(ctx, dl.ID)
	if outboxRows != 1 || got.Status != deadletter.StatusRedriven || got.Redrive.Actor != "alice" {
		t.Fatalf("commit: outbox=%d status=%s redrive=%+v", outboxRows, got.Status, got.Redrive)
	}
}

func TestPostgresDeleteExpired(t *testing.T) {
	repo := NewDeadLetterRepository(testDB(t))
	ctx := context.Background()
	old := sample("old", time.Now().Add(-model.ReplayHorizon-time.Hour))
	fresh := sample("fresh", time.Now())
	for _, dl := range []deadletter.DeadLetter{old, fresh} {
		if _, err := repo.Insert(ctx, dl); err != nil {
			t.Fatal(err)
		}
	}
	gone, err := repo.DeleteExpired(ctx, time.Now().Add(-model.ReplayHorizon), 10)
	if err != nil || len(gone) != 1 || gone[0].ID != old.ID {
		t.Fatalf("gone = %v %v", gone, err)
	}
}

// A row whose original_at is exactly the cutoff instant is expired (CheckRedrive
// treats it as non-redrivable), so DeleteExpired removes it too.
func TestPostgresDeleteExpired_CutoffInstantIsInclusive(t *testing.T) {
	repo := NewDeadLetterRepository(testDB(t))
	ctx := context.Background()
	cutoff := time.Now().Add(-model.ReplayHorizon).UTC().Truncate(time.Microsecond)
	edge := sample("edge", cutoff)
	if _, err := repo.Insert(ctx, edge); err != nil {
		t.Fatal(err)
	}
	gone, err := repo.DeleteExpired(ctx, cutoff, 10)
	if err != nil || len(gone) != 1 || gone[0].ID != edge.ID {
		t.Fatalf("row at the cutoff instant not deleted: %v %v", gone, err)
	}
}

func TestPostgresBacklog(t *testing.T) {
	repo := NewDeadLetterRepository(testDB(t))
	ctx := context.Background()
	for _, k := range []string{"x", "y"} {
		if _, err := repo.Insert(ctx, sample(k, time.Now())); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := repo.Backlog(ctx)
	if err != nil || len(rows) != 1 || rows[0].Open != 2 || rows[0].Kind != model.DeadLetterKindPermanent {
		t.Fatalf("backlog = %+v %v", rows, err)
	}
}
