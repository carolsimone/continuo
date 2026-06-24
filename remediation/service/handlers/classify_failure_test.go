package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/remediation/domain/event"
	"github.com/carolsimone/continuo/remediation/domain/failure"
	"github.com/carolsimone/continuo/remediation/domain/repository"
	"github.com/carolsimone/continuo/remediation/service/ports"
	"github.com/carolsimone/continuo/remediation/service/uow"
)

// --- fakes ---

type fakeLogReader struct {
	text string
	err  error
}

func (f fakeLogReader) Fetch(_ context.Context, _ string) (string, error) { return f.text, f.err }

// mapLogReader returns a different body per URI, so a test can give the dbt log
// and the run-results artifact distinct contents.
type mapLogReader struct{ byURI map[string]string }

func (m mapLogReader) Fetch(_ context.Context, uri string) (string, error) {
	if v, ok := m.byURI[uri]; ok {
		return v, nil
	}
	return "", ports.ErrLogNotFound
}

type fakeClock struct{}

func (fakeClock) Now() time.Time { return time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC) }

type fakeDecisionRepo struct {
	saved    []repository.ClassificationDecision
	inserted bool
}

func (r *fakeDecisionRepo) Upsert(_ context.Context, d repository.ClassificationDecision) (bool, error) {
	r.saved = append(r.saved, d)
	return r.inserted, nil
}

// fakeOutbox implements outbox.Repository for in-process tests.
type fakeOutbox struct{ entries []*outbox.Entry }

func (o *fakeOutbox) Create(_ context.Context, e *outbox.Entry) error {
	o.entries = append(o.entries, e)
	return nil
}

func (o *fakeOutbox) GetPendingBatch(_ context.Context, _ int) ([]*outbox.Entry, error) {
	return nil, nil
}

func (o *fakeOutbox) MarkProcessed(_ context.Context, _ uuid.UUID) error               { return nil }
func (o *fakeOutbox) MarkProcessedBatch(_ context.Context, _ []uuid.UUID) error        { return nil }
func (o *fakeOutbox) MarkFailed(_ context.Context, _ uuid.UUID, _ string) error        { return nil }
func (o *fakeOutbox) IncrementRetry(_ context.Context, _ uuid.UUID) error              { return nil }

type fakeUoW struct {
	dec       *fakeDecisionRepo
	ob        *fakeOutbox
	committed bool
}

func (u *fakeUoW) Begin(context.Context) error                                { return nil }
func (u *fakeUoW) Commit() error                                              { u.committed = true; return nil }
func (u *fakeUoW) Rollback() error                                            { return nil }
func (u *fakeUoW) DecisionRepo() repository.ClassificationDecisionRepository { return u.dec }
func (u *fakeUoW) OutboxRepo() outbox.Repository                              { return u.ob }

func depsWith(u *fakeUoW, log string, err error) Deps {
	return Deps{
		NewUoW:    func() uow.UnitOfWork { return u },
		LogReader: fakeLogReader{text: log, err: err},
		Clock:     fakeClock{},
		Logger:    slog.Default(),
	}
}

// --- tests ---

func TestClassifyFailure_LogicEmitsTrigger(t *testing.T) {
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	ev := failure.FailureEvidence{
		Source:    failure.SourceValidation,
		ReleaseID: "r1",
		NodeID:    "s.n",
		Repo:      "o/r",
		CommitSHA: "abc",
		DBTLogURI: "s3://b/k",
	}
	err := ClassifyFailure(context.Background(), depsWith(u, `Database Error: column "x" does not exist`, nil), ev)
	if err != nil {
		t.Fatal(err)
	}
	if len(u.dec.saved) != 1 || u.dec.saved[0].Category != failure.CategoryLogic {
		t.Fatalf("decision not recorded as logic: %+v", u.dec.saved)
	}
	if len(u.ob.entries) != 1 {
		t.Fatalf("expected 1 outbox trigger, got %d", len(u.ob.entries))
	}
	var p event.RemediationRequested
	_ = json.Unmarshal(u.ob.entries[0].Payload, &p)
	if p.Category != "logic" || p.NodeID != "s.n" || p.EventID != event.RemediationEventID("r1", "s.n").String() {
		t.Fatalf("bad trigger payload: %+v", p)
	}
	if !u.committed {
		t.Fatal("tx not committed")
	}
}

func TestClassifyFailure_StructuredBranchWinsOverLog(t *testing.T) {
	// The text log alone would classify infra (drop, no trigger); the structured
	// run_results says status=fail (a test), which is healable and must emit.
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	ev := failure.FailureEvidence{
		Source:        failure.SourceValidation,
		ReleaseID:     "r1",
		NodeID:        "s.n",
		DBTLogURI:     "s3://b/log",
		RunResultsURI: "run-results/x.json",
	}
	reader := mapLogReader{byURI: map[string]string{
		"s3://b/log":         "could not connect to database: connection refused",
		"run-results/x.json": `{"schema_version":1,"status":"fail","message":"Failure in test not_null_x","failures":3,"unique_id":"test.svc.x"}`,
	}}
	deps := Deps{
		NewUoW:    func() uow.UnitOfWork { return u },
		LogReader: reader,
		Clock:     fakeClock{},
		Logger:    slog.Default(),
	}
	if err := ClassifyFailure(context.Background(), deps, ev); err != nil {
		t.Fatal(err)
	}
	if len(u.dec.saved) != 1 || u.dec.saved[0].Category != failure.CategoryTest {
		t.Fatalf("expected structured test classification, got %+v", u.dec.saved)
	}
	if len(u.ob.entries) != 1 {
		t.Fatalf("test category is healable → expected 1 trigger, got %d", len(u.ob.entries))
	}
}

func TestClassifyFailure_InfraDropsNoTrigger(t *testing.T) {
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	ev := failure.FailureEvidence{Source: failure.SourceValidation, ReleaseID: "r1", NodeID: "s.n"}
	err := ClassifyFailure(context.Background(), depsWith(u, "could not connect to database: connection refused", nil), ev)
	if err != nil {
		t.Fatal(err)
	}
	if len(u.dec.saved) != 1 || u.dec.saved[0].Decision != failure.DecisionDrop {
		t.Fatalf("infra failure must be recorded as drop: %+v", u.dec.saved)
	}
	if len(u.ob.entries) != 0 {
		t.Fatalf("dropped failure must not emit a trigger, got %d", len(u.ob.entries))
	}
}

func TestClassifyFailure_IdempotentSkipsTrigger(t *testing.T) {
	// inserted=false → already classified → no duplicate trigger.
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: false}, ob: &fakeOutbox{}}
	ev := failure.FailureEvidence{Source: failure.SourceValidation, ReleaseID: "r1", NodeID: "s.n"}
	err := ClassifyFailure(context.Background(), depsWith(u, `Database Error: column "x" does not exist`, nil), ev)
	if err != nil {
		t.Fatal(err)
	}
	if len(u.ob.entries) != 0 {
		t.Fatalf("redelivery must not re-emit, got %d", len(u.ob.entries))
	}
}
