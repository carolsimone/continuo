package handlers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/dead-letter-controller/serialization"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

var now = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

func storedOpen(repo *fakeRepo, src deadletter.Source, group string, original time.Time) deadletter.DeadLetter {
	dl := deadletter.DeadLetter{ID: uuid.New(), DedupKey: uuid.NewString(), Source: src, FailureKind: model.DeadLetterKindPermanent,
		Stream: "node.updated:v1", Group: group, Fields: map[string]string{"k": "v"}, Redrivable: true,
		OriginalAt: original, RecordedAt: original, Status: deadletter.StatusOpen}
	repo.put(dl)
	return dl
}

func TestRedrive_MarksAndWritesOneOutboxRowEach(t *testing.T) {
	h := newHarness()
	a := storedOpen(h.repo, deadletter.SourceConsumer, "g1", now.Add(-time.Hour))
	b := storedOpen(h.repo, deadletter.SourceOutbox, "", now.Add(-time.Hour))

	got, err := h.redriver.Redrive(context.Background(), []uuid.UUID{a.ID, b.ID}, "alice", "fixed upstream")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != a.ID || got[1].ID != b.ID {
		t.Fatalf("result order = %v", got)
	}
	for _, dl := range got {
		if dl.Status != deadletter.StatusRedriven || dl.Redrive.Actor != "alice" {
			t.Fatalf("dl = %+v", dl)
		}
	}
	if len(h.outbox.created) != 2 {
		t.Fatalf("outbox rows = %d", len(h.outbox.created))
	}
	p, err := serialization.DecodeRedrive(h.outbox.created[0].Payload)
	if err != nil || p.RedriveGroup != "g1" || p.RedrivenFrom != a.ID.String() {
		t.Fatalf("payload = %+v %v", p, err)
	}
	if h.outbox.created[0].StreamName != "node.updated:v1" || h.outbox.created[0].EventType != serialization.EventTypeRedrive {
		t.Fatalf("entry = %+v", h.outbox.created[0])
	}
	if h.uow.commits != 1 || len(h.obs.redriven) != 2 {
		t.Fatalf("commits=%d observed=%d", h.uow.commits, len(h.obs.redriven))
	}
}

func TestRedrive_AlreadyRedrivenIsNoOp(t *testing.T) {
	h := newHarness()
	a := storedOpen(h.repo, deadletter.SourceConsumer, "g1", now.Add(-time.Hour))
	if _, err := h.redriver.Redrive(context.Background(), []uuid.UUID{a.ID}, "alice", "first"); err != nil {
		t.Fatal(err)
	}
	got, err := h.redriver.Redrive(context.Background(), []uuid.UUID{a.ID}, "bob", "second")
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Redrive.Actor != "alice" || len(h.outbox.created) != 1 {
		t.Fatalf("second redrive changed state: %+v, outbox rows %d", got[0].Redrive, len(h.outbox.created))
	}
}

func TestRedrive_ExpiredRefused(t *testing.T) {
	h := newHarness()
	fresh := storedOpen(h.repo, deadletter.SourceConsumer, "g1", now.Add(-time.Hour))
	old := storedOpen(h.repo, deadletter.SourceConsumer, "g1", now.Add(-model.ReplayHorizon-time.Minute))
	_, err := h.redriver.Redrive(context.Background(), []uuid.UUID{fresh.ID, old.ID}, "alice", "r")
	if !errors.Is(err, deadletter.ErrExpired) {
		t.Fatalf("err = %v, want ErrExpired", err)
	}
	if len(h.outbox.created) != 0 || h.uow.commits != 0 || h.uow.rollbacks != 1 {
		t.Fatalf("partial redrive: outbox=%d commits=%d rollbacks=%d", len(h.outbox.created), h.uow.commits, h.uow.rollbacks)
	}
	if h.repo.get(fresh.ID).Status != deadletter.StatusOpen {
		t.Fatal("all-or-nothing: the fresh dead letter must stay open")
	}
}

func TestRedrive_UnknownIDNotFound(t *testing.T) {
	h := newHarness()
	missing := uuid.New()
	_, err := h.redriver.Redrive(context.Background(), []uuid.UUID{missing}, "alice", "r")
	if !errors.Is(err, deadletter.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestRedrive_NotRedrivableRefused(t *testing.T) {
	h := newHarness()
	dl := storedOpen(h.repo, deadletter.SourceOutbox, "", now.Add(-time.Hour))
	dl.Redrivable = false
	h.repo.put(dl)
	_, err := h.redriver.Redrive(context.Background(), []uuid.UUID{dl.ID}, "alice", "r")
	if !errors.Is(err, deadletter.ErrNotRedrivable) {
		t.Fatalf("err = %v", err)
	}
}

func TestRedrive_BlankReasonAndNoIDs(t *testing.T) {
	h := newHarness()
	if _, err := h.redriver.Redrive(context.Background(), []uuid.UUID{uuid.New()}, "alice", "  "); !errors.Is(err, deadletter.ErrReasonRequired) {
		t.Fatalf("blank reason: %v", err)
	}
	if _, err := h.redriver.Redrive(context.Background(), nil, "alice", "r"); !errors.Is(err, deadletter.ErrNoIDs) {
		t.Fatalf("no ids: %v", err)
	}
}

func TestRedrive_DuplicateIDsRedriveOnce(t *testing.T) {
	h := newHarness()
	a := storedOpen(h.repo, deadletter.SourceConsumer, "g1", now.Add(-time.Hour))
	got, err := h.redriver.Redrive(context.Background(), []uuid.UUID{a.ID, a.ID}, "alice", "r")
	if err != nil || len(got) != 1 || len(h.outbox.created) != 1 {
		t.Fatalf("got=%d err=%v outbox=%d", len(got), err, len(h.outbox.created))
	}
}
