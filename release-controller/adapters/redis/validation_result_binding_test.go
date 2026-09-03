package redis

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/carolsimone/continuo/release-controller/service/uow"
	goredis "github.com/redis/go-redis/v9"
)

// newValidationResultDeps builds a handlers.Deps over a fake UnitOfWork whose
// ReleaseRepo records the IDs it was asked to Load and whether AdvanceQueue's
// tx-scoped queue lock was taken. Every release loads as absent, so both
// handlers reach their unknown-release drop and the routing can be asserted
// without standing up a real aggregate.
func newValidationResultDeps() (*handlers.Deps, *fakeReleaseRepo) {
	repo := &fakeReleaseRepo{}
	u := &fakeUoW{releaseRepo: repo}
	deps := &handlers.Deps{
		NewUoW: func() uow.UnitOfWork { return u },
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return deps, repo
}

func newDiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestValidationResultHandler_RoutesNodeKind verifies that a kind=node message is
// decoded into handlers.NodeValidationResultInput and dispatched to
// HandleNodeValidationResult — proven by the release ID reaching Load — without
// advancing the queue (only the terminal decision does that).
func TestValidationResultHandler_RoutesNodeKind(t *testing.T) {
	deps, repo := newValidationResultDeps()
	handler := newValidationResultHandler(deps, newDiscardLogger())

	payload := map[string]any{
		"kind":       "node",
		"release_id": "rel-node",
		"stage":      "validation",
		"node_id":    "core",
		"status":     "ok",
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	msg := goredis.XMessage{ID: "0-1", Values: map[string]any{"payload": string(raw)}}

	if err := handler(context.Background(), msg); err != nil {
		t.Fatalf("want nil (unknown release dropped), got %v", err)
	}
	if repo.loadedID != "rel-node" {
		t.Errorf("Load called with %q, want %q — node input never reached HandleNodeValidationResult", repo.loadedID, "rel-node")
	}
	if repo.advanceLocked {
		t.Error("kind=node must not advance the release queue")
	}
}

// TestValidationResultHandler_RoutesCompleteKind verifies that a kind=complete
// message is decoded into handlers.HandleValidationResultInput, dispatched to
// HandleValidationResult (proven by the release ID reaching Load), and then
// advances the queue (proven by the queue lock being taken in AdvanceQueue).
func TestValidationResultHandler_RoutesCompleteKind(t *testing.T) {
	deps, repo := newValidationResultDeps()
	handler := newValidationResultHandler(deps, newDiscardLogger())

	payload := map[string]any{
		"kind":             "complete",
		"release_id":       "rel-complete",
		"aggregate_status": "ok",
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	msg := goredis.XMessage{ID: "0-2", Values: map[string]any{"payload": string(raw)}}

	if err := handler(context.Background(), msg); err != nil {
		t.Fatalf("want nil (unknown release dropped), got %v", err)
	}
	if repo.loadedID != "rel-complete" {
		t.Errorf("Load called with %q, want %q — complete input never reached HandleValidationResult", repo.loadedID, "rel-complete")
	}
	if !repo.advanceLocked {
		t.Error("kind=complete must advance the release queue after the terminal decision")
	}
}

// TestValidationResultHandler_AcksMalformedPayload verifies that an undecodable
// payload is treated as a permanent failure: the handler returns nil so the
// consumer ACKs and drops it. deps is nil because decode failure short-circuits
// before any handler runs.
func TestValidationResultHandler_AcksMalformedPayload(t *testing.T) {
	handler := newValidationResultHandler(nil, newDiscardLogger())

	msg := goredis.XMessage{ID: "0-3", Values: map[string]any{"payload": "not valid json"}}
	if err := handler(context.Background(), msg); err != nil {
		t.Fatalf("want nil (ack) for malformed payload, got %v", err)
	}
}

// TestValidationResultHandler_AcksUnknownKind verifies that a well-formed payload
// with a kind the consumer does not recognise is dropped (ack) rather than
// retried forever. deps is nil because an unknown kind never dispatches.
func TestValidationResultHandler_AcksUnknownKind(t *testing.T) {
	handler := newValidationResultHandler(nil, newDiscardLogger())

	raw, err := json.Marshal(map[string]any{"kind": "banana", "release_id": "rel-x"})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	msg := goredis.XMessage{ID: "0-4", Values: map[string]any{"payload": string(raw)}}
	if err := handler(context.Background(), msg); err != nil {
		t.Fatalf("want nil (ack) for unknown kind, got %v", err)
	}
}

// --- minimal fakes ---

// fakeUoW is a minimal uow.UnitOfWork stub. Begin/Commit/Rollback are no-ops;
// ReleaseRepo backs both handlers' unknown-release path and AdvanceQueue's
// queue read. LockReleaseQueue records that AdvanceQueue's serialisation lock
// was taken so the complete path can be told apart from the node path.
type fakeUoW struct {
	releaseRepo *fakeReleaseRepo
}

func (f *fakeUoW) RunRepo() repository.RunRepository                 { return f.releaseRepo }
func (f *fakeUoW) CurrentProdRepo() repository.CurrentProdRepository { panic("not implemented") }
func (f *fakeUoW) ServiceProdRepo() repository.ServiceProdRepository { panic("not implemented") }
func (f *fakeUoW) OutboxRepo() pkgoutbox.Repository                  { panic("not implemented") }
func (f *fakeUoW) MessageProcessingRepo() messageprocessing.Repository {
	panic("not implemented")
}
func (f *fakeUoW) Begin(ctx context.Context) error { return nil }
func (f *fakeUoW) Commit() error                   { return nil }
func (f *fakeUoW) Rollback() error                 { return nil }
func (f *fakeUoW) LockReleaseQueue(ctx context.Context) error {
	f.releaseRepo.advanceLocked = true
	return nil
}

// fakeReleaseRepo records the last Load ID and whether AdvanceQueue's lock was
// taken. Load reports every release as absent; the queue queries report an empty
// queue so AdvanceQueue completes as a no-op after taking its lock.
type fakeReleaseRepo struct {
	loadedID      string
	advanceLocked bool
}

func (r *fakeReleaseRepo) Get(ctx context.Context, id string) (*pipeline.Run, error) {
	panic("not implemented")
}
func (r *fakeReleaseRepo) Load(ctx context.Context, id string) (*pipeline.Run, error) {
	r.loadedID = id
	return nil, nil
}
func (r *fakeReleaseRepo) Save(ctx context.Context, rel *pipeline.Run) error {
	panic("not implemented")
}
func (r *fakeReleaseRepo) NextQueued(ctx context.Context) (*pipeline.Run, error) {
	return nil, nil
}
func (r *fakeReleaseRepo) Active(ctx context.Context) (*pipeline.Run, error) {
	return nil, nil
}
func (r *fakeReleaseRepo) List(ctx context.Context, f repository.ListFilter) ([]*pipeline.Run, *repository.ListCursor, error) {
	panic("not implemented")
}
func (r *fakeReleaseRepo) DeleteFinishedBefore(ctx context.Context, cutoff time.Time, keepReleaseIDs []string) (int, error) {
	panic("not implemented")
}
