package snapshotsvc_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/carolsimone/continuo/orchestrator/service/snapshotsvc"
	"github.com/google/uuid"
)

// fakeTxRunner runs fn directly without a real Neo4j tx, using injected
// reader+writer fakes. Test-only.
type fakeTxRunner struct {
	reader snapshot.TopologyReader
	writer snapshot.SnapshotWriter
}

func (f *fakeTxRunner) Run(ctx context.Context, fn func(snapshot.TopologyReader, snapshot.SnapshotWriter) error) error {
	return fn(f.reader, f.writer)
}

type fakeWriter struct {
	called    bool
	gotParams snapshot.Params
	gotProj   []snapshot.TaskProjection
	err       error
}

func (w *fakeWriter) WriteRunAndExecutesEdges(ctx context.Context, p snapshot.Params, proj []snapshot.TaskProjection) error {
	w.called = true
	w.gotParams = p
	w.gotProj = proj
	return w.err
}

// stubSelector returns canned projection / error.
type stubSelector struct {
	projection []snapshot.TaskProjection
	err        error
}

func (s stubSelector) SelectTasks(ctx context.Context, _ snapshot.TopologyReader, _ snapshot.Params) ([]snapshot.TaskProjection, error) {
	return s.projection, s.err
}

// fakeCancelledRepo is a test double for repository.CancelledSchedulesRepository.
type fakeCancelledRepo struct {
	cancelled bool
	err       error
}

var _ repository.CancelledSchedulesRepository = (*fakeCancelledRepo)(nil)

func (f *fakeCancelledRepo) Insert(context.Context, uuid.UUID) error { return nil }
func (f *fakeCancelledRepo) Exists(context.Context, uuid.UUID) (bool, error) {
	return f.cancelled, f.err
}
func (f *fakeCancelledRepo) DeleteExpired(context.Context, time.Duration) (int64, error) {
	return 0, nil
}

func newSvc(reader snapshot.TopologyReader, writer snapshot.SnapshotWriter) *snapshotsvc.Service {
	return newSvcCancel(reader, writer, &fakeCancelledRepo{})
}

func newSvcCancel(reader snapshot.TopologyReader, writer snapshot.SnapshotWriter, cancelledRepo repository.CancelledSchedulesRepository) *snapshotsvc.Service {
	return snapshotsvc.NewService(&fakeTxRunner{reader: reader, writer: writer}, cancelledRepo, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestService_HappyPath_WriterCalledWithProjection(t *testing.T) {
	w := &fakeWriter{}
	want := []snapshot.TaskProjection{{TaskID: uuid.New(), ServiceName: "s", SchemaName: "c", TableName: "t", InitialStatus: "PENDING"}}
	svc := newSvc(nil, w)
	got, err := svc.Snapshot(context.Background(), snapshot.Params{
		RunID: uuid.New().String(), ScheduleName: "x", Kind: "cron",
		Selector: stubSelector{projection: want},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !w.called {
		t.Error("writer not called")
	}
	if len(got) != 1 || got[0].TableName != "t" {
		t.Errorf("got %+v", got)
	}
}

func TestService_EmptyProjection_ReturnsErrEmptyProjection_WriterNotCalled(t *testing.T) {
	w := &fakeWriter{}
	svc := newSvc(nil, w)
	_, err := svc.Snapshot(context.Background(), snapshot.Params{
		Selector: stubSelector{projection: nil},
	})
	if !errors.Is(err, snapshot.ErrEmptyProjection) {
		t.Fatalf("got %v", err)
	}
	if w.called {
		t.Error("writer should not be called on empty projection")
	}
}

func TestService_SelectorError_Propagates(t *testing.T) {
	want := errors.New("boom")
	svc := newSvc(nil, &fakeWriter{})
	_, err := svc.Snapshot(context.Background(), snapshot.Params{
		Selector: stubSelector{err: want},
	})
	if !errors.Is(err, want) {
		t.Fatalf("got %v", err)
	}
}

func TestService_TargetNotFound_PropagatesUnchanged(t *testing.T) {
	svc := newSvc(nil, &fakeWriter{})
	_, err := svc.Snapshot(context.Background(), snapshot.Params{
		Selector: stubSelector{err: snapshot.ErrTargetNotFound},
	})
	if !errors.Is(err, snapshot.ErrTargetNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestService_WriterError_Propagates(t *testing.T) {
	want := errors.New("write failed")
	w := &fakeWriter{err: want}
	svc := newSvc(nil, w)
	_, err := svc.Snapshot(context.Background(), snapshot.Params{
		RunID:    uuid.New().String(),
		Selector: stubSelector{projection: []snapshot.TaskProjection{{TaskID: uuid.New()}}},
	})
	if !errors.Is(err, want) {
		t.Fatalf("got %v", err)
	}
}

func TestService_CancelledSchedule_StampsCancelledOnParams(t *testing.T) {
	w := &fakeWriter{}
	svc := newSvcCancel(nil, w, &fakeCancelledRepo{cancelled: true})
	_, err := svc.Snapshot(context.Background(), snapshot.Params{
		RunID:    uuid.New().String(),
		Selector: stubSelector{projection: []snapshot.TaskProjection{{TaskID: uuid.New()}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !w.called {
		t.Fatal("writer not called")
	}
	if !w.gotParams.Cancelled {
		t.Error("expected Params.Cancelled=true for a cancelled schedule")
	}
}

func TestService_NotCancelled_LeavesCancelledFalse(t *testing.T) {
	w := &fakeWriter{}
	svc := newSvcCancel(nil, w, &fakeCancelledRepo{cancelled: false})
	_, err := svc.Snapshot(context.Background(), snapshot.Params{
		RunID:    uuid.New().String(),
		Selector: stubSelector{projection: []snapshot.TaskProjection{{TaskID: uuid.New()}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if w.gotParams.Cancelled {
		t.Error("expected Params.Cancelled=false for a non-cancelled schedule")
	}
}

func TestService_CancelledRepoError_Propagates_WriterNotCalled(t *testing.T) {
	want := errors.New("guard read failed")
	w := &fakeWriter{}
	svc := newSvcCancel(nil, w, &fakeCancelledRepo{err: want})
	_, err := svc.Snapshot(context.Background(), snapshot.Params{
		RunID:    uuid.New().String(),
		Selector: stubSelector{projection: []snapshot.TaskProjection{{TaskID: uuid.New()}}},
	})
	if !errors.Is(err, want) {
		t.Fatalf("got %v", err)
	}
	if w.called {
		t.Error("writer must not be called when the cancelled-guard read fails")
	}
}
