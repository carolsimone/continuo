package reconciler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain"
)

type fakeLister struct {
	runs []*domain.ActiveRun
	err  error
}

func (f *fakeLister) ListActiveRuns(context.Context) ([]*domain.ActiveRun, error) {
	return f.runs, f.err
}

type fakeStatus struct {
	terminal map[string]string // runID -> terminal status; absent = in-flight
	err      error
}

func (f *fakeStatus) GetTerminalStatus(_ context.Context, runID string) (string, bool, error) {
	if f.err != nil {
		return "", false, f.err
	}
	if s, ok := f.terminal[runID]; ok {
		return s, true, nil
	}
	return "", false, nil
}

type fakeFinalizer struct {
	calls map[string]string // runID -> status
	err   error
}

func (f *fakeFinalizer) FinalizeRun(_ context.Context, runID, status string) error {
	if f.err != nil {
		return f.err
	}
	if f.calls == nil {
		f.calls = map[string]string{}
	}
	f.calls[runID] = status
	return nil
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestReconciler_FinalizesOnlyTerminalActiveRuns(t *testing.T) {
	lister := &fakeLister{runs: []*domain.ActiveRun{
		{RunID: "r-cancelled", ScheduleName: "a"},
		{RunID: "r-inflight", ScheduleName: "b"},
	}}
	st := &fakeStatus{terminal: map[string]string{"r-cancelled": "cancelled"}}
	fin := &fakeFinalizer{}

	New(lister, st, fin, 1, discard()).tick(context.Background())

	if got := fin.calls["r-cancelled"]; got != "cancelled" {
		t.Fatalf("FinalizeRun(r-cancelled) status = %q, want cancelled", got)
	}
	if _, ok := fin.calls["r-inflight"]; ok {
		t.Error("in-flight run must not be finalized")
	}
}

func TestReconciler_StatusErrorSkipsRun(t *testing.T) {
	lister := &fakeLister{runs: []*domain.ActiveRun{{RunID: "r1"}}}
	fin := &fakeFinalizer{}

	New(lister, &fakeStatus{err: errors.New("state down")}, fin, 1, discard()).tick(context.Background())

	if len(fin.calls) != 0 {
		t.Error("must not finalize when the state status read errors")
	}
}

func TestReconciler_ListErrorIsNoop(t *testing.T) {
	fin := &fakeFinalizer{}
	r := New(&fakeLister{err: errors.New("neo4j down")}, &fakeStatus{}, fin, 1, discard())

	r.tick(context.Background()) // must not panic

	if len(fin.calls) != 0 {
		t.Error("no finalize on list error")
	}
}
