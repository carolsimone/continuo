package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// cancelCapturingSchedRepo is a SchedulerTrackerRepository test double that
// records the timestamp passed to CancelTx so a unit test can assert the
// adapter persists the aggregate's authoritative cancellation instant. Only
// CancelTx is exercised; the other methods are unreachable in this test and
// fail loudly if called.
type cancelCapturingSchedRepo struct {
	SchedulerTrackerRepository
	gotCancelledAt time.Time
	called         bool
}

func (r *cancelCapturingSchedRepo) CancelTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _, _ string, cancelledAt time.Time) error {
	r.called = true
	r.gotCancelledAt = cancelledAt
	return nil
}

// TestRunRepositoryAdapter_SaveRun_CancelPersistsAggregateInstant verifies the
// adapter feeds CancelTx the aggregate's CancelledAt() — the same instant the
// aggregate stamped on completed_at — so the persisted row equals the value the
// gRPC response reports. This pins the cancel-path half of the one-timestamp-
// per-logical-event invariant at the adapter boundary.
func TestRunRepositoryAdapter_SaveRun_CancelPersistsAggregateInstant(t *testing.T) {
	now := time.Date(2026, 6, 11, 9, 30, 0, 0, time.UTC)

	// Hydrate a RUNNING run, then cancel it at `now` so cancelDirty is set and
	// cancelled_at == completed_at == now.
	r := run.HydrateRun(
		uuid.New(), "daily",
		run.SchedulerStatusRunning,
		run.InitStatusCompleted,
		run.KindCron,
		nil,
		identity.SystemUserID,
		now.Add(-time.Hour),
		nil, nil, nil, nil,
		nil, nil,
		nil,
		0,
		map[string]run.ServiceMetadata{},
	)
	noopTasks := &noopCancelTaskCollection{}
	if _, err := r.Cancel(context.Background(), noopTasks, "ops", "drift", now); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	sched := &cancelCapturingSchedRepo{}
	// A non-nil tx is required by SaveRun's guard; the fake never dereferences it.
	adapter := NewRunRepository(&sqlx.Tx{}, sched)
	if err := adapter.SaveRun(context.Background(), r); err != nil {
		t.Fatalf("SaveRun: %v", err)
	}

	if !sched.called {
		t.Fatal("CancelTx must be called on the cancel-dirty save path")
	}
	if !sched.gotCancelledAt.Equal(now) {
		t.Fatalf("CancelTx got %v, want the aggregate instant %v", sched.gotCancelledAt, now)
	}
	if r.CompletedAt() == nil || !r.CompletedAt().Equal(sched.gotCancelledAt) {
		t.Fatalf("persisted cancel instant %v must equal aggregate completed_at %v",
			sched.gotCancelledAt, r.CompletedAt())
	}
}

// noopCancelTaskCollection is a TaskCollection whose BulkCancel is a no-op;
// only BulkCancel is invoked by run.Cancel in this test.
type noopCancelTaskCollection struct{ run.TaskCollection }

func (n *noopCancelTaskCollection) BulkCancel(_ context.Context, _ uuid.UUID, _ string, _ time.Time) (int, error) {
	return 0, nil
}
