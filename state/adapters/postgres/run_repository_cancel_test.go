package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// cancelCapturingSchedRepo is a RunTableRepository test double that
// records what the adapter passes to CancelTx and UpdateRunRowTx, so a unit
// test can assert the adapter persists the aggregate's cancellation instant
// and status. Only those two are exercised; the other methods are unreachable
// in this test and fail loudly if called.
type cancelCapturingSchedRepo struct {
	RunTableRepository
	gotCancelledAt time.Time
	gotStatus      run.RunStatus
	called         bool
	gotFields      RunRowUpdate
}

func (r *cancelCapturingSchedRepo) CancelTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, status run.RunStatus, _, _ string, cancelledAt time.Time) error {
	r.called = true
	r.gotStatus = status
	r.gotCancelledAt = cancelledAt
	return nil
}

func (r *cancelCapturingSchedRepo) UpdateRunRowTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, fields RunRowUpdate) error {
	r.gotFields = fields
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
	r := run.HydrateRun(pkgevents.DefaultTenantID,
		uuid.New(), "daily",
		run.RunStatusRunning,
		run.InitStatusCompleted,
		run.KindCron,
		nil,
		identity.SystemUserID,
		model.OperationRun,
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
	if sched.gotStatus != run.RunStatusCancelled {
		t.Fatalf("CancelTx got status %q, want cancelled: nothing was in flight", sched.gotStatus)
	}
	if sched.gotFields.Status != nil || sched.gotFields.CompletedAtNow {
		t.Fatalf("the consolidated update rewrote what CancelTx wrote: %+v", sched.gotFields)
	}
	if sched.gotFields.TerminalTaskCount == nil || *sched.gotFields.TerminalTaskCount != 2 {
		t.Fatalf("terminal count = %v, want the two pending tasks the cancel ended", sched.gotFields.TerminalTaskCount)
	}
}

// noopCancelTaskCollection is a TaskCollection whose BeginStop reports two
// pending tasks cancelled and nothing in flight; only BeginStop is invoked by
// run.Cancel in this test.
type noopCancelTaskCollection struct{ run.TaskCollection }

func (n *noopCancelTaskCollection) BeginStop(_ context.Context, _ uuid.UUID, _ string, _ time.Time) (run.StopCounts, error) {
	return run.StopCounts{Cancelled: 2}, nil
}
