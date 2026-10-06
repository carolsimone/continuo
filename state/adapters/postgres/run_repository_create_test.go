package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// existingRunSchedRepo is a SchedulerTrackerRepository whose CreateTx reports
// that the row already exists. Only CreateTx is exercised.
type existingRunSchedRepo struct {
	SchedulerTrackerRepository
}

func (existingRunSchedRepo) CreateTx(_ context.Context, _ *sqlx.Tx, _ *SchedulerTracker) error {
	return ErrRunExists
}

func TestRunRepositoryAdapter_SaveRun_ExistingRunIsErrRunAlreadyExists(t *testing.T) {
	rn, _, err := run.NewPromotedSeedsRun(uuid.New(), "promote-seed-test", "rel-1",
		[]run.SeedNode{{NodeID: run.NodeID{ServiceName: "core", SchemaName: "analytics", TableName: "seed_users"}}},
		time.Now())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	// A non-nil tx is required by SaveRun's guard; the fake never dereferences it.
	adapter := NewRunRepository(&sqlx.Tx{}, existingRunSchedRepo{})

	if err := adapter.SaveRun(context.Background(), rn); !errors.Is(err, run.ErrRunAlreadyExists) {
		t.Fatalf("SaveRun err = %v, want run.ErrRunAlreadyExists", err)
	}
}
