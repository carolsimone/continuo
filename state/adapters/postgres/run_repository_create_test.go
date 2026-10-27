package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// existingRunSchedRepo is a RunTableRepository whose CreateTx reports
// that the row already exists. Only CreateTx is exercised.
type existingRunSchedRepo struct {
	RunTableRepository
}

func (existingRunSchedRepo) CreateTx(_ context.Context, _ *sqlx.Tx, _ *RunRow) error {
	return ErrRunExists
}

func TestRunRepositoryAdapter_SaveRun_ExistingRunIsErrRunAlreadyExists(t *testing.T) {
	rn, err := run.NewPromotedSeedsRun(pkgevents.DefaultTenantID, uuid.New(), "promote-seed-test", run.ReleaseStamp{ReleaseID: "rel-1", PromotionSeq: 1}, time.Now())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	// A non-nil tx is required by SaveRun's guard; the fake never dereferences it.
	adapter := NewRunRepository(&sqlx.Tx{}, existingRunSchedRepo{})

	if err := adapter.SaveRun(context.Background(), rn); !errors.Is(err, run.ErrRunAlreadyExists) {
		t.Fatalf("SaveRun err = %v, want run.ErrRunAlreadyExists", err)
	}
}
