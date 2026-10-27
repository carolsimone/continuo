package postgres

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// sameInstant matches a time argument equal to want.
type sameInstant struct{ want time.Time }

func (m sameInstant) Match(v driver.Value) bool {
	got, ok := v.(time.Time)
	return ok && got.Equal(m.want)
}

// TestRunRepository_SaveRun_CancelPersistsAggregateInstant verifies the
// repository feeds the cancel statement the aggregate's CancelledAt() — the
// same instant the aggregate stamped on completed_at — so the persisted row
// equals the value the gRPC response reports. This pins the cancel-path half of
// the one-timestamp-per-logical-event invariant at the repository boundary.
func TestRunRepository_SaveRun_CancelPersistsAggregateInstant(t *testing.T) {
	now := time.Date(2026, 6, 11, 9, 30, 0, 0, time.UTC)

	// Hydrate a RUNNING run, then cancel it at `now` so cancelDirty is set and
	// cancelled_at == completed_at == now.
	r := run.HydrateRun(pkgevents.DefaultTenantID,
		uuid.New(), "daily",
		run.RunStatusRunning,
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

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	// The cancel statement writes the aggregate's status and instant (status
	// cancelled, so completed_at is the same instant), the actor and the
	// reason. The consolidated update that follows writes only the terminal
	// count: it does not rewrite the status or
	// completed_at the cancel statement wrote.
	mock.ExpectBegin()
	mock.ExpectExec(`^\s*UPDATE runs\s+SET status\s+= \$1,\s+cancelled_at\s+= \$2,\s+completed_at\s+= \$3,`).
		WithArgs(string(run.RunStatusCancelled), sameInstant{now}, sameInstant{now}, "ops", "drift",
			r.RunID(), sqlmock.AnyArg(), pkgevents.DefaultTenantID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`^UPDATE runs SET terminal_task_count = \$1 WHERE tenant_id = \$2 AND run_id = \$3$`).
		WithArgs(int64(2), pkgevents.DefaultTenantID, r.RunID()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	tx, err := sqlxDB.BeginTxx(context.Background(), nil)
	require.NoError(t, err)

	if err := NewRunRepository(sqlxDB, discardLogger()).WithTx(tx).SaveRun(context.Background(), r); err != nil {
		t.Fatalf("SaveRun: %v", err)
	}
	require.NoError(t, mock.ExpectationsWereMet())

	if r.CompletedAt() == nil || !r.CompletedAt().Equal(now) {
		t.Fatalf("persisted cancel instant %v must equal aggregate completed_at %v", now, r.CompletedAt())
	}
}

// noopCancelTaskCollection is a TaskCollection whose BeginStop reports two
// pending tasks cancelled and nothing in flight; only BeginStop is invoked by
// run.Cancel in this test.
type noopCancelTaskCollection struct{ run.TaskCollection }

func (n *noopCancelTaskCollection) BeginStop(_ context.Context, _ uuid.UUID, _ string, _ time.Time) (run.StopCounts, error) {
	return run.StopCounts{Cancelled: 2}, nil
}
