package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A report for a run state never created, for a task its run does not hold,
// for an attempt state never dispatched, or a stopped report of an attempt
// state never asked to stop, is permanent, so the consumer dead-letters it at
// once; every other handler result keeps its class.
func TestPermanentWhenNeverValid(t *testing.T) {
	cases := map[string]struct {
		err       error
		permanent bool
	}{
		"a task the run does not hold":   {fmt.Errorf("record attempt report: %w", run.ErrTaskNotHeld), true},
		"an attempt state never issued":  {fmt.Errorf("record attempt report: %w", run.ErrAttemptNotIssued), true},
		"an attempt never asked to stop": {fmt.Errorf("record attempt report: %w", run.ErrStopNotRequested), true},
		"a run state never created":      {fmt.Errorf("load run: %w", postgres.ErrNotFound), true},
		"any other failure":              {errors.New("connection reset"), false},
		"no failure":                     {nil, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			handle := permanentWhenNeverValid(func(context.Context, uow.UnitOfWork, events.AttemptReported, uuid.UUID) error {
				return tc.err
			})
			err := handle(context.Background(), nil, events.AttemptReported{}, uuid.Nil)
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, tc.permanent, errors.Is(err, pkgevents.ErrPermanent))
		})
	}
}

// tenantRuns holds one run under its own tenant: locking it under another
// tenant finds no row, as the runs table's (tenant_id, run_id) key does.
type tenantRuns struct {
	repository.RunRepository
	stored *run.Run
	asked  []string
	saved  bool
}

func (r *tenantRuns) LoadRunForUpdate(_ context.Context, tenantID string, runID uuid.UUID) (*run.Run, error) {
	r.asked = append(r.asked, tenantID)
	if r.stored.TenantID() != tenantID || r.stored.RunID() != runID {
		return nil, postgres.ErrNotFound
	}
	return r.stored, nil
}

func (r *tenantRuns) SaveRun(context.Context, *run.Run) error {
	r.saved = true
	return nil
}

// countingOutbox counts the events appended to it.
type countingOutbox struct{ appended int }

func (o *countingOutbox) Append(_ context.Context, evts []run.DomainEvent, _ uuid.UUID) error {
	o.appended += len(evts)
	return nil
}

// A report finds its run by the tenant its envelope names and the run id it
// carries. A report whose envelope names a tenant other than its run's finds
// no run, so the binding marks it permanent and the consumer dead-letters it
// at once; the same report under its run's tenant is applied.
func TestAttemptReport_AReportNamingAnotherTenantIsDeadLettered(t *testing.T) {
	msg := reportEntry(t, "succeeded")
	runID := uuid.MustParse("7f3e1a2b-4c5d-4e6f-8a9b-0c1d2e3f4a5b")
	now := time.Now()
	ended := run.HydrateRun(pkgevents.DefaultTenantID, runID, "daily", run.RunStatusSucceeded, run.KindCron, nil, identity.SystemUserID, model.OperationRun, now, nil, &now, nil, nil, nil, nil, nil, 0, nil)
	handler := handlers.NewAttemptReportHandler(run.TaskDefaults{MaxRetries: 2, Timeout: time.Hour}, nil, bindTestLogger())
	bind := func(runs *tenantRuns) (func(context.Context, map[string]any) error, *recordingDedupRepo) {
		u, dedup := newRecordingUnitOfWork(false)
		u.SetRunRepo(runs)
		u.SetOutboxPublisher(&countingOutbox{})
		binding := NewTaskStatusReportBinding(func() uow.UnitOfWork { return u }, handler, bindTestLogger())
		return func(ctx context.Context, values map[string]any) error {
			m := msg
			m.Values = values
			return binding(ctx, m)
		}, dedup
	}

	t.Run("another tenant", func(t *testing.T) {
		runs := &tenantRuns{stored: ended}
		deliver, dedup := bind(runs)
		values := copyValues(msg.Values)
		values["tenant_id"] = "acme"

		err := deliver(context.Background(), values)

		require.ErrorIs(t, err, pkgevents.ErrPermanent)
		require.ErrorIs(t, err, postgres.ErrNotFound)
		assert.Equal(t, []string{"acme"}, runs.asked, "the run is looked up under the envelope's tenant")
		assert.False(t, runs.saved)
		assert.Empty(t, dedup.changes, "the dedup row is not marked completed")
	})

	t.Run("the run's own tenant", func(t *testing.T) {
		runs := &tenantRuns{stored: ended}
		deliver, dedup := bind(runs)

		require.NoError(t, deliver(context.Background(), copyValues(msg.Values)))

		assert.Equal(t, []string{pkgevents.DefaultTenantID}, runs.asked)
		require.Len(t, dedup.changes, 1)
		assert.Equal(t, messageprocessing.StateCompleted, dedup.changes[0].state)
	})
}

// copyValues copies a message's fields, so a test can change one without
// changing the fixture.
func copyValues(values map[string]any) map[string]any {
	out := make(map[string]any, len(values))
	for k, v := range values {
		out[k] = v
	}
	return out
}
