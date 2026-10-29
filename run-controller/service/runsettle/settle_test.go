package runsettle_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/run-controller/domain/repository"
	"github.com/carolsimone/continuo/run-controller/service/runsettle"
	"github.com/carolsimone/continuo/run-controller/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var now = time.Date(2026, 10, 27, 9, 0, 0, 0, time.UTC)

type fixedClock struct{}

func (fixedClock) Now() time.Time { return now }

// unplannedRuns lists the unplanned active runs it holds and serves each for
// update only under its own tenant; it records every load and save.
type unplannedRuns struct {
	repository.RunRepository
	held  []*run.Run
	loads []repository.RunRef
	saved []repository.RunRef
}

func (s *unplannedRuns) ListUnplannedActive(context.Context) ([]repository.RunRef, error) {
	out := make([]repository.RunRef, 0, len(s.held))
	for _, r := range s.held {
		out = append(out, repository.RunRef{TenantID: r.TenantID(), RunID: r.RunID()})
	}
	return out, nil
}

func (s *unplannedRuns) LoadRunForUpdate(_ context.Context, tenantID string, id uuid.UUID) (*run.Run, error) {
	s.loads = append(s.loads, repository.RunRef{TenantID: tenantID, RunID: id})
	for _, r := range s.held {
		if r.TenantID() == tenantID && r.RunID() == id {
			return r, nil
		}
	}
	return nil, fmt.Errorf("run %s of tenant %s not found", id, tenantID)
}

func (s *unplannedRuns) SaveRun(_ context.Context, r *run.Run) error {
	s.saved = append(s.saved, repository.RunRef{TenantID: r.TenantID(), RunID: r.RunID()})
	return nil
}

// idleTasks is a run's tasks with none in flight: a cancel ends them at once.
type idleTasks struct{ run.TaskCollection }

func (idleTasks) BeginStop(context.Context, uuid.UUID, string, time.Time) (run.StopCounts, error) {
	return run.StopCounts{}, nil
}

type recordingOutbox struct{ events []run.DomainEvent }

func (o *recordingOutbox) Append(_ context.Context, events []run.DomainEvent, _ uuid.UUID) error {
	o.events = append(o.events, events...)
	return nil
}

// unplannedRun is a running run of tenantID whose tasks state did not plan.
func unplannedRun(tenantID string) *run.Run {
	started := now.Add(-time.Hour)
	return run.HydrateRun(tenantID, uuid.New(), "daily", run.RunStatusRunning, run.KindCron, nil, "system",
		model.OperationRun, started, &started, nil, nil, nil, nil, nil, nil, 0)
}

// Settle cancels the unplanned runs of every tenant, each under its own
// tenant: the run is loaded, and its tasks stopped, with the tenant the list
// named it with.
func TestRunSettle_CancelsWithTheRunsTenant(t *testing.T) {
	ours, theirs := unplannedRun("default"), unplannedRun("acme")
	runs := &unplannedRuns{held: []*run.Run{ours, theirs}}
	outbox := &recordingOutbox{}
	var uows []*uow.FakeUnitOfWork
	newUoW := func() uow.UnitOfWork {
		u := &uow.FakeUnitOfWork{}
		u.SetRunRepo(runs)
		u.SetTaskCollection(idleTasks{})
		u.SetOutboxPublisher(outbox)
		u.SetClock(fixedClock{})
		uows = append(uows, u)
		return u
	}

	n, err := runsettle.Settle(context.Background(), newUoW, slog.New(slog.NewTextHandler(io.Discard, nil)))

	require.NoError(t, err)
	assert.Equal(t, 2, n, "the unplanned runs of both tenants are cancelled")
	want := []repository.RunRef{{TenantID: "default", RunID: ours.RunID()}, {TenantID: "acme", RunID: theirs.RunID()}}
	assert.Equal(t, want, runs.loads, "each run is loaded under its own tenant")
	assert.Equal(t, want, runs.saved)
	assert.Equal(t, run.RunStatusCancelled, ours.Status())
	assert.Equal(t, run.RunStatusCancelled, theirs.Status())
	require.Len(t, uows, 3, "one unit of work lists the runs, one cancels each")
	assert.Equal(t, []string{"default"}, uows[1].TaskCollectionTenants)
	assert.Equal(t, []string{"acme"}, uows[2].TaskCollectionTenants, "each run's tasks are stopped under its own tenant")
	require.Len(t, outbox.events, 4, "each cancel writes its run.cancelled and its finalization")
}
