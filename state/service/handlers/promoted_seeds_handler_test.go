package handlers_test

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakePromotedSeedsRunRepo captures the run the handler creates.
type fakePromotedSeedsRunRepo struct {
	saved []*run.Run
}

func (f *fakePromotedSeedsRunRepo) SaveRun(_ context.Context, r *run.Run) error {
	f.saved = append(f.saved, r)
	return nil
}
func (f *fakePromotedSeedsRunRepo) LoadRunForUpdate(_ context.Context, _ uuid.UUID) (*run.Run, error) {
	panic("LoadRunForUpdate not implemented in fake")
}
func (f *fakePromotedSeedsRunRepo) GetRun(_ context.Context, _ uuid.UUID) (*run.Run, error) {
	panic("GetRun not implemented in fake")
}
func (f *fakePromotedSeedsRunRepo) HasActiveSchedule(_ context.Context, _ string) (bool, error) {
	panic("HasActiveSchedule not implemented in fake")
}
func (f *fakePromotedSeedsRunRepo) GetActiveScheduler(_ context.Context, _ string) (*run.Run, error) {
	panic("GetActiveScheduler not implemented in fake")
}
func (f *fakePromotedSeedsRunRepo) GetLastRunPerSchedule(_ context.Context) (map[string]repository.LastRunSummary, error) {
	panic("GetLastRunPerSchedule not implemented in fake")
}

type fakePromotedSeedsOutbox struct {
	appended []run.DomainEvent
}

func (f *fakePromotedSeedsOutbox) Append(_ context.Context, evts []run.DomainEvent, _ uuid.UUID) error {
	f.appended = append(f.appended, evts...)
	return nil
}

func promotedSeedsUoW(repo *fakePromotedSeedsRunRepo, outbox *fakePromotedSeedsOutbox) *uow.FakeUnitOfWork {
	u := &uow.FakeUnitOfWork{}
	u.SetRunRepo(repo)
	u.SetOutboxPublisher(outbox)
	return u
}

func promotedSeedsLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func seedNode(table string) events.ReleasePromotedNode {
	return events.ReleasePromotedNode{
		ServiceName: "core", SchemaName: "analytics", TableName: table,
		NodeType: events.NodeTypeDBTSeed, Changed: true,
	}
}

func TestPromotedSeedsHandler_CreatesOneRunForEveryChangedSeed(t *testing.T) {
	repo, outbox := &fakePromotedSeedsRunRepo{}, &fakePromotedSeedsOutbox{}
	u := promotedSeedsUoW(repo, outbox)
	require.NoError(t, u.Begin(context.Background()))

	err := handlers.NewPromotedSeedsHandler(promotedSeedsLogger()).Handle(
		context.Background(), u,
		events.ReleasePromoted{
			ReleaseID: "rel-1",
			Topology: []events.ReleasePromotedNode{
				seedNode("seed_users"),
				seedNode("seed_fx_transactions"),
			},
		},
		uuid.New(),
	)
	require.NoError(t, err)

	require.Len(t, repo.saved, 1, "one run carries all of the release's seeds")
	created := repo.saved[0]
	assert.Equal(t, run.KindPromoteSeed, created.Kind())
	assert.Equal(t, run.SchedulerStatusRunning, created.Status())

	require.Len(t, outbox.appended, 1)
	evt, ok := outbox.appended[0].(run.PromotedSeedsRunRequested)
	require.True(t, ok, "expected PromotedSeedsRunRequested, got %T", outbox.appended[0])
	assert.Equal(t, "rel-1", evt.ReleaseID)
	assert.Len(t, evt.Nodes, 2, "both seeds travel on the event")
	assert.Equal(t, created.ScheduleID(), evt.ID)
}

// Only changed seeds are built. An unchanged seed's data is already materialised
// and its content hash has not moved, so rebuilding it would cost a Job for no
// effect; a model is not this path's work at all.
func TestPromotedSeedsHandler_SkipsUnchangedSeedsAndModels(t *testing.T) {
	repo, outbox := &fakePromotedSeedsRunRepo{}, &fakePromotedSeedsOutbox{}
	u := promotedSeedsUoW(repo, outbox)
	require.NoError(t, u.Begin(context.Background()))

	unchangedSeed := seedNode("seed_old")
	unchangedSeed.Changed = false
	changedModel := events.ReleasePromotedNode{
		ServiceName: "core", SchemaName: "analytics", TableName: "daily_transactions",
		NodeType: "dbt-model", Changed: true,
	}

	err := handlers.NewPromotedSeedsHandler(promotedSeedsLogger()).Handle(
		context.Background(), u,
		events.ReleasePromoted{
			ReleaseID: "rel-2",
			Topology:  []events.ReleasePromotedNode{unchangedSeed, changedModel, seedNode("seed_new")},
		},
		uuid.New(),
	)
	require.NoError(t, err)

	require.Len(t, repo.saved, 1)
	evt := outbox.appended[0].(run.PromotedSeedsRunRequested)
	require.Len(t, evt.Nodes, 1)
	assert.Equal(t, "seed_new", evt.Nodes[0].TableName)
}

// A promotion that changed no seeds must not create a run at all. An empty run
// would appear in the UI on every model-only release and could never reach a
// terminal state, having no tasks to finish.
func TestPromotedSeedsHandler_NoChangedSeeds_CreatesNothing(t *testing.T) {
	repo, outbox := &fakePromotedSeedsRunRepo{}, &fakePromotedSeedsOutbox{}
	u := promotedSeedsUoW(repo, outbox)
	require.NoError(t, u.Begin(context.Background()))

	err := handlers.NewPromotedSeedsHandler(promotedSeedsLogger()).Handle(
		context.Background(), u,
		events.ReleasePromoted{
			ReleaseID: "rel-3",
			Topology: []events.ReleasePromotedNode{{
				ServiceName: "core", SchemaName: "analytics", TableName: "daily_transactions",
				NodeType: "dbt-model", Changed: true,
			}},
		},
		uuid.New(),
	)
	require.NoError(t, err)

	assert.Empty(t, repo.saved, "no run for a promotion with no changed seeds")
	assert.Empty(t, outbox.appended, "and therefore no trigger event")
}

// The run id is derived from the release id, so a redelivered release.promoted:v1
// resolves to the run that already exists rather than minting a second one and
// rebuilding seeds that are already built.
func TestPromotedSeedsRunID_IsDeterministicPerRelease(t *testing.T) {
	assert.Equal(t, handlers.PromotedSeedsRunID("rel-1"), handlers.PromotedSeedsRunID("rel-1"))
	assert.NotEqual(t, handlers.PromotedSeedsRunID("rel-1"), handlers.PromotedSeedsRunID("rel-2"))
	assert.NotEqual(t, uuid.Nil, handlers.PromotedSeedsRunID("rel-1"))
}
