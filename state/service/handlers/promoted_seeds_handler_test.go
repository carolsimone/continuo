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

func seedNode(table string) events.SeedNode {
	return events.SeedNode{
		ServiceName: "core", SchemaName: "analytics", TableName: table,
		NodeType: "dbt-seed", ImageTag: "v1",
	}
}

func TestPromotedSeedsHandler_CreatesOneRunForEveryChangedSeed(t *testing.T) {
	repo, outbox := &fakePromotedSeedsRunRepo{}, &fakePromotedSeedsOutbox{}
	u := promotedSeedsUoW(repo, outbox)
	require.NoError(t, u.Begin(context.Background()))

	err := handlers.NewPromotedSeedsHandler(promotedSeedsLogger()).Handle(
		context.Background(), u,
		events.ReleaseSeedsPending{
			ReleaseID: "rel-1",
			Nodes: []events.SeedNode{
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

// The metadata the release pinned must survive onto the trigger: orchestrator
// uses it instead of the topology's current values, so a promotion overtaken by
// a later one still builds its seeds with its own image.
func TestPromotedSeedsHandler_CarriesPinnedMetadataOntoTheTrigger(t *testing.T) {
	repo, outbox := &fakePromotedSeedsRunRepo{}, &fakePromotedSeedsOutbox{}
	u := promotedSeedsUoW(repo, outbox)
	require.NoError(t, u.Begin(context.Background()))

	node := seedNode("seed_users")
	node.ImageTag = "sha-abc123"

	require.NoError(t, handlers.NewPromotedSeedsHandler(promotedSeedsLogger()).Handle(
		context.Background(), u,
		events.ReleaseSeedsPending{ReleaseID: "rel-2", Nodes: []events.SeedNode{node}},
		uuid.New(),
	))

	evt := outbox.appended[0].(run.PromotedSeedsRunRequested)
	require.Len(t, evt.Nodes, 1)
	assert.Equal(t, "sha-abc123", evt.Nodes[0].ImageTag)
	assert.Equal(t, "dbt-seed", evt.Nodes[0].NodeType)
}

// The run id is derived from the release id, so a redelivered release.promoted:v1
// resolves to the run that already exists rather than minting a second one and
// rebuilding seeds that are already built.
func TestPromotedSeedsRunID_IsDeterministicPerRelease(t *testing.T) {
	assert.Equal(t, handlers.PromotedSeedsRunID("rel-1"), handlers.PromotedSeedsRunID("rel-1"))
	assert.NotEqual(t, handlers.PromotedSeedsRunID("rel-1"), handlers.PromotedSeedsRunID("rel-2"))
	assert.NotEqual(t, uuid.Nil, handlers.PromotedSeedsRunID("rel-1"))
}
