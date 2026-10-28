package uow_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/state/adapters/cron"
	"github.com/carolsimone/continuo/state/domain/repository/repositorytest"
	"github.com/carolsimone/continuo/state/domain/schedule"
	"github.com/carolsimone/continuo/state/service/uow"
)

// plantedFake is the fake the contract suite drives, with a way to plant rows
// through it.
type plantedFake struct{ *uow.FakeScheduleRepository }

func (p plantedFake) Plant(_ *testing.T, row repositorytest.PlantedSchedule) {
	p.Put(uow.StoredSchedule{
		TenantID: row.TenantID, Name: row.Name, Cron: row.Cron, Timezone: row.Timezone, Paused: row.Paused,
		NextFireAt: row.NextFireAt, Version: row.Version, UpdatedBy: "contract",
	})
}

func newParser(t *testing.T) *cron.Parser {
	t.Helper()
	parser, err := cron.NewParser()
	require.NoError(t, err)
	return parser
}

// The fake holds the behaviour every ScheduleRepository shares, as the Postgres
// adapter does, so a test that runs on the fake sees what production would.
func TestFakeScheduleRepository_HoldsTheScheduleContract(t *testing.T) {
	parser := newParser(t)
	repositorytest.RunScheduleContract(t, parser, func(t *testing.T) repositorytest.ScheduleStore {
		return plantedFake{uow.NewFakeScheduleRepository(parser, func() bool { return true })}
	})
}

// LoadForUpdate locks a row for the transaction, so outside one it is an error,
// as it is in the adapter; the unit of work the repository belongs to says
// whether one is open.
func TestFakeScheduleRepository_LoadForUpdateNeedsATransaction(t *testing.T) {
	parser := newParser(t)
	u := &uow.FakeUnitOfWork{}
	repo := uow.NewFakeScheduleRepository(parser, u.InTransaction)
	repo.Put(uow.StoredSchedule{TenantID: "acme", Name: "daily", Cron: "0 * * * *", Timezone: "UTC", Version: 1})

	_, _, err := repo.LoadForUpdate(context.Background(), "acme", "daily")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "active transaction")

	require.NoError(t, u.Begin(context.Background()))
	got, ok, err := repo.LoadForUpdate(context.Background(), "acme", "daily")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "daily", got.Name())
	require.NoError(t, u.Rollback())
}

// A row with an expression of its own is rebuilt with it, whatever its cron
// says, and a save keeps it, so a test can drive a schedule with a fake
// expression through fires and saves.
func TestFakeScheduleRepository_ARowKeepsItsOwnExpression(t *testing.T) {
	ctx := context.Background()
	repo := uow.NewFakeScheduleRepository(nil, func() bool { return true })
	next := time.Date(2026, 11, 2, 1, 0, 0, 0, time.UTC)
	repo.Put(uow.StoredSchedule{
		TenantID: "acme", Name: "daily", Cron: "not a cron", Timezone: "UTC", NextFireAt: &next, Version: 3,
		UpdatedBy: "alice", Expression: everyDay{},
	})

	s, ok, err := repo.Get(ctx, "acme", "daily")
	require.NoError(t, err)
	require.True(t, ok)
	slot, _, due := s.Due(next)
	require.True(t, due)
	s.Fired(slot)
	require.NoError(t, repo.Save(ctx, s, s.Version()))

	again, ok, err := repo.Get(ctx, "acme", "daily")
	require.NoError(t, err)
	require.True(t, ok, "the saved row still reads")
	after, _ := again.NextFireAt()
	assert.True(t, next.AddDate(0, 0, 1).Equal(after), "next fire: got %s", after)
	scheduled, paused, unparsable, err := repo.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 0, 0}, []int{scheduled, paused, unparsable}, "a row with an expression of its own always parses")
}

// everyDay fires once a day, at the time of the instant it is asked from.
type everyDay struct{}

func (everyDay) Next(after time.Time) time.Time { return after.AddDate(0, 0, 1) }

var _ schedule.Expression = everyDay{}
