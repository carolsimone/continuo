package commands_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/domain/commandrequest"
	"github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/service/commands"
	"github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	tenant  = "acme"
	command = "start_run"
)

// outcomeCounter records the command_request outcomes Once counts, as
// "command/outcome" keys.
type outcomeCounter struct {
	ports.RunMetrics
	mu     sync.Mutex
	counts map[string]int
}

func newOutcomeCounter() *outcomeCounter { return &outcomeCounter{counts: map[string]int{}} }

func (c *outcomeCounter) CommandRequest(cmd, outcome string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[cmd+"/"+outcome]++
}

func (c *outcomeCounter) count(outcome string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[command+"/"+outcome]
}

// memoryRequests is an in-memory CommandRequestRepository. claims scripts the
// answers of Claim, in call order; once they are spent Claim inserts like a
// table does. onFind runs on every Find.
type memoryRequests struct {
	rows   map[uuid.UUID]commandrequest.Record
	claims []bool
	nClaim int
	nFind  int
	onFind func(m *memoryRequests, call int)
}

var _ repository.CommandRequestRepository = (*memoryRequests)(nil)

func newMemoryRequests() *memoryRequests {
	return &memoryRequests{rows: map[uuid.UUID]commandrequest.Record{}}
}

func (m *memoryRequests) Find(_ context.Context, _ string, id uuid.UUID) (commandrequest.Record, bool, error) {
	m.nFind++
	if m.onFind != nil {
		m.onFind(m, m.nFind)
	}
	rec, ok := m.rows[id]
	return rec, ok, nil
}

func (m *memoryRequests) Claim(_ context.Context, rec commandrequest.Record) (bool, error) {
	m.nClaim++
	if len(m.claims) > 0 {
		answer := m.claims[0]
		m.claims = m.claims[1:]
		if !answer {
			return false, nil
		}
	} else if _, exists := m.rows[rec.RequestID]; exists {
		return false, nil
	}
	m.rows[rec.RequestID] = rec
	return true, nil
}

func (m *memoryRequests) DeleteOlderThan(context.Context, time.Duration) (int64, error) {
	return 0, errors.New("not used")
}

func fakeUoW(requests *memoryRequests) (func() uow.UnitOfWork, *uow.FakeUnitOfWork) {
	u := &uow.FakeUnitOfWork{CommandRequestRepo: requests}
	return func() uow.UnitOfWork { return u }, u
}

func newRecord(runID uuid.UUID) commandrequest.Record {
	return commandrequest.Record{
		TenantID:    tenant,
		RequestID:   uuid.New(),
		Command:     command,
		Fingerprint: commandrequest.Fingerprint(command, "daily", "run"),
		RunID:       runID,
	}
}

// A command whose do returns no run (a cancel of a schedule with no active
// run) stores no record, because a record names a run its foreign key checks;
// the transaction commits what do wrote (nothing), the call is counted
// no_run, and a retry runs do again.
func TestOnce_ACommandThatActsOnNoRunStoresNoRecord(t *testing.T) {
	requests := newMemoryRequests()
	newUoW, u := fakeUoW(requests)
	metrics := newOutcomeCounter()
	rec := newRecord(uuid.Nil)
	calls := 0
	do := func(uow.UnitOfWork) (uuid.UUID, error) { calls++; return uuid.Nil, nil }

	for i := 1; i <= 2; i++ {
		runID, replayed, err := commands.Once(context.Background(), newUoW, metrics, rec, do)
		require.NoError(t, err)
		assert.Equal(t, uuid.Nil, runID)
		assert.False(t, replayed)
		assert.Equal(t, i, calls, "no record, so a retry runs the command again")
	}
	assert.Empty(t, requests.rows)
	assert.Zero(t, requests.nClaim, "nothing is claimed for a command with no run")
	assert.Equal(t, 2, metrics.count("no_run"))
	assert.Equal(t, 2, u.CommitCalled)
	assert.Zero(t, u.RollbackCalled)
}

// A command that finds its run inside do (cancel by schedule name) is
// recorded under that run after do returns, and a retry replays it.
func TestOnce_ACommandThatFindsItsRunIsRecordedUnderIt(t *testing.T) {
	requests := newMemoryRequests()
	newUoW, _ := fakeUoW(requests)
	metrics := newOutcomeCounter()
	found := uuid.New()
	rec := newRecord(uuid.Nil)
	calls := 0
	do := func(uow.UnitOfWork) (uuid.UUID, error) { calls++; return found, nil }

	runID, replayed, err := commands.Once(context.Background(), newUoW, metrics, rec, do)
	require.NoError(t, err)
	assert.Equal(t, found, runID)
	assert.False(t, replayed)
	assert.Equal(t, found, requests.rows[rec.RequestID].RunID)

	runID, replayed, err = commands.Once(context.Background(), newUoW, metrics, rec, do)
	require.NoError(t, err)
	assert.Equal(t, found, runID)
	assert.True(t, replayed)
	assert.Equal(t, 1, calls)
	assert.Equal(t, 1, metrics.count("created"))
	assert.Equal(t, 1, metrics.count("replayed"))
}

// A command that claimed a run acts on that run: a do that returns another
// run, or none, rolls everything back and stores nothing.
func TestOnce_ADoThatActsOnAnotherRunThanClaimedIsRefused(t *testing.T) {
	for name, returned := range map[string]uuid.UUID{"another run": uuid.New(), "no run": uuid.Nil} {
		t.Run(name, func(t *testing.T) {
			requests := newMemoryRequests()
			newUoW, u := fakeUoW(requests)
			rec := newRecord(uuid.New())

			_, _, err := commands.Once(context.Background(), newUoW, newOutcomeCounter(), rec,
				func(uow.UnitOfWork) (uuid.UUID, error) { return returned, nil })

			require.ErrorIs(t, err, commands.ErrWrongRun)
			assert.Equal(t, 1, u.RollbackCalled)
			assert.Zero(t, u.CommitCalled)
		})
	}
}

// A claim another transaction beat, whose record then appears, is a retry of
// that request: Once replays the record's run without running do.
func TestOnce_ALostClaimReplaysTheWinnersRun(t *testing.T) {
	requests := newMemoryRequests()
	winner := newRecord(uuid.New())
	requests.claims = []bool{false}
	requests.onFind = func(m *memoryRequests, call int) {
		if call == 2 { // the winner commits between the lost claim and the second look
			m.rows[winner.RequestID] = winner
		}
	}
	newUoW, u := fakeUoW(requests)
	metrics := newOutcomeCounter()
	rec := winner
	rec.RunID = uuid.New()

	runID, replayed, err := commands.Once(context.Background(), newUoW, metrics, rec,
		func(uow.UnitOfWork) (uuid.UUID, error) {
			t.Fatal("do must not run for a lost claim")
			return uuid.Nil, nil
		})

	require.NoError(t, err)
	assert.Equal(t, winner.RunID, runID)
	assert.True(t, replayed)
	assert.Equal(t, 1, u.RollbackCalled, "the lost claim's transaction is rolled back")
	assert.Equal(t, 1, metrics.count("replayed"))
}

// A claim that keeps inserting nothing while no record can be read cannot be
// retried for ever: Once gives up with an error and writes nothing.
func TestOnce_AClaimThatKeepsLosingGivesUp(t *testing.T) {
	requests := newMemoryRequests()
	requests.claims = []bool{false, false, false, false, false, false, false, false}
	newUoW, u := fakeUoW(requests)

	_, _, err := commands.Once(context.Background(), newUoW, newOutcomeCounter(), newRecord(uuid.New()),
		func(uow.UnitOfWork) (uuid.UUID, error) { t.Fatal("do must not run"); return uuid.Nil, nil })

	require.ErrorIs(t, err, commands.ErrClaimContended)
	assert.Zero(t, u.CommitCalled)
	assert.Empty(t, requests.rows)
}

func TestOnce_ARecordWithTheSameFingerprintReplaysAndAnotherIsRefused(t *testing.T) {
	requests := newMemoryRequests()
	newUoW, u := fakeUoW(requests)
	metrics := newOutcomeCounter()
	stored := newRecord(uuid.New())
	requests.rows[stored.RequestID] = stored

	runID, replayed, err := commands.Once(context.Background(), newUoW, metrics, stored,
		func(uow.UnitOfWork) (uuid.UUID, error) { t.Fatal("do must not run"); return uuid.Nil, nil })
	require.NoError(t, err)
	assert.True(t, replayed)
	assert.Equal(t, stored.RunID, runID)

	other := stored
	other.Fingerprint = commandrequest.Fingerprint(command, "weekly", "run")
	_, _, err = commands.Once(context.Background(), newUoW, metrics, other,
		func(uow.UnitOfWork) (uuid.UUID, error) { t.Fatal("do must not run"); return uuid.Nil, nil })
	require.ErrorIs(t, err, commandrequest.ErrReused)
	assert.Equal(t, 1, metrics.count("replayed"))
	assert.Equal(t, 1, metrics.count("conflict"))
	assert.Zero(t, u.BeginCalled, "a replay or a refusal opens no transaction")
}

// A do that fails rolls back, stores nothing and counts nothing, so a retry is
// evaluated afresh.
func TestOnce_AFailingDoStoresNothingAndCountsNothing(t *testing.T) {
	requests := newMemoryRequests()
	newUoW, u := fakeUoW(requests)
	metrics := newOutcomeCounter()
	boom := errors.New("no such node")
	rec := newRecord(uuid.New())

	_, _, err := commands.Once(context.Background(), newUoW, metrics, rec,
		func(uow.UnitOfWork) (uuid.UUID, error) { return uuid.Nil, boom })

	require.ErrorIs(t, err, boom)
	assert.Equal(t, 1, u.RollbackCalled)
	assert.Zero(t, u.CommitCalled)
	assert.Empty(t, metrics.counts)
}
