//go:build integration

package commands_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/adapters/postgres"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/commandrequest"
	"github.com/carolsimone/continuo/run-controller/service/commands"
	"github.com/carolsimone/continuo/run-controller/service/ports"
	"github.com/carolsimone/continuo/run-controller/service/uow"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// onceFixture is a real run-controller database, a tenant no other test uses (its runs,
// and with them its command requests, are deleted when the test ends) and the
// counter Once reports to.
type onceFixture struct {
	db      *sqlx.DB
	tenant  string
	metrics *outcomeCounter
}

func newOnceFixture(t *testing.T) *onceFixture {
	t.Helper()
	db, err := pkgdb.Open(context.Background(), pkgconfig.LoadPostgres(&pkgconfig.Validator{}), pkgconfig.PoolConfig{MaxOpenConns: 10, MaxIdleConns: 2})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	f := &onceFixture{db: db, tenant: "t-" + uuid.NewString(), metrics: newOutcomeCounter()}
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM runs WHERE tenant_id = $1`, f.tenant) })
	return f
}

func (f *onceFixture) newUoW() uow.UnitOfWork {
	return postgres.NewPostgresUnitOfWork(f.db, ports.SystemClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
}

// pending builds the pending run a start command would create for schedule.
func (f *onceFixture) pending(t *testing.T, schedule string) *run.Run {
	t.Helper()
	r, err := run.NewPendingRun(f.tenant, uuid.New(), schedule, run.KindTrigger, nil, "tester", model.OperationRun, run.ReleaseStamp{}, time.Now())
	require.NoError(t, err)
	return r
}

// record is the request a start command of r makes under requestID.
func (f *onceFixture) record(requestID uuid.UUID, r *run.Run, parts ...string) commandrequest.Record {
	return commandrequest.Record{
		TenantID:    f.tenant,
		RequestID:   requestID,
		Command:     command,
		Fingerprint: commandrequest.Fingerprint(command, parts...),
		RunID:       r.RunID(),
	}
}

// create is the do of a start command: it saves r.
func create(r *run.Run, calls *int) func(uow.UnitOfWork) (uuid.UUID, error) {
	return func(u uow.UnitOfWork) (uuid.UUID, error) {
		*calls++
		if err := u.Run().SaveRun(context.Background(), r); err != nil {
			return uuid.Nil, err
		}
		return r.RunID(), nil
	}
}

func (f *onceFixture) runCount(t *testing.T, schedule string) int {
	t.Helper()
	var n int
	require.NoError(t, f.db.Get(&n, `SELECT count(*) FROM runs WHERE tenant_id = $1 AND schedule_name = $2`, f.tenant, schedule))
	return n
}

func (f *onceFixture) requestCount(t *testing.T) int {
	t.Helper()
	var n int
	require.NoError(t, f.db.Get(&n, `SELECT count(*) FROM command_requests WHERE tenant_id = $1`, f.tenant))
	return n
}

func TestOnce_FirstCallCreatesTheRun(t *testing.T) {
	f := newOnceFixture(t)
	r := f.pending(t, "first-call")
	id := uuid.New()
	calls := 0

	runID, replayed, err := commands.Once(context.Background(), f.newUoW, f.metrics, f.record(id, r, "first-call", "run"), create(r, &calls))

	require.NoError(t, err)
	assert.Equal(t, r.RunID(), runID)
	assert.False(t, replayed)
	assert.Equal(t, 1, calls)
	assert.Equal(t, 1, f.runCount(t, "first-call"))
	assert.Equal(t, 1, f.requestCount(t))
	assert.Equal(t, 1, f.metrics.count("created"))
}

func TestOnce_ARetryReturnsTheRunAndWritesNothing(t *testing.T) {
	f := newOnceFixture(t)
	r := f.pending(t, "retry")
	id := uuid.New()
	calls := 0
	_, _, err := commands.Once(context.Background(), f.newUoW, f.metrics, f.record(id, r, "retry", "run"), create(r, &calls))
	require.NoError(t, err)

	again := f.pending(t, "retry")
	runID, replayed, err := commands.Once(context.Background(), f.newUoW, f.metrics, f.record(id, again, "retry", "run"), create(again, &calls))

	require.NoError(t, err)
	assert.Equal(t, r.RunID(), runID, "the retry returns the first call's run")
	assert.True(t, replayed)
	assert.Equal(t, 1, calls, "the command did not run again")
	assert.Equal(t, 1, f.runCount(t, "retry"))
	assert.Equal(t, 1, f.requestCount(t))
	assert.Equal(t, 1, f.metrics.count("replayed"))
}

func TestOnce_ReuseWithOtherParametersIsRefused(t *testing.T) {
	f := newOnceFixture(t)
	r := f.pending(t, "reuse-a")
	id := uuid.New()
	calls := 0
	_, _, err := commands.Once(context.Background(), f.newUoW, f.metrics, f.record(id, r, "reuse-a", "run"), create(r, &calls))
	require.NoError(t, err)

	other := f.pending(t, "reuse-b")
	_, _, err = commands.Once(context.Background(), f.newUoW, f.metrics, f.record(id, other, "reuse-b", "run"), create(other, &calls))

	require.ErrorIs(t, err, commandrequest.ErrReused)
	assert.Equal(t, 1, calls)
	assert.Zero(t, f.runCount(t, "reuse-b"))
	assert.Equal(t, 1, f.metrics.count("conflict"))
}

// Two identical calls at once: the second's claim waits for the first's
// uncommitted record and, once the first commits, inserts nothing and returns
// the first's run, so one run exists and the second's command never ran.
func TestOnce_TwoIdenticalCallsAtOnceMakeOneRun(t *testing.T) {
	f := newOnceFixture(t)
	id := uuid.New()
	runs := []*run.Run{f.pending(t, "at-once"), f.pending(t, "at-once")}
	var calls [2]int
	var ids [2]uuid.UUID
	var replayed [2]bool
	var errs [2]error

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range runs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ids[i], replayed[i], errs[i] = commands.Once(context.Background(), f.newUoW, f.metrics, f.record(id, runs[i], "at-once", "run"), create(runs[i], &calls[i]))
		}()
	}
	close(start)
	wg.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	assert.Equal(t, ids[0], ids[1], "both calls return the same run")
	assert.Equal(t, 1, calls[0]+calls[1], "the command ran once")
	assert.NotEqual(t, replayed[0], replayed[1], "one call created the run, the other replayed it")
	assert.Equal(t, 1, f.runCount(t, "at-once"))
	assert.Equal(t, 1, f.requestCount(t))
	assert.Equal(t, 1, f.metrics.count("created"))
	assert.Equal(t, 1, f.metrics.count("replayed"))
}

// A refused call (the command fails) rolls back its record with everything
// else, so a retry is evaluated afresh instead of replaying a failure.
func TestOnce_ARefusedCallStoresNothing(t *testing.T) {
	f := newOnceFixture(t)
	id := uuid.New()
	r := f.pending(t, "refused")
	refuse := errors.New("no such node")

	_, _, err := commands.Once(context.Background(), f.newUoW, f.metrics, f.record(id, r, "refused", "run"),
		func(uow.UnitOfWork) (uuid.UUID, error) { return uuid.Nil, refuse })
	require.ErrorIs(t, err, refuse)
	assert.Zero(t, f.requestCount(t))
	assert.Zero(t, f.runCount(t, "refused"))

	calls := 0
	runID, replayed, err := commands.Once(context.Background(), f.newUoW, f.metrics, f.record(id, r, "refused", "run"), create(r, &calls))
	require.NoError(t, err)
	assert.Equal(t, r.RunID(), runID)
	assert.False(t, replayed)
	assert.Equal(t, 1, calls)
}

// A command that ends up acting on no run has nothing for the record's run key
// to point at: no record is stored, the call is counted no_run, and a retry
// runs the command again.
func TestOnce_ACommandThatActsOnNoRunStoresNoRecordInPostgres(t *testing.T) {
	f := newOnceFixture(t)
	rec := commandrequest.Record{TenantID: f.tenant, RequestID: uuid.New(), Command: "cancel_active_run",
		Fingerprint: commandrequest.Fingerprint("cancel_active_run", "idle-schedule")}
	calls := 0
	do := func(uow.UnitOfWork) (uuid.UUID, error) { calls++; return uuid.Nil, nil }

	for i := 1; i <= 2; i++ {
		runID, replayed, err := commands.Once(context.Background(), f.newUoW, f.metrics, rec, do)
		require.NoError(t, err)
		assert.Equal(t, uuid.Nil, runID)
		assert.False(t, replayed)
		assert.Equal(t, i, calls)
	}
	assert.Zero(t, f.requestCount(t))
	assert.Equal(t, 2, f.metrics.counts["cancel_active_run/no_run"])
}

// A command that finds its run inside the transaction is recorded under it.
func TestOnce_ACommandThatFindsItsRunIsRecordedUnderItInPostgres(t *testing.T) {
	f := newOnceFixture(t)
	r := f.pending(t, "find-run")
	calls := 0
	_, _, err := commands.Once(context.Background(), f.newUoW, f.metrics, f.record(uuid.New(), r, "x"), create(r, &calls))
	require.NoError(t, err)

	rec := commandrequest.Record{TenantID: f.tenant, RequestID: uuid.New(), Command: "cancel_active_run",
		Fingerprint: commandrequest.Fingerprint("cancel_active_run", "find-run")}
	do := func(uow.UnitOfWork) (uuid.UUID, error) { calls++; return r.RunID(), nil }

	runID, replayed, err := commands.Once(context.Background(), f.newUoW, f.metrics, rec, do)
	require.NoError(t, err)
	assert.Equal(t, r.RunID(), runID)
	assert.False(t, replayed)
	runID, replayed, err = commands.Once(context.Background(), f.newUoW, f.metrics, rec, do)
	require.NoError(t, err)
	assert.Equal(t, r.RunID(), runID)
	assert.True(t, replayed)
	assert.Equal(t, 2, calls, "the create and one cancel; the retry did not run")
}

// Two cancels of one request both miss the first look. The second's do has
// already read in its transaction when the first cancels the run and commits
// its record; the second's do then acts on no run. The second looks for the
// record again in its own transaction, sees the first's record, committed
// after that transaction's first statement, and returns the first's run as a
// replay instead of settling as no_run.
func TestOnce_ANoRunResultReplaysTheRecordAConcurrentCallCommittedInPostgres(t *testing.T) {
	f := newOnceFixture(t)
	r := f.pending(t, "cancel-race")
	calls := 0
	_, _, err := commands.Once(context.Background(), f.newUoW, f.metrics, f.record(uuid.New(), r, "cancel-race", "run"), create(r, &calls))
	require.NoError(t, err)

	rec := commandrequest.Record{TenantID: f.tenant, RequestID: uuid.New(), Command: "cancel_active_run",
		Fingerprint: commandrequest.Fingerprint("cancel_active_run", "cancel-race")}
	entered := make(chan struct{})
	release := make(chan struct{})
	type result struct {
		runID    uuid.UUID
		replayed bool
		err      error
	}
	second := make(chan result, 1)
	go func() {
		runID, replayed, err := commands.Once(context.Background(), f.newUoW, f.metrics, rec,
			func(u uow.UnitOfWork) (uuid.UUID, error) {
				// The cancel's own read is the transaction's first statement,
				// so the recheck that follows it must see a record committed
				// after it: each statement reads what has committed by then.
				active, err := u.Run().GetActiveRun(context.Background(), f.tenant, "cancel-race")
				if err != nil {
					return uuid.Nil, err
				}
				if active == nil {
					return uuid.Nil, errors.New("the run is active before the first cancel")
				}
				close(entered) // the first look has missed
				<-release      // the first cancel has committed
				return uuid.Nil, nil
			})
		second <- result{runID, replayed, err}
	}()

	<-entered
	runID, replayed, err := commands.Once(context.Background(), f.newUoW, f.metrics, rec,
		func(uow.UnitOfWork) (uuid.UUID, error) { return r.RunID(), nil })
	require.NoError(t, err)
	assert.Equal(t, r.RunID(), runID)
	assert.False(t, replayed)
	close(release)

	got := <-second
	require.NoError(t, got.err)
	assert.Equal(t, r.RunID(), got.runID, "the second call returns the run the first cancelled")
	assert.True(t, got.replayed)
	assert.Equal(t, 1, f.metrics.counts["cancel_active_run/created"])
	assert.Equal(t, 1, f.metrics.counts["cancel_active_run/replayed"])
	assert.Zero(t, f.metrics.counts["cancel_active_run/no_run"])
	assert.Equal(t, 2, f.requestCount(t), "the start's record and the first cancel's")
}
