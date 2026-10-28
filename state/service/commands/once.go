// Package commands makes the commands that create or cancel a run idempotent:
// a command repeated under the same request id returns the run of its first
// call instead of acting again.
package commands

import (
	"context"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/state/domain/commandrequest"
	"github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// The outcomes Once counts under continuo_command_requests_total.
const (
	outcomeCreated  = "created"
	outcomeReplayed = "replayed"
	outcomeConflict = "conflict"
	outcomeNoRun    = "no_run"
)

// maxClaimAttempts bounds how many times Once looks for a record, claims and
// loses the claim to a transaction that then rolls back. A claim is lost to
// another call of the same request, which either commits its record (the next
// look finds it) or rolls back (the next claim succeeds), so more than a
// couple of rounds means the request is being retried by many callers at once.
const maxClaimAttempts = 5

var (
	// ErrWrongRun reports a command that claimed one run and acted on another,
	// or on none. Nothing it wrote is kept.
	ErrWrongRun = errors.New("the command acted on another run than the one its request names")

	// ErrClaimContended reports a request whose claim was lost maxClaimAttempts
	// times to calls that left no record behind.
	ErrClaimContended = errors.New("the request id is claimed by calls that keep rolling back")
)

// Once runs do at most once per rec.RequestID of rec.TenantID, and returns the
// run do acted on.
//
// It looks for the request's record first. A record with rec's fingerprint is
// a repeat: Once returns the record's run with replayed true and does nothing
// else. A record with another fingerprint is a reuse of the id and returns
// commandrequest.ErrReused.
//
// With no record, Once opens a transaction, runs do in it and commits, so a
// request, the run and every other write of do land together; a do that fails
// rolls all of it back and leaves no record, so a retry is evaluated afresh.
//
// The record needs the run it names. The two shapes of command supply it
// differently:
//
//   - A command that creates a run knows the run's id up front and sets
//     rec.RunID. Once claims the record before it calls do, so two calls of one
//     request serialise on the claim: the later one waits for the earlier
//     transaction and, if that commits, finds its record and replays it without
//     running do. do must return rec.RunID, else Once rolls back and returns
//     ErrWrongRun.
//   - A command that finds its run inside the transaction (a cancel of a
//     schedule's active run) leaves rec.RunID as uuid.Nil. Once calls do, then
//     claims the record under the run do returned, still in the same
//     transaction; a claim lost to a concurrent call of the same request rolls
//     the transaction back and replays that call's record.
//
// A do that returns uuid.Nil acted on no run (a cancel of a schedule with no
// active run). Two calls of one request can both miss the first look, and the
// one that commits first can leave no run for the other's do to find (a
// cancel that ended the run at once). So Once looks for the record again in
// the same transaction, which reads what committed since: a record found
// there is that call's, and Once rolls back and replays it (or returns
// commandrequest.ErrReused for another fingerprint) as it does for a record
// found first. With no record, the record's run key cannot name a run that
// does not exist, so Once stores none: it commits the transaction (do wrote
// nothing a run would own), returns uuid.Nil with replayed false and counts
// the call no_run. A retry runs do again and finds the schedule's state as it
// then is. A command that claimed a run id up front must not return uuid.Nil.
//
// Each call is counted by command and outcome: created, replayed, conflict or
// no_run. A call that returns another error is not counted.
func Once(
	ctx context.Context,
	newUoW func() uow.UnitOfWork,
	metrics ports.RunMetrics,
	rec commandrequest.Record,
	do func(u uow.UnitOfWork) (uuid.UUID, error),
) (runID uuid.UUID, replayed bool, err error) {
	u := newUoW()
	for range maxClaimAttempts {
		stored, found, err := u.CommandRequests().Find(ctx, rec.TenantID, rec.RequestID)
		if err != nil {
			return uuid.Nil, false, err
		}
		if found {
			if stored.Fingerprint != rec.Fingerprint {
				metrics.CommandRequest(rec.Command, outcomeConflict)
				return uuid.Nil, false, commandrequest.ErrReused
			}
			metrics.CommandRequest(rec.Command, outcomeReplayed)
			return stored.RunID, true, nil
		}

		runID, claimed, err := runOnce(ctx, u, rec, do)
		if err != nil {
			return uuid.Nil, false, err
		}
		if !claimed {
			continue
		}
		if runID == uuid.Nil {
			metrics.CommandRequest(rec.Command, outcomeNoRun)
		} else {
			metrics.CommandRequest(rec.Command, outcomeCreated)
		}
		return runID, false, nil
	}
	return uuid.Nil, false, fmt.Errorf("%w: command %s, request %s", ErrClaimContended, rec.Command, rec.RequestID)
}

// runOnce runs do in one transaction of u and commits it. claimed is false,
// with the transaction rolled back and do's effects discarded, when another
// call of the request has committed its record: the claim of rec found it, or
// do acted on no run and a second look found it.
func runOnce(
	ctx context.Context,
	u uow.UnitOfWork,
	rec commandrequest.Record,
	do func(u uow.UnitOfWork) (uuid.UUID, error),
) (runID uuid.UUID, claimed bool, err error) {
	if err := u.Begin(ctx); err != nil {
		return uuid.Nil, false, fmt.Errorf("begin the command %s: %w", rec.Command, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = u.Rollback()
		}
	}()
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = u.Clock().Now()
	}

	// A command that knows its run claims first, so a concurrent call of the
	// same request waits here instead of acting twice.
	if rec.RunID != uuid.Nil {
		ok, err := u.CommandRequests().Claim(ctx, rec)
		if err != nil {
			return uuid.Nil, false, err
		}
		if !ok {
			return uuid.Nil, false, nil
		}
	}

	runID, err = do(u)
	if err != nil {
		return uuid.Nil, false, err
	}
	switch {
	case rec.RunID != uuid.Nil && runID != rec.RunID:
		return uuid.Nil, false, fmt.Errorf("%w: command %s claimed run %s and acted on %s", ErrWrongRun, rec.Command, rec.RunID, runID)
	case rec.RunID == uuid.Nil && runID != uuid.Nil:
		rec.RunID = runID
		ok, err := u.CommandRequests().Claim(ctx, rec)
		if err != nil {
			return uuid.Nil, false, err
		}
		if !ok {
			return uuid.Nil, false, nil
		}
	case rec.RunID == uuid.Nil:
		// No run to act on may mean a concurrent call of the same request
		// acted on it first: its record commits with its effect, so a record
		// this transaction now reads belongs to that call, and Once replays
		// or refuses it.
		_, found, err := u.CommandRequests().Find(ctx, rec.TenantID, rec.RequestID)
		if err != nil {
			return uuid.Nil, false, err
		}
		if found {
			return uuid.Nil, false, nil
		}
	}

	if err := u.Commit(); err != nil {
		return uuid.Nil, false, fmt.Errorf("commit the command %s: %w", rec.Command, err)
	}
	committed = true
	return runID, true, nil
}
