// Package backup backs up each tenant's catalog graph to object storage on a
// schedule, keeps the newest backups, and restores a backup. A backup or a
// restore holds the tenant's backup lease while it reads or writes the graph,
// so no catalog write lands meanwhile. The lease is renewed while the work
// makes progress: it expires by itself if the process dies, and work that
// moves nothing for stallLimit is stopped and releases it, so neither a
// crashed nor a stuck holder pauses the tenant's writes for longer than
// about a lease's ttl.
package backup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/carolsimone/continuo/catalog/service/graphbackup"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/google/uuid"
)

const (
	// leaseTTL is how long a lease lasts past its last renewal, so a holder
	// that crashed pauses the tenant's writes for at most this long.
	leaseTTL = 2 * time.Minute
	// leaseRenewEvery is how often a running backup or restore renews its
	// lease: a renewal that fails is retried at the next tick, three times
	// before the lease can lapse.
	leaseRenewEvery = 30 * time.Second
	// releaseTimeout bounds the release of a lease, which runs even when the
	// work's context was cancelled.
	releaseTimeout = 10 * time.Second
	// stallLimit is how long a backup or a restore may move no record,
	// counted afresh at the start of each of its steps, before it is stopped:
	// a holder that is stuck pauses the tenant's writes no longer than one
	// that crashed.
	stallLimit = leaseTTL
	// renewTimeout bounds one renewal, so a database that never answers does
	// not keep the stall from being noticed.
	renewTimeout = 10 * time.Second
	// inFlightRetryEvery is how often a backup that was refused because a
	// message's writes are in flight asks for the lease again.
	inFlightRetryEvery = 2 * time.Second
)

// ErrStalled means a backup or a restore moved no record for stallLimit and
// was stopped, so its lease does not keep pausing the tenant's writes.
var ErrStalled = errors.New("no record moved for the stall limit")

// ErrSkipped means the backup or restore did not start: another backup or a
// restore holds the tenant's lease.
var ErrSkipped = errors.New("another backup or restore holds the tenant's lease")

// Result is one stored backup: its key, the size the store keeps, the
// records it holds and how long it held the tenant's lease, the time the
// tenant's writes were paused.
type Result struct {
	ObjectKey     string
	Bytes         int64
	Nodes         int64
	Relationships int64
	Duration      time.Duration
}

// Taker backs up one tenant's graph at a time, streaming its records into
// the backup store, and keeps the tenant's newest backups.
type Taker struct {
	keeper  leaseKeeper
	graph   ports.GraphSaver
	store   ports.BackupStore
	metrics ports.BackupMetrics
	keep    int
	clock   func() time.Time
	logger  *slog.Logger
}

// NewTaker returns an Taker that keeps the newest keep backups of each
// tenant. clock dates each backup.
func NewTaker(lease ports.BackupLease, graph ports.GraphSaver, store ports.BackupStore, metrics ports.BackupMetrics, keep int, clock func() time.Time, logger *slog.Logger) *Taker {
	return &Taker{
		keeper: newLeaseKeeper(lease, logger),
		graph:  graph, store: store, metrics: metrics, keep: keep, clock: clock, logger: logger,
	}
}

// TakeTenant backups tenantID's graph. It opens the backup first, so a
// store that refuses writes fails the backup before the tenant's writes are
// paused; then it takes the tenant's backup lease, streams the graph's
// records into the backup, confirms the lease is still held, completes the
// backup and releases the lease. A held lease skips the backup
// (ErrSkipped). Any failure aborts the backup, so nothing is stored, and
// releases the lease. After a stored backup it deletes the tenant's backups
// beyond the newest keep. Every backup is logged and observed with its
// outcome.
func (e *Taker) TakeTenant(ctx context.Context, tenantID string) (Result, error) {
	res, err := e.take(ctx, tenantID)
	switch {
	case errors.Is(err, ErrSkipped):
		e.logger.Info("catalog backup skipped: another backup or restore holds the tenant's lease", "tenant_id", tenantID)
		e.metrics.BackupObserved(ports.BackupSkipped, 0, 0, 0, 0)
	case err != nil:
		level := slog.LevelError
		if ctx.Err() != nil {
			level = slog.LevelWarn
		}
		e.logger.Log(ctx, level, "catalog backup failed: nothing was stored", "tenant_id", tenantID, "error", err)
		e.metrics.BackupObserved(ports.BackupFailure, 0, 0, 0, 0)
	default:
		e.logger.Info("catalog backup stored", "tenant_id", tenantID, "object_key", res.ObjectKey,
			"bytes", res.Bytes, "nodes", res.Nodes, "relationships", res.Relationships,
			"duration_seconds", res.Duration.Seconds())
		e.prune(ctx, tenantID, res.ObjectKey)
		e.metrics.BackupObserved(ports.BackupSuccess, res.Duration, res.Bytes, res.Nodes, res.Relationships)
	}
	return res, err
}

// TakeEach backs up the tenants one after the other, in the order given, and
// calls each with a tenant's result and error as soon as its backup ends, so
// one tenant's failure or skip does not stop the rest. When ctx ends it starts
// no further backup and returns the tenants it did not start.
func (e *Taker) TakeEach(ctx context.Context, tenants []string, each func(tenantID string, res Result, err error)) (notReached []string) {
	for i, tenantID := range tenants {
		if ctx.Err() != nil {
			return tenants[i:]
		}
		res, err := e.TakeTenant(ctx, tenantID)
		each(tenantID, res, err)
	}
	return nil
}

func (e *Taker) take(ctx context.Context, tenantID string) (Result, error) {
	// The backup is opened with the work's context, so a stall or a lost
	// lease that cancels it also ends a write the store never answers.
	workCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	work := &progress{}
	w, key, err := e.store.Open(workCtx, tenantID, e.clock(), work.advance)
	if err != nil {
		return Result{}, fmt.Errorf("open the backup of tenant %q: %w", tenantID, err)
	}
	holder := newHolder(ports.LeaseBackup)
	if err := e.keeper.acquire(workCtx, tenantID, ports.LeaseBackup, holder); err != nil {
		return Result{}, errors.Join(fmt.Errorf("backup of tenant %q: %w", tenantID, err), e.abort(w, key))
	}
	held := e.clock()
	res, err := e.stream(workCtx, cancel, tenantID, holder, w, work)
	// The lease is released before a failed backup is aborted, so the
	// tenant's writes resume without waiting on the store.
	releaseErr := e.keeper.release(ctx, tenantID, holder)
	res.Duration = e.clock().Sub(held)
	if err != nil {
		return Result{}, errors.Join(err, e.abort(w, key), releaseErr)
	}
	if releaseErr != nil {
		e.logger.Error("catalog backup stored, but its lease was not released: the tenant's writes resume when it expires",
			"tenant_id", tenantID, "object_key", key, "expires_within", e.keeper.ttl.String(), "error", releaseErr)
	}
	res.ObjectKey = key
	return res, nil
}

// stream writes the tenant's graph to w and completes it, while holder's
// lease is held, renewed and watched; ctx is the work's context and cancel
// ends it. Every record written to w is progress, and so is the start of
// completing w, and so is every write w makes to the store (work was handed to
// the store's Open); the counting the graph does before its header has the
// stall limit from the start. It completes w only once the lease is confirmed held
// after the read: no write can have changed the graph under it.
func (e *Taker) stream(ctx context.Context, cancel context.CancelCauseFunc, tenantID, holder string, w ports.BackupSink, work *progress) (Result, error) {
	stop := e.keeper.watch(ctx, cancel, tenantID, holder, work)
	defer stop()
	fail := func(err error) error {
		if cause := stopCause(ctx); cause != nil {
			return fmt.Errorf("backup of tenant %q stopped: %w", tenantID, cause)
		}
		return err
	}
	records := &countingRecords{RecordWriter: w, work: work}
	if err := e.graph.Save(ctx, tenantID, records); err != nil {
		return Result{}, fail(fmt.Errorf("back up the graph of tenant %q: %w", tenantID, err))
	}
	if cause := stopCause(ctx); cause != nil {
		return Result{}, fail(cause)
	}
	if err := e.keeper.confirm(ctx, tenantID, holder); err != nil {
		return Result{}, fail(err)
	}
	work.advance()
	size, err := w.Close()
	if err != nil {
		return Result{}, fail(fmt.Errorf("complete the backup of tenant %q: %w", tenantID, err))
	}
	return Result{Bytes: size, Nodes: records.nodes, Relationships: records.rels}, nil
}

// abort discards a backup that will not be completed. A failure is logged:
// what was uploaded so far stays in the store until a bucket rule that
// aborts incomplete multipart uploads removes it.
func (e *Taker) abort(w ports.BackupSink, key string) error {
	if err := w.Abort(); err != nil {
		e.logger.Error("catalog backup not aborted: its uploaded parts stay in the store", "object_key", key, "error", err)
		return fmt.Errorf("abort the backup %s: %w", key, err)
	}
	return nil
}

// countingRecords counts the nodes and relationships written through it,
// each record written being progress.
type countingRecords struct {
	ports.RecordWriter
	work        *progress
	nodes, rels int64
}

func (c *countingRecords) WriteHeader(h graphbackup.Header) error {
	if err := c.RecordWriter.WriteHeader(h); err != nil {
		return err
	}
	c.work.advance()
	return nil
}

func (c *countingRecords) WriteNode(n graphbackup.NodeRecord) error {
	if err := c.RecordWriter.WriteNode(n); err != nil {
		return err
	}
	c.nodes++
	c.work.advance()
	return nil
}

func (c *countingRecords) WriteRel(r graphbackup.RelRecord) error {
	if err := c.RecordWriter.WriteRel(r); err != nil {
		return err
	}
	c.rels++
	c.work.advance()
	return nil
}

// progress counts what a backup or a restore has done; the watch reads it
// to tell work that moves from work that is stuck. It is safe for concurrent
// use.
type progress struct{ n atomic.Int64 }

// advance records one step of progress: a record moved, or the start of a
// step that moves none for a while.
func (p *progress) advance() { p.n.Add(1) }

func (p *progress) load() int64 { return p.n.Load() }

// hostname names this process in a lease's holder.
var hostname = func() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "unknown"
}()

// newHolder returns a holder no other backup or restore uses:
// "<kind>/<host>/<uuid>".
func newHolder(kind ports.LeaseKind) string {
	return fmt.Sprintf("%s/%s/%s", kind, hostname, uuid.NewString())
}

// leaseKeeper holds a tenant's backup lease while a backup or a restore
// runs, and stops the work when the lease is lost or the work stalls.
type leaseKeeper struct {
	lease        ports.BackupLease
	ttl          time.Duration
	renewEvery   time.Duration
	stallAfter   time.Duration
	renewTimeout time.Duration
	// waitEvery is the interval at which acquire asks again while a message's
	// writes are in flight.
	waitEvery time.Duration
	logger    *slog.Logger
}

func newLeaseKeeper(lease ports.BackupLease, logger *slog.Logger) leaseKeeper {
	return leaseKeeper{
		lease: lease, ttl: leaseTTL, renewEvery: leaseRenewEvery,
		stallAfter: stallLimit, renewTimeout: renewTimeout, waitEvery: inFlightRetryEvery, logger: logger,
	}
}

// watch renews holder's lease of tenantID every renewEvery while the work
// whose context is ctx runs, each renewal bounded by renewTimeout, until
// stop is called. It ends the work by calling cancel with the cause when a
// renewal reports the lease lost (ports.ErrLeaseLost), or when work has not
// advanced for stallAfter (ErrStalled), counted from the start of the watch
// or the last advance; it then renews no more, so the caller's release is the
// last word on the lease. Any other renewal failure is logged and retried at
// the next tick, since the lease lasts ttl past its last renewal. stop waits
// for the renewals to end.
func (k leaseKeeper) watch(ctx context.Context, cancel context.CancelCauseFunc, tenantID, holder string, work *progress) func() {
	quit := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(k.renewEvery)
		defer tick.Stop()
		seen, movedAt := work.load(), time.Now()
		for {
			select {
			case <-quit:
				return
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			if n := work.load(); n != seen {
				seen, movedAt = n, time.Now()
			} else if idle := time.Since(movedAt); idle >= k.stallAfter {
				err := fmt.Errorf("%w: nothing moved for %s", ErrStalled, idle.Round(time.Millisecond))
				k.logger.Error("backup or restore stalled: stopping it so its lease no longer pauses the tenant's writes",
					"tenant_id", tenantID, "holder", holder, "stall_limit", k.stallAfter.String())
				cancel(err)
				return
			}
			renewCtx, cancelRenew := context.WithTimeout(ctx, k.renewTimeout)
			err := k.lease.Renew(renewCtx, tenantID, holder, k.ttl)
			cancelRenew()
			switch {
			case err == nil:
			case errors.Is(err, ports.ErrLeaseLost):
				k.logger.Error("backup lease lost: stopping the work it covered", "tenant_id", tenantID, "holder", holder, "error", err)
				cancel(err)
				return
			case ctx.Err() == nil:
				k.logger.Warn("backup lease renewal failed; retrying at the next tick", "tenant_id", tenantID, "holder", holder, "error", err)
			}
		}
	}()
	return func() {
		close(quit)
		<-done
	}
}

// stopCause returns the cause watch ended the work of ctx with (a lost lease
// or a stall), or nil.
func stopCause(ctx context.Context) error {
	if cause := context.Cause(ctx); errors.Is(cause, ports.ErrLeaseLost) || errors.Is(cause, ErrStalled) {
		return cause
	}
	return nil
}

// acquire takes tenantID's lease for holder. A lease another holder has is
// ErrSkipped. While a message's writes are in flight (ports.ErrWritesInFlight)
// it asks again every waitEvery until it gets the lease, finds one held
// (ErrSkipped) or ctx ends; a refusal took nothing, so it releases nothing.
// Any other failure may hide an Acquire that committed while its answer was
// lost, so the lease is released for holder before the failure is returned;
// releasing a lease holder does not have does nothing.
func (k leaseKeeper) acquire(ctx context.Context, tenantID string, kind ports.LeaseKind, holder string) error {
	var waitingSince time.Time
	for {
		err := k.lease.Acquire(ctx, tenantID, kind, holder, k.ttl)
		switch {
		case err == nil:
			if !waitingSince.IsZero() {
				k.logger.Info("catalog backup lease taken after waiting",
					"tenant_id", tenantID, "waited", time.Since(waitingSince).Round(time.Millisecond).String())
			}
			return nil
		case errors.Is(err, ports.ErrLeaseHeld):
			return ErrSkipped
		case errors.Is(err, ports.ErrWritesInFlight):
			if waitingSince.IsZero() {
				waitingSince = time.Now()
				k.logger.Info("catalog backup waiting: a message's writes are in flight",
					"tenant_id", tenantID, "holder", holder)
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("wait for the writes in flight of tenant %q: %w", tenantID, context.Cause(ctx))
			case <-time.After(k.waitEvery):
			}
			continue
		}
		return errors.Join(fmt.Errorf("take the backup lease: %w", err), k.release(ctx, tenantID, holder))
	}
}

// confirm renews holder's lease once more. Every renewal requires a live
// lease, so success proves the lease never lapsed since it was taken.
func (k leaseKeeper) confirm(ctx context.Context, tenantID, holder string) error {
	if err := k.lease.Renew(ctx, tenantID, holder, k.ttl); err != nil {
		return fmt.Errorf("confirm the backup lease of tenant %q: %w", tenantID, err)
	}
	return nil
}

// release ends holder's lease on a context that outlives ctx's cancellation,
// so a shutdown still releases it.
func (k leaseKeeper) release(ctx context.Context, tenantID, holder string) error {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	defer cancel()
	if err := k.lease.Release(releaseCtx, tenantID, holder); err != nil {
		return fmt.Errorf("release the backup lease of tenant %q: %w", tenantID, err)
	}
	return nil
}
